use crate::all_storages::AllStorages;
use crate::borrow::Mutability;
use crate::component::{Component, Unique};
use crate::scheduler::info::{BatchInfo, Conflict, SystemId, SystemInfo, TypeInfo, WorkloadInfo};
use crate::scheduler::{Batches, IntoWorkloadSystem, Label, Scheduler, WorkloadSystem};
use crate::sparse_set::SparseSet;
use crate::type_id::TypeId;
use crate::unique::UniqueStorage;
use crate::view::AllStoragesView;
use crate::world::World;
use crate::{error, track};
// this is the macro, not the module
use crate::storage::StorageId;
use alloc::boxed::Box;
// macro not module
use alloc::vec;
use alloc::vec::Vec;
#[cfg(not(feature = "std"))]
use core::any::Any;
use hashbrown::HashMap;
#[cfg(feature = "std")]
use std::error::Error;

/// Used to create a [`WorkloadBuilder`].
///
/// You can also use [`WorkloadBuilder::new`].
///
/// [`WorkloadBuilder`]: crate::WorkloadBuilder
/// [`WorkloadBuilder::new`]: crate::WorkloadBuilder::new()
pub struct ScheduledWorkload {
    name: Box<dyn Label>,
    #[allow(clippy::type_complexity)]
    systems: Vec<Box<dyn Fn(&World) -> Result<(), error::Run> + Send + Sync + 'static>>,
    system_names: Vec<&'static str>,
    #[allow(unused)]
    system_generators: Vec<fn(&mut Vec<TypeInfo>) -> TypeId>,
    // system's `TypeId` to an index into both systems and system_names
    #[allow(unused)]
    lookup_table: HashMap<TypeId, usize>,
    /// workload name to list of "batches"
    workloads: HashMap<Box<dyn Label>, Batches>,
}

impl ScheduledWorkload {
    /// Creates a new empty [`WorkloadBuilder`].
    ///
    /// [`WorkloadBuilder`]: crate::WorkloadBuilder
    pub fn builder<L: Label>(label: L) -> WorkloadBuilder {
        WorkloadBuilder::new(label)
    }

    /// Runs the workload.
    ///
    /// ### Borrows
    ///
    /// - Systems' borrow as they are executed
    ///
    /// ### Errors
    ///
    /// - Storage borrow failed.
    /// - User error returned by system.
    pub fn run_with_world(&self, world: &World) -> Result<(), error::RunWorkload> {
        world.run_batches(
            &self.systems,
            &self.system_names,
            &self.workloads[&self.name],
            #[cfg(feature = "tracing")]
            &self.name,
        )
    }
}

pub(super) enum WorkUnit {
    System(WorkloadSystem),
    WorkloadName(Box<dyn Label>),
}

impl From<WorkloadSystem> for WorkUnit {
    fn from(system: WorkloadSystem) -> Self {
        WorkUnit::System(system)
    }
}

impl From<Box<dyn Label>> for WorkUnit {
    fn from(workload: Box<dyn Label>) -> Self {
        WorkUnit::WorkloadName(workload)
    }
}

/// Keeps information to create a workload.
///
/// A workload is a collection of systems. They will execute as much in parallel as possible.  
/// They are evaluated first to last when they can't be parallelized.  
/// The default workload will automatically be set to the first workload added.
pub struct WorkloadBuilder {
    pub(super) work_units: Vec<WorkUnit>,
    pub(super) name: Box<dyn Label>,
    pub(super) skip_if: Vec<Box<dyn Fn(AllStoragesView<'_>) -> bool + Send + Sync + 'static>>,
}

impl WorkloadBuilder {
    /// Creates a new empty [`WorkloadBuilder`].
    ///
    /// [`WorkloadBuilder`]: crate::WorkloadBuilder
    ///
    /// ### Example
    /// ```
    /// use shipyard::{Component, IntoIter, View, ViewMut, Workload, World};
    ///
    /// #[derive(Component, Clone, Copy)]
    /// struct U32(u32);
    ///
    /// #[derive(Component, Debug, PartialEq, Eq)]
    /// struct USIZE(usize);
    ///
    /// fn add(mut usizes: ViewMut<USIZE>, u32s: View<U32>) {
    ///     for (mut x, &y) in (&mut usizes, &u32s).iter() {
    ///         x.0 += y.0 as usize;
    ///     }
    /// }
    ///
    /// fn check(usizes: View<USIZE>) {
    ///     let mut iter = usizes.iter();
    ///     assert_eq!(iter.next(), Some(&USIZE(1)));
    ///     assert_eq!(iter.next(), Some(&USIZE(5)));
    ///     assert_eq!(iter.next(), Some(&USIZE(9)));
    /// }
    ///
    /// let mut world = World::new();
    ///
    /// world.add_entity((USIZE(0), U32(1)));
    /// world.add_entity((USIZE(2), U32(3)));
    /// world.add_entity((USIZE(4), U32(5)));
    ///
    /// Workload::builder("Add & Check")
    ///     .with_system(add)
    ///     .with_system(check)
    ///     .add_to_world(&world)
    ///     .unwrap();
    ///
    /// world.run_default();
    /// ```
    pub fn new<L: Label>(label: L) -> Self {
        WorkloadBuilder {
            work_units: Vec::new(),
            name: Box::new(label),
            skip_if: Vec::new(),
        }
    }
    /// Moves all systems of `other` into `Self`, leaving `other` empty.  
    /// This allows us to collect systems in different builders before joining them together.
    pub fn append(mut self, other: &mut Self) -> Self {
        self.work_units.append(&mut other.work_units);

        self
    }
    /// Nests a workload by adding all its systems.  
    /// This other workload must be present in the `World` by the time `add_to_world` is called.
    pub fn with_workload<W: Label>(mut self, workload: W) -> Self {
        let workload: Box<dyn Label> = Box::new(workload);

        self.work_units.push(workload.into());

        self
    }
    /// Adds a system to the workload being created.
    ///
    /// ### Example:
    /// ```
    /// use shipyard::{Component, EntitiesViewMut, IntoIter, View, ViewMut, Workload, World};
    ///
    /// #[derive(Component, Clone, Copy)]
    /// struct U32(u32);
    ///
    /// #[derive(Component, Debug, PartialEq, Eq)]
    /// struct USIZE(usize);
    ///
    /// fn add(mut usizes: ViewMut<USIZE>, u32s: View<U32>) {
    ///     for (mut x, &y) in (&mut usizes, &u32s).iter() {
    ///         x.0 += y.0 as usize;
    ///     }
    /// }
    ///
    /// fn check(usizes: View<USIZE>) {
    ///     let mut iter = usizes.iter();
    ///     assert_eq!(iter.next(), Some(&USIZE(1)));
    ///     assert_eq!(iter.next(), Some(&USIZE(5)));
    ///     assert_eq!(iter.next(), Some(&USIZE(9)));
    /// }
    ///
    /// let mut world = World::new();
    ///
    /// world.add_entity((USIZE(0), U32(1)));
    /// world.add_entity((USIZE(2), U32(3)));
    /// world.add_entity((USIZE(4), U32(5)));
    ///
    /// Workload::builder("Add & Check")
    ///     .with_system(add)
    ///     .with_system(check)
    ///     .add_to_world(&world)
    ///     .unwrap();
    ///
    /// world.run_default();
    /// ```
    #[track_caller]
    pub fn with_system<B, R, S: IntoWorkloadSystem<B, R>>(mut self, system: S) -> Self {
        self.work_units
            .push(system.into_workload_system().unwrap().into());

        self
    }
    /// Adds a fallible system to the workload being created.  
    /// The workload's execution will stop if any error is encountered.
    ///
    /// ### Example:
    /// ```
    /// use shipyard::{Component, EntitiesViewMut, Get, IntoIter, IntoWithId, View, ViewMut, Workload, World};
    /// use shipyard::error::MissingComponent;
    ///
    /// #[derive(Component, Clone, Copy)]
    /// struct U32(u32);
    ///
    /// #[derive(Component, Debug, PartialEq, Eq)]
    /// struct USIZE(usize);
    ///
    /// fn add(mut usizes: ViewMut<USIZE>, u32s: View<U32>) {
    ///     for (mut x, &y) in (&mut usizes, &u32s).iter() {
    ///         x.0 += y.0 as usize;
    ///     }
    /// }
    ///
    /// fn check(usizes: View<USIZE>) -> Result<(), MissingComponent> {
    ///     for (id, i) in usizes.iter().with_id() {
    ///         assert!(usizes.get(id)? == i);
    ///     }
    ///
    ///     Ok(())
    /// }
    ///
    /// let mut world = World::new();
    ///
    /// world.add_entity((USIZE(0), U32(1)));
    /// world.add_entity((USIZE(2), U32(3)));
    /// world.add_entity((USIZE(4), U32(5)));
    ///
    /// Workload::builder("Add & Check")
    ///     .with_system(add)
    ///     .with_try_system(check)
    ///     .add_to_world(&world)
    ///     .unwrap();
    ///
    /// world.run_default();
    /// ```
    #[track_caller]
    #[cfg(feature = "std")]
    pub fn with_try_system<
        B,
        Ok,
        Err: 'static + Into<Box<dyn Error + Send + Sync>>,
        R: Into<Result<Ok, Err>>,
        S: IntoWorkloadSystem<B, R>,
    >(
        mut self,
        system: S,
    ) -> Self {
        self.work_units
            .push(system.into_workload_try_system::<Ok, Err>().unwrap().into());

        self
    }
    /// Adds a fallible system to the workload being created.  
    /// The workload's execution will stop if any error is encountered.
    ///
    /// ### Example:
    /// ```
    /// use shipyard::{EntitiesViewMut, Get, IntoIter, IntoWithId, View, ViewMut, Workload, World};
    /// use shipyard::error::MissingComponent;
    ///
    /// fn add(mut usizes: ViewMut<usize>, u32s: View<u32>) {
    ///     for (mut x, &y) in (&mut usizes, &u32s).iter() {
    ///         *x += y as usize;
    ///     }
    /// }
    ///
    /// fn check(usizes: View<usize>) -> Result<(), MissingComponent> {
    ///     for (id, i) in usizes.iter().with_id() {
    ///         assert!(usizes.get(id)? == i);
    ///     }
    ///
    ///     Ok(())
    /// }
    ///
    /// let mut world = World::new();
    ///
    /// world.add_entity((0usize, 1u32));
    /// world.add_entity((2usize, 3u32));
    /// world.add_entity((4usize, 5u32));
    ///
    /// Workload::builder("Add & Check")
    ///     .with_system(add)
    ///     .with_try_system(check)
    ///     .add_to_world(&world)
    ///     .unwrap();
    ///
    /// world.run_default();
    /// ```
    #[track_caller]
    #[cfg(not(feature = "std"))]
    pub fn with_try_system<
        B,
        Ok,
        Err: 'static + Send + Any,
        R: Into<Result<Ok, Err>>,
        S: IntoWorkloadSystem<B, R>,
    >(
        mut self,
        system: S,
    ) -> Self {
        self.work_units
            .push(system.into_workload_try_system::<Ok, Err>().unwrap().into());

        self
    }
    /// Finishes the workload creation and stores it in the [`World`].  
    /// Returns a struct with describing how the workload has been split in batches.
    ///
    /// ### Borrows
    ///
    /// - Scheduler (exclusive)
    ///
    /// ### Errors
    ///
    /// - Scheduler borrow failed.
    /// - Workload with an identical name already present.
    /// - Nested workload is not present in `world`.
    ///
    /// [`World`]: crate::World
    #[allow(clippy::blocks_in_if_conditions)]
    pub fn add_to_world(self, world: &World) -> Result<WorkloadInfo, error::AddWorkload> {
        let Scheduler {
            systems,
            system_names,
            system_generators,
            lookup_table,
            workloads,
            default,
        } = &mut *world
            .scheduler
            .borrow_mut()
            .map_err(|_| error::AddWorkload::Borrow)?;

        create_workload(
            self,
            systems,
            system_names,
            system_generators,
            lookup_table,
            workloads,
            default,
        )
    }
    /// Returns the first [`Unique`] storage borrowed by this workload that is not present in `world`.\
    /// If the workload contains nested workloads they have to be present in the `World`.
    ///
    /// ### Borrows
    ///
    /// - AllStorages (shared)
    /// - Scheduler (shared)
    pub fn are_all_uniques_present_in_world(
        &self,
        world: &World,
    ) -> Result<(), error::UniquePresence> {
        struct ComponentType;

        impl Component for ComponentType {
            type Tracking = track::Untracked;
        }
        impl Unique for ComponentType {
            type Tracking = track::Untracked;
        }

        let all_storages = world
            .all_storages
            .borrow()
            .map_err(|_| error::UniquePresence::AllStorages)?;
        let storages = all_storages.storages.read();
        let scheduler = world
            .scheduler
            .borrow()
            .map_err(|_| error::UniquePresence::Scheduler)?;

        let unique_name = core::any::type_name::<UniqueStorage<ComponentType>>()
            .split_once('<')
            .unwrap()
            .0;
        let mut type_infos = Vec::new();

        for work_unit in &self.work_units {
            if let Some(value) = check_uniques_in_work_unit(
                work_unit,
                unique_name,
                &storages,
                &scheduler,
                &mut type_infos,
            ) {
                return value;
            }
        }

        for type_info in type_infos {
            if type_info.name.starts_with(unique_name)
                && !storages.contains_key(&type_info.storage_id)
            {
                return Err(error::UniquePresence::Unique(type_info));
            }
        }

        Ok(())
    }
    /// Build the [`Workload`](super::Workload) from the [`WorkloadBuilder`].
    pub fn build(self) -> Result<(ScheduledWorkload, WorkloadInfo), error::AddWorkload> {
        let mut workload = ScheduledWorkload {
            name: self.name.clone(),
            systems: Vec::new(),
            system_names: Vec::new(),
            system_generators: Vec::new(),
            lookup_table: HashMap::new(),
            workloads: HashMap::new(),
        };

        let mut default: Box<dyn Label> = Box::new("");

        let workload_info = create_workload(
            self,
            &mut workload.systems,
            &mut workload.system_names,
            &mut workload.system_generators,
            &mut workload.lookup_table,
            &mut workload.workloads,
            &mut default,
        )?;

        Ok((workload, workload_info))
    }
    /// Do not run the workload if the function evaluates to `true`.
    pub fn skip_if<F>(mut self, should_skip: F) -> Self
    where
        F: Fn(AllStoragesView<'_>) -> bool + Send + Sync + 'static,
    {
        self.skip_if.push(Box::new(should_skip));
        self
    }
    /// Do not run the workload if the `T` storage is empty.
    ///
    /// If the storage is not present it is considered empty.
    /// If the storage is already borrowed, assume it's not empty.
    pub fn skip_if_storage_empty<T: Component>(self) -> Self {
        let storage_id = StorageId::of::<SparseSet<T>>();
        self.skip_if_storage_empty_by_id(storage_id)
    }
    /// Do not run the workload if the `T` unique storage is not present in the `World`.
    pub fn skip_if_missing_unique<T: Unique>(self) -> Self {
        let storage_id = StorageId::of::<UniqueStorage<T>>();
        self.skip_if_storage_empty_by_id(storage_id)
    }
    /// Do not run the workload if the storage is empty.
    ///
    /// If the storage is not present it is considered empty.
    /// If the storage is already borrowed, assume it's not empty.
    pub fn skip_if_storage_empty_by_id(self, storage_id: StorageId) -> Self {
        use crate::all_storages::CustomStorageAccess;

        let should_skip = move |all_storages: AllStoragesView<'_>| match all_storages
            .custom_storage_by_id(storage_id)
        {
            Ok(storage) => storage.is_empty(),
            Err(error::GetStorage::MissingStorage { .. }) => true,
            Err(_) => false,
        };

        self.skip_if(should_skip)
    }
}

fn check_uniques_in_work_unit(
    work_unit: &WorkUnit,
    unique_name: &str,
    storages: &HashMap<StorageId, crate::storage::SBox>,
    scheduler: &Scheduler,
    type_infos: &mut Vec<TypeInfo>,
) -> Option<Result<(), error::UniquePresence>> {
    match work_unit {
        WorkUnit::System(WorkloadSystem::System {
            borrow_constraints, ..
        }) => {
            for type_info in borrow_constraints {
                if type_info.name.starts_with(unique_name)
                    && !storages.contains_key(&type_info.storage_id)
                {
                    return Some(Err(error::UniquePresence::Unique(type_info.clone())));
                }
            }
        }
        WorkUnit::WorkloadName(workload) => {
            if let Some(workload) = scheduler.workloads.get(workload) {
                for system_index in &workload.sequential {
                    scheduler.system_generators[*system_index](type_infos);
                }
            } else {
                return Some(Err(error::UniquePresence::Workload(workload.clone())));
            }
        }
        WorkUnit::System(WorkloadSystem::Workload(workload)) => {
            for wu in &workload.work_units {
                let check =
                    check_uniques_in_work_unit(wu, unique_name, storages, scheduler, type_infos);

                if check.is_some() {
                    return check;
                }
            }
        }
    }

    None
}

#[allow(clippy::type_complexity)]
fn create_workload(
    mut builder: WorkloadBuilder,
    systems: &mut Vec<Box<dyn Fn(&World) -> Result<(), error::Run> + Send + Sync + 'static>>,
    system_names: &mut Vec<&'static str>,
    system_generators: &mut Vec<fn(&mut Vec<TypeInfo>) -> TypeId>,
    lookup_table: &mut HashMap<TypeId, usize>,
    workloads: &mut HashMap<Box<dyn Label>, Batches>,
    default: &mut Box<dyn Label>,
) -> Result<WorkloadInfo, error::AddWorkload> {
    if workloads.contains_key(&*builder.name) {
        return Err(error::AddWorkload::AlreadyExists);
    }

    if builder.work_units.is_empty() {
        if workloads.is_empty() {
            *default = builder.name.clone();
        }

        workloads.insert(builder.name.clone(), Batches::default());

        Ok(WorkloadInfo {
            name: builder.name,
            batch_info: Vec::new(),
        })
    } else {
        for work_unit in &builder.work_units {
            if let WorkUnit::WorkloadName(workload) = work_unit {
                if !workloads.contains_key(&**workload) {
                    return Err(error::AddWorkload::UnknownWorkload(
                        builder.name,
                        workload.clone(),
                    ));
                }
            }
        }

        let mut collected_systems: Vec<(TypeId, &'static str, usize, Vec<TypeInfo>)> =
            Vec::with_capacity(builder.work_units.len());

        for work_unit in builder.work_units.drain(..) {
            flatten_work_unit(
                work_unit,
                systems,
                lookup_table,
                &mut collected_systems,
                workloads,
                system_generators,
                system_names,
            );
        }

        if workloads.is_empty() {
            *default = builder.name.clone();
        }

        let batches = workloads.entry(builder.name.clone()).or_default();

        batches.skip_if = builder.skip_if;

        if collected_systems.len() == 1 {
            let (system_type_id, system_type_name, system_index, borrow_constraints) =
                collected_systems.pop().unwrap();

            let mut all_storages = None;
            let mut non_send_sync = None;

            for type_info in &borrow_constraints {
                if type_info.storage_id == TypeId::of::<AllStorages>() {
                    all_storages = Some(type_info);
                    break;
                } else if !type_info.thread_safe {
                    non_send_sync = Some(type_info);
                    break;
                }
            }

            if all_storages.is_some() || non_send_sync.is_some() {
                batches.parallel.push((Some(system_index), Vec::new()));
            } else {
                batches.parallel.push((None, vec![system_index]));
            }

            batches.sequential.push(system_index);

            let batch_info = BatchInfo {
                systems: (
                    Some(SystemInfo {
                        name: system_type_name,
                        type_id: system_type_id,
                        borrow: borrow_constraints,
                        conflict: None,
                    }),
                    Vec::new(),
                ),
            };

            Ok(WorkloadInfo {
                name: builder.name,
                batch_info: vec![batch_info],
            })
        } else {
            let mut workload_info = WorkloadInfo {
                name: builder.name,
                batch_info: vec![],
            };

            'systems: for (system_type_id, system_type_name, system_index, borrow_constraints) in
                collected_systems
            {
                batches.sequential.push(system_index);

                let mut valid = batches.parallel.len();

                let mut all_storages = None;
                let mut non_send_sync = None;

                for type_info in &borrow_constraints {
                    if type_info.storage_id == TypeId::of::<AllStorages>() {
                        all_storages = Some(type_info.clone());
                        break;
                    } else if !type_info.thread_safe {
                        non_send_sync = Some(type_info.clone());
                        break;
                    }
                }

                if let Some(all_storages_type_info) = all_storages {
                    for (i, batch_info) in workload_info.batch_info.iter().enumerate().rev() {
                        match (
                            &batch_info.systems.0,
                            batch_info
                                .systems
                                .1
                                .iter()
                                .rev()
                                .find(|other_system_info| !other_system_info.borrow.is_empty()),
                        ) {
                            (None, None) => valid = i,
                            (Some(other_system_info), None)
                            | (None, Some(other_system_info))
                            | (Some(other_system_info), Some(_)) => {
                                let system_info = SystemInfo {
                                    name: system_type_name,
                                    type_id: system_type_id,
                                    borrow: borrow_constraints,
                                    conflict: Some(Conflict::Borrow {
                                        type_info: Some(all_storages_type_info.clone()),
                                        other_system: SystemId {
                                            name: other_system_info.name,
                                            type_id: other_system_info.type_id,
                                        },
                                        other_type_info: other_system_info
                                            .borrow
                                            .last()
                                            .unwrap()
                                            .clone(),
                                    }),
                                };

                                if valid < batches.parallel.len() {
                                    batches.parallel[valid].0 = Some(system_index);
                                    workload_info.batch_info[valid].systems.0 = Some(system_info);
                                } else {
                                    batches.parallel.push((Some(system_index), Vec::new()));
                                    workload_info.batch_info.push(BatchInfo {
                                        systems: (Some(system_info), Vec::new()),
                                    });
                                }

                                continue 'systems;
                            }
                        }
                    }

                    let system_info = SystemInfo {
                        name: system_type_name,
                        type_id: system_type_id,
                        borrow: borrow_constraints,
                        conflict: None,
                    };

                    if valid < batches.parallel.len() {
                        batches.parallel[valid].0 = Some(system_index);
                        workload_info.batch_info[valid].systems.0 = Some(system_info);
                    } else {
                        batches.parallel.push((Some(system_index), Vec::new()));
                        workload_info.batch_info.push(BatchInfo {
                            systems: (Some(system_info), Vec::new()),
                        });
                    }
                } else {
                    let mut conflict = None;

                    'batch: for (i, batch_info) in workload_info.batch_info.iter().enumerate().rev()
                    {
                        if let (Some(non_send_sync_type_info), Some(other_system_info)) =
                            (&non_send_sync, &batch_info.systems.0)
                        {
                            let system_info = SystemInfo {
                                name: system_type_name,
                                type_id: system_type_id,
                                borrow: borrow_constraints,
                                conflict: Some(Conflict::Borrow {
                                    type_info: Some(non_send_sync_type_info.clone()),
                                    other_system: SystemId {
                                        name: other_system_info.name,
                                        type_id: other_system_info.type_id,
                                    },
                                    other_type_info: other_system_info
                                        .borrow
                                        .last()
                                        .unwrap()
                                        .clone(),
                                }),
                            };

                            if valid < batches.parallel.len() {
                                batches.parallel[valid].0 = Some(system_index);
                                workload_info.batch_info[valid].systems.0 = Some(system_info);
                            } else {
                                batches.parallel.push((Some(system_index), Vec::new()));
                                workload_info.batch_info.push(BatchInfo {
                                    systems: (Some(system_info), Vec::new()),
                                });
                            }

                            continue 'systems;
                        } else {
                            for other_system in batch_info
                                .systems
                                .0
                                .iter()
                                .chain(batch_info.systems.1.iter())
                            {
                                for other_type_info in &other_system.borrow {
                                    for type_info in &borrow_constraints {
                                        match type_info.mutability {
                                            Mutability::Exclusive => {
                                                if !type_info.thread_safe
                                                    && !other_type_info.thread_safe
                                                {
                                                    conflict = Some(Conflict::OtherNotSendSync {
                                                        system: SystemId {
                                                            name: other_system.name,
                                                            type_id: other_system.type_id,
                                                        },
                                                        type_info: other_type_info.clone(),
                                                    });

                                                    break 'batch;
                                                }

                                                if type_info.storage_id
                                                    == other_type_info.storage_id
                                                    || type_info.storage_id
                                                        == TypeId::of::<AllStorages>()
                                                    || other_type_info.storage_id
                                                        == TypeId::of::<AllStorages>()
                                                {
                                                    conflict = Some(Conflict::Borrow {
                                                        type_info: Some(type_info.clone()),
                                                        other_system: SystemId {
                                                            name: other_system.name,
                                                            type_id: other_system.type_id,
                                                        },
                                                        other_type_info: other_type_info.clone(),
                                                    });

                                                    break 'batch;
                                                }
                                            }
                                            Mutability::Shared => {
                                                if !type_info.thread_safe
                                                    && !other_type_info.thread_safe
                                                {
                                                    conflict = Some(Conflict::OtherNotSendSync {
                                                        system: SystemId {
                                                            name: other_system.name,
                                                            type_id: other_system.type_id,
                                                        },
                                                        type_info: other_type_info.clone(),
                                                    });

                                                    break 'batch;
                                                }

                                                if (type_info.storage_id
                                                    == other_type_info.storage_id
                                                    && other_type_info.mutability
                                                        == Mutability::Exclusive)
                                                    || type_info.storage_id
                                                        == TypeId::of::<AllStorages>()
                                                    || other_type_info.storage_id
                                                        == TypeId::of::<AllStorages>()
                                                {
                                                    conflict = Some(Conflict::Borrow {
                                                        type_info: Some(type_info.clone()),
                                                        other_system: SystemId {
                                                            name: other_system.name,
                                                            type_id: other_system.type_id,
                                                        },
                                                        other_type_info: other_type_info.clone(),
                                                    });

                                                    break 'batch;
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            valid = i;
                        }
                    }

                    let system_info = SystemInfo {
                        name: system_type_name,
                        type_id: system_type_id,
                        borrow: borrow_constraints,
                        conflict,
                    };

                    if valid < batches.parallel.len() {
                        if non_send_sync.is_some() {
                            batches.parallel[valid].0 = Some(system_index);
                            workload_info.batch_info[valid].systems.0 = Some(system_info);
                        } else {
                            batches.parallel[valid].1.push(system_index);
                            workload_info.batch_info[valid].systems.1.push(system_info);
                        }
                    } else if non_send_sync.is_some() {
                        batches.parallel.push((Some(system_index), Vec::new()));
                        workload_info.batch_info.push(BatchInfo {
                            systems: (Some(system_info), Vec::new()),
                        });
                    } else {
                        batches.parallel.push((None, vec![system_index]));
                        workload_info.batch_info.push(BatchInfo {
                            systems: (None, vec![system_info]),
                        });
                    }
                }
            }

            Ok(workload_info)
        }
    }
}

#[allow(clippy::type_complexity)]
fn flatten_work_unit(
    work_unit: WorkUnit,
    systems: &mut Vec<Box<dyn Fn(&World) -> Result<(), error::Run> + Send + Sync>>,
    lookup_table: &mut HashMap<TypeId, usize>,
    collected_systems: &mut Vec<(TypeId, &str, usize, Vec<TypeInfo>)>,
    workloads: &mut HashMap<Box<dyn Label>, Batches>,
    system_generators: &mut Vec<fn(&mut Vec<TypeInfo>) -> TypeId>,
    system_names: &mut Vec<&'static str>,
) {
    match work_unit {
        WorkUnit::System(WorkloadSystem::System {
            mut borrow_constraints,
            system_type_name,
            system_type_id,
            generator,
            system_fn,
        }) => {
            let borrow_constraints = core::mem::take(&mut borrow_constraints);
            let system_type_name = system_type_name;
            let system_type_id = system_type_id;

            let system_index = *lookup_table.entry(system_type_id).or_insert_with(|| {
                systems.push(system_fn);
                system_names.push(system_type_name);
                system_generators.push(generator);
                systems.len() - 1
            });

            collected_systems.push((
                system_type_id,
                system_type_name,
                system_index,
                borrow_constraints,
            ));
        }
        WorkUnit::WorkloadName(workload) => {
            for &system_index in &*workloads[&workload].sequential {
                let mut borrow = Vec::new();

                collected_systems.push((
                    system_generators[system_index](&mut borrow),
                    system_names[system_index],
                    system_index,
                    borrow,
                ));
            }
        }
        WorkUnit::System(WorkloadSystem::Workload(workload)) => {
            for wu in workload.work_units {
                flatten_work_unit(
                    wu,
                    systems,
                    lookup_table,
                    collected_systems,
                    workloads,
                    system_generators,
                    system_names,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::{Component, Unique};
    use crate::track;

    struct Usize(usize);
    struct U32(u32);
    struct U16(u16);

    impl Component for Usize {
        type Tracking = track::Untracked;
    }
    impl Component for U32 {
        type Tracking = track::Untracked;
    }
    impl Component for U16 {
        type Tracking = track::Untracked;
    }
    impl Unique for Usize {
        type Tracking = track::Untracked;
    }
    impl Unique for U32 {
        type Tracking = track::Untracked;
    }
    impl Unique for U16 {
        type Tracking = track::Untracked;
    }

    #[test]
    fn single_immutable() {
        use crate::{View, World};

        fn system1(_: View<'_, Usize>) {}

        let world = World::new();

        ScheduledWorkload::builder("System1")
            .with_system(system1)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("System1");
        assert_eq!(scheduler.systems.len(), 1);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0])],
                sequential: vec![0],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn single_mutable() {
        use crate::{ViewMut, World};

        fn system1(_: ViewMut<'_, Usize>) {}

        let world = World::new();

        ScheduledWorkload::builder("System1")
            .with_system(system1)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("System1");
        assert_eq!(scheduler.systems.len(), 1);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0])],
                sequential: vec![0],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn multiple_immutable() {
        use crate::{IntoWorkloadSystem, View, World};

        fn system1(_: View<'_, Usize>) {}
        fn system2(_: View<'_, Usize>) {}

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .with_system(system1)
            .with_system(system2.into_workload_system().unwrap())
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Systems");
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0, 1])],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn multiple_mutable() {
        use crate::{ViewMut, World};

        fn system1(_: ViewMut<'_, Usize>) {}
        fn system2(_: ViewMut<'_, Usize>) {}

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .with_system(system1)
            .with_system(system2)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Systems");
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0]), (None, vec![1])],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn multiple_mixed() {
        use crate::{View, ViewMut, World};

        fn system1(_: ViewMut<'_, Usize>) {}
        fn system2(_: View<'_, Usize>) {}

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .with_system(system1)
            .with_system(system2)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Systems");
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0]), (None, vec![1])],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .with_system(system2)
            .with_system(system1)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Systems");
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0]), (None, vec![1])],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn append_optimizes_batches() {
        use crate::{View, ViewMut, World};

        fn system_a1(_: View<'_, Usize>, _: ViewMut<'_, U32>) {}
        fn system_a2(_: View<'_, Usize>, _: ViewMut<'_, U32>) {}
        fn system_b1(_: View<'_, Usize>) {}

        let world = World::new();

        let mut group_a = ScheduledWorkload::builder("Group A")
            .with_system(system_a1)
            .with_system(system_a2);

        let mut group_b = ScheduledWorkload::builder("Group B").with_system(system_b1);

        ScheduledWorkload::builder("Combined")
            .append(&mut group_a)
            .append(&mut group_b)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Combined");
        assert_eq!(scheduler.systems.len(), 3);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0, 2]), (None, vec![1])],
                sequential: vec![0, 1, 2],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn all_storages() {
        use crate::{AllStoragesViewMut, View, World};

        fn system1(_: View<'_, Usize>) {}
        fn system2(_: AllStoragesViewMut<'_>) {}

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .with_system(system2)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Systems");
        assert_eq!(scheduler.systems.len(), 1);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(Some(0), Vec::new())],
                sequential: vec![0],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .with_system(system2)
            .with_system(system2)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        assert_eq!(scheduler.systems.len(), 1);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(Some(0), Vec::new()), (Some(0), Vec::new())],
                sequential: vec![0, 0],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .with_system(system1)
            .with_system(system2)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Systems");
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0]), (Some(1), Vec::new())],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .with_system(system2)
            .with_system(system1)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(Some(0), Vec::new()), (None, vec![1])],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[cfg(feature = "thread_local")]
    #[test]
    fn non_send() {
        use crate::{NonSend, View, ViewMut, World};

        struct NotSend(*const ());
        unsafe impl Sync for NotSend {}
        impl Component for NotSend {
            type Tracking = track::Untracked;
        }

        fn sys1(_: NonSend<View<'_, NotSend>>) {}
        fn sys2(_: NonSend<ViewMut<'_, NotSend>>) {}
        fn sys3(_: View<'_, Usize>) {}
        fn sys4(_: ViewMut<'_, Usize>) {}

        let world = World::new();

        let info = ScheduledWorkload::builder("Test")
            .with_system(sys1)
            .with_system(sys1)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Test");
        assert_eq!(scheduler.systems.len(), 1);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0, 0])],
                sequential: vec![0, 0],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
        assert!(info.batch_info[0].systems.1[0].conflict.is_none());

        let world = World::new();

        ScheduledWorkload::builder("Test")
            .with_system(sys1)
            .with_system(sys2)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0]), (Some(1), Vec::new())],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);

        let world = World::new();

        ScheduledWorkload::builder("Test")
            .with_system(sys2)
            .with_system(sys1)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(Some(0), Vec::new()), (None, vec![1])],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);

        let world = World::new();

        let info = ScheduledWorkload::builder("Test")
            .with_system(sys1)
            .with_system(sys3)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0, 1])],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
        assert!(info.batch_info[0].systems.1[0].conflict.is_none());

        let world = World::new();

        ScheduledWorkload::builder("Test")
            .with_system(sys1)
            .with_system(sys4)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0, 1])],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn unique_and_non_unique() {
        use crate::{UniqueViewMut, ViewMut, World};

        fn system1(_: ViewMut<'_, Usize>) {}
        fn system2(_: UniqueViewMut<'_, Usize>) {}

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .with_system(system1)
            .with_system(system2)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Systems");
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0, 1])],
                sequential: vec![0, 1],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn empty_workload() {
        use crate::World;

        let world = World::new();

        ScheduledWorkload::builder("Systems")
            .add_to_world(&world)
            .unwrap();

        dbg!("here");

        // let scheduler = world.scheduler.borrow_mut().unwrap();
        // let label: Box<dyn Label> = Box::new("Systems");
        // assert_eq!(scheduler.systems.len(), 0);
        // assert_eq!(scheduler.workloads.len(), 1);
        // assert_eq!(
        //     scheduler.workloads.get(&label),
        //     Some(&Batches {
        //         parallel: vec![],
        //         sequential: vec![],
        //         skip_if: Vec::new(),
        //     })
        // );
        // assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn append_ensures_multiple_batches_can_be_optimized_over() {
        use crate::{View, ViewMut, World};

        fn sys_a1(_: ViewMut<'_, Usize>, _: ViewMut<'_, U32>) {}
        fn sys_a2(_: View<'_, Usize>, _: ViewMut<'_, U32>) {}
        fn sys_b1(_: View<'_, Usize>) {}
        fn sys_c1(_: View<'_, U16>) {}

        let world = World::new();

        let mut group_a = ScheduledWorkload::builder("Group A")
            .with_system(sys_a1)
            .with_system(sys_a2);
        let mut group_b = ScheduledWorkload::builder("Group B").with_system(sys_b1);
        let mut group_c = ScheduledWorkload::builder("Group C").with_system(sys_c1);

        ScheduledWorkload::builder("Combined")
            .append(&mut group_a)
            .append(&mut group_b)
            .append(&mut group_c)
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        let label: Box<dyn Label> = Box::new("Combined");
        assert_eq!(scheduler.systems.len(), 4);
        assert_eq!(scheduler.workloads.len(), 1);
        assert_eq!(
            scheduler.workloads.get(&label),
            Some(&Batches {
                parallel: vec![(None, vec![0, 3]), (None, vec![1, 2])],
                sequential: vec![0, 1, 2, 3],
                skip_if: Vec::new(),
            })
        );
        assert_eq!(&scheduler.default, &label);
    }

    #[test]
    fn workload_flattening() {
        use crate::{View, ViewMut, World};

        fn sys1(_: View<'_, U32>) {}
        fn sys2(_: ViewMut<'_, U32>) {}

        let world = World::new();

        ScheduledWorkload::builder("1")
            .with_system(sys1)
            .with_system(sys2)
            .with_system(sys1)
            .add_to_world(&world)
            .unwrap();

        let debug_info = ScheduledWorkload::builder("2")
            .with_workload("1")
            .with_system(sys1)
            .with_workload("1")
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        assert_eq!(scheduler.systems.len(), 2);
        assert_eq!(debug_info.batch_info.len(), 5);
    }

    #[test]
    fn empty_workload_flattening() {
        use crate::World;

        let world = World::new();

        ScheduledWorkload::builder("1")
            .add_to_world(&world)
            .unwrap();

        let debug_info = ScheduledWorkload::builder("2")
            .with_workload("1")
            .add_to_world(&world)
            .unwrap();

        let scheduler = world.scheduler.borrow_mut().unwrap();
        assert_eq!(scheduler.systems.len(), 0);
        assert_eq!(debug_info.batch_info.len(), 0);
    }

    #[test]
    fn skip_if_missing_storage() {
        let world = World::new();

        ScheduledWorkload::builder("test")
            .skip_if_storage_empty::<Usize>()
            .with_system(|| panic!())
            .build()
            .unwrap()
            .0
            .run_with_world(&world)
            .unwrap();

        ScheduledWorkload::builder("test")
            .skip_if_storage_empty::<Usize>()
            .with_system(|| panic!())
            .add_to_world(&world)
            .unwrap();

        world.run_default().unwrap();
    }

    #[test]
    fn skip_if_empty_storage() {
        let mut world = World::new();

        let eid = world.add_entity((Usize(0),));
        world.remove::<(Usize,)>(eid);

        ScheduledWorkload::builder("test")
            .skip_if_storage_empty::<Usize>()
            .with_system(|| panic!())
            .build()
            .unwrap()
            .0
            .run_with_world(&world)
            .unwrap();

        ScheduledWorkload::builder("test")
            .skip_if_storage_empty::<Usize>()
            .with_system(|| panic!())
            .add_to_world(&world)
            .unwrap();

        world.run_default().unwrap();
    }
}
