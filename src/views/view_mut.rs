use crate::all_storages::AllStorages;
use crate::atomic_refcell::{ARef, ARefMut, ExclusiveBorrow, SharedBorrow};
use crate::component::Component;
use crate::entity_id::EntityId;
use crate::error;
use crate::get::Get;
use crate::sparse_set::{SparseSet, SparseSetDrain};
use crate::storage::StorageId;
use crate::track;
use crate::tracking::{
    is_track_within_bounds, DeletionTracking, Inserted, InsertedOrModified, InsertionTracking,
    ModificationTracking, Modified, RemovalOrDeletionTracking, RemovalTracking, Track, Tracking,
};
use core::fmt;
use core::marker::PhantomData;
use core::ops::{Deref, DerefMut};

/// Exclusive view over a component storage.
pub struct ViewMut<'a, T: Component, TRACK = track::Untracked> {
    pub(crate) sparse_set: &'a mut SparseSet<T>,
    pub(crate) _all_borrow: Option<SharedBorrow<'a>>,
    pub(crate) _borrow: ExclusiveBorrow<'a>,
    pub(crate) last_insertion: u32,
    pub(crate) last_modification: u32,
    pub(crate) last_removal_or_deletion: u32,
    pub(crate) current: u32,
    pub(crate) phantom: PhantomData<TRACK>,
}

impl<'a, T: Component> ViewMut<'a, T, track::Untracked> {
    /// Creates a new [`ViewMut`] for custom [`SparseSet`] storage.
    ///
    /// ```
    /// use shipyard::{track, Component, SparseSet, StorageId, ViewMut, World};
    ///
    /// struct ScriptingComponent(Vec<u8>);
    /// impl Component for ScriptingComponent {}
    ///
    /// let world = World::new();
    ///
    /// world.add_custom_storage(
    ///     StorageId::Custom(0),
    ///     SparseSet::<ScriptingComponent>::new_custom_storage(),
    /// ).unwrap();
    ///
    /// let all_storages = world.all_storages().unwrap();
    /// let scripting_storage =
    ///     ViewMut::<ScriptingComponent>::new_for_custom_storage(StorageId::Custom(0), all_storages)
    ///         .unwrap();
    /// ```
    pub fn new_for_custom_storage(
        storage_id: StorageId,
        all_storages: ARef<'a, &'a AllStorages>,
    ) -> Result<Self, error::CustomStorageView> {
        use crate::all_storages::CustomStorageAccess;

        let (all_storages, all_borrow) = unsafe { ARef::destructure(all_storages) };

        let storage = all_storages.custom_storage_mut_by_id(storage_id)?;
        let (storage, borrow) = unsafe { ARefMut::destructure(storage) };

        let name = storage.name();

        if let Some(sparse_set) = storage.any_mut().downcast_mut() {
            Ok(ViewMut {
                sparse_set,
                _all_borrow: Some(all_borrow),
                _borrow: borrow,
                last_insertion: 0,
                last_modification: 0,
                last_removal_or_deletion: 0,
                current: 0,
                phantom: PhantomData,
            })
        } else {
            Err(error::CustomStorageView::WrongType(name))
        }
    }
}

impl<'a, T: Component, TRACK> ViewMut<'a, T, TRACK>
where
    Track<TRACK>: Tracking,
{
    /// Deletes all components in this storage.
    pub fn clear(&mut self) {
        self.sparse_set.private_clear(self.current);
    }
    /// Creates a draining iterator that empties the storage and yields the removed items.
    pub fn drain(&mut self) -> SparseSetDrain<'_, T> {
        self.sparse_set.private_drain(self.current)
    }
    /// Applies the given function `f` to the entities `a` and `b`.\
    /// The two entities shouldn't point to the same component.  
    ///
    /// ### Panics
    ///
    /// - MissingComponent - if one of the entity doesn't have any component in the storage.
    /// - IdenticalIds - if the two entities point to the same component.
    #[track_caller]
    pub fn apply<R, F: FnOnce(&mut T, &T) -> R>(&mut self, a: EntityId, b: EntityId, f: F) -> R {
        self.sparse_set.private_apply(a, b, f, self.current)
    }
    /// Applies the given function `f` to the entities `a` and `b`.\
    /// The two entities shouldn't point to the same component.  
    ///
    /// ### Panics
    ///
    /// - MissingComponent - if one of the entity doesn't have any component in the storage.
    /// - IdenticalIds - if the two entities point to the same component.
    #[track_caller]
    pub fn apply_mut<R, F: FnOnce(&mut T, &mut T) -> R>(
        &mut self,
        a: EntityId,
        b: EntityId,
        f: F,
    ) -> R {
        self.sparse_set.private_apply_mut(a, b, f, self.current)
    }
}

impl<TRACK, T: Component> ViewMut<'_, T, TRACK>
where
    Track<TRACK>: InsertionTracking,
{
    /// Inside a workload returns `true` if `entity`'s component was inserted since the last run of this system.\
    /// Outside workloads returns `true` if `entity`'s component was inserted since the last call to [`clear_all_inserted`](ViewMut::clear_all_inserted).\
    /// Returns `false` if `entity` does not have a component in this storage.
    #[inline]
    pub fn is_inserted(&self, entity: EntityId) -> bool {
        Track::<TRACK>::is_inserted(self.sparse_set, entity, self.last_insertion, self.current)
    }
    /// Wraps this view to be able to iterate *inserted* components.
    #[inline]
    pub fn inserted(&self) -> Inserted<&Self> {
        Inserted(self)
    }
    /// Wraps this view to be able to iterate *inserted* components.
    #[inline]
    pub fn inserted_mut(&mut self) -> Inserted<&mut Self> {
        Inserted(self)
    }
    /// Removes the *inserted* flag on all components of this storage.
    #[inline]
    pub fn clear_all_inserted(self) {
        self.sparse_set.private_clear_all_inserted(self.current);
    }
}

impl<TRACK, T: Component> ViewMut<'_, T, TRACK>
where
    Track<TRACK>: ModificationTracking,
{
    /// Inside a workload returns `true` if `entity`'s component was modified since the last run of this system.\
    /// Outside workloads returns `true` if `entity`'s component was modified since the last call to [`clear_all_modified`](ViewMut::clear_all_modified).\
    /// Returns `false` if `entity` does not have a component in this storage.
    #[inline]
    pub fn is_modified(&self, entity: EntityId) -> bool {
        Track::<TRACK>::is_modified(
            self.sparse_set,
            entity,
            self.last_modification,
            self.current,
        )
    }
    /// Wraps this view to be able to iterate *modified* components.
    #[inline]
    pub fn modified(&self) -> Modified<&Self> {
        Modified(self)
    }
    /// Wraps this view to be able to iterate *modified* components.
    #[inline]
    pub fn modified_mut(&mut self) -> Modified<&mut Self> {
        Modified(self)
    }
    /// Removes the *modified* flag on all components of this storage.
    #[inline]
    pub fn clear_all_modified(self) {
        self.sparse_set.private_clear_all_modified(self.current);
    }
}

impl<TRACK, T: Component> ViewMut<'_, T, TRACK>
where
    Track<TRACK>: InsertionTracking + ModificationTracking,
{
    /// Inside a workload returns `true` if `entity`'s component was inserted or modified since the last run of this system.\
    /// Outside workloads returns `true` if `entity`'s component was inserted or modified since the last call to [`clear_all_inserted`](ViewMut::clear_all_inserted).\
    /// Returns `false` if `entity` does not have a component in this storage.
    #[inline]
    pub fn is_inserted_or_modified(&self, entity: EntityId) -> bool {
        self.is_inserted(entity) || self.is_modified(entity)
    }
    /// Wraps this view to be able to iterate *inserted* and *modified* components.
    #[inline]
    pub fn inserted_or_modified(&self) -> InsertedOrModified<&Self> {
        InsertedOrModified(self)
    }
    /// Wraps this view to be able to iterate *inserted* and *modified* components.
    #[inline]
    pub fn inserted_or_modified_mut(&mut self) -> InsertedOrModified<&mut Self> {
        InsertedOrModified(self)
    }
    /// Removes the *inserted* and *modified* flags on all components of this storage.
    #[inline]
    pub fn clear_all_inserted_and_modified(self) {
        self.sparse_set
            .private_clear_all_inserted_and_modified(self.current);
    }
}

impl<TRACK, T: Component> ViewMut<'_, T, TRACK>
where
    Track<TRACK>: DeletionTracking,
{
    /// Inside a workload returns `true` if `entity`'s component was deleted since the last run of this system.\
    /// Outside workloads returns `true` if `entity`'s component was deleted since the last call to [`clear_all_deleted`](SparseSet::clear_all_deleted).\
    /// Returns `false` if `entity` does not have a component in this storage.
    #[inline]
    pub fn is_deleted(&self, entity: EntityId) -> bool {
        Track::<TRACK>::is_deleted(self, entity, self.last_removal_or_deletion, self.current)
    }
    /// Returns the *deleted* components of a storage tracking deletion.
    pub fn deleted(&self) -> impl Iterator<Item = (EntityId, &T)> + '_ {
        self.sparse_set
            .deletion_data
            .iter()
            .filter_map(move |(entity, timestamp, component)| {
                if is_track_within_bounds(*timestamp, self.last_removal_or_deletion, self.current) {
                    Some((*entity, component))
                } else {
                    None
                }
            })
    }
}

impl<TRACK, T: Component> ViewMut<'_, T, TRACK>
where
    Track<TRACK>: RemovalTracking,
{
    /// Inside a workload returns `true` if `entity`'s component was removed since the last run of this system.\
    /// Outside workloads returns `true` if `entity`'s component was removed since the last call to [`clear_all_removed`](SparseSet::clear_all_removed).\
    /// Returns `false` if `entity` does not have a component in this storage.
    #[inline]
    pub fn is_removed(&self, entity: EntityId) -> bool {
        Track::<TRACK>::is_removed(self, entity, self.last_removal_or_deletion, self.current)
    }
    /// Returns the ids of *removed* components of a storage tracking removal.
    pub fn removed(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.sparse_set
            .removal_data
            .iter()
            .filter_map(move |(entity, timestamp)| {
                if is_track_within_bounds(*timestamp, self.last_removal_or_deletion, self.current) {
                    Some(*entity)
                } else {
                    None
                }
            })
    }
}

impl<TRACK, T: Component> ViewMut<'_, T, TRACK>
where
    Track<TRACK>: RemovalOrDeletionTracking,
{
    /// Inside a workload returns `true` if `entity`'s component was deleted or removed since the last run of this system.\
    /// Outside workloads returns `true` if `entity`'s component was deleted or removed since the last clear call.\
    /// Returns `false` if `entity` does not have a component in this storage.
    #[inline]
    pub fn is_removed_or_deleted(&self, entity: EntityId) -> bool {
        Track::<TRACK>::is_removed(self, entity, self.last_removal_or_deletion, self.current)
            || Track::<TRACK>::is_deleted(self, entity, self.last_removal_or_deletion, self.current)
    }
    /// Returns the ids of *removed* or *deleted* components of a storage tracking removal and/or deletion.
    pub fn removed_or_deleted(&self) -> impl Iterator<Item = EntityId> + '_ {
        Track::removed_or_deleted(self.sparse_set).filter_map(move |(entity, timestamp)| {
            if is_track_within_bounds(timestamp, self.last_removal_or_deletion, self.current) {
                Some(entity)
            } else {
                None
            }
        })
    }
}

impl<T: Component, TRACK> Deref for ViewMut<'_, T, TRACK> {
    type Target = SparseSet<T>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.sparse_set
    }
}

impl<T: Component, TRACK> DerefMut for ViewMut<'_, T, TRACK> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.sparse_set
    }
}

impl<'a, T: Component, TRACK> AsRef<SparseSet<T>> for ViewMut<'a, T, TRACK> {
    #[inline]
    fn as_ref(&self) -> &SparseSet<T> {
        self.sparse_set
    }
}

impl<'a, T: Component, TRACK> AsMut<SparseSet<T>> for ViewMut<'a, T, TRACK> {
    #[inline]
    fn as_mut(&mut self) -> &mut SparseSet<T> {
        self.sparse_set
    }
}

impl<'a, T: Component, TRACK> AsMut<Self> for ViewMut<'a, T, TRACK> {
    #[inline]
    fn as_mut(&mut self) -> &mut Self {
        self
    }
}

impl<T: fmt::Debug + Component, TRACK> fmt::Debug for ViewMut<'_, T, TRACK> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.sparse_set.fmt(f)
    }
}

impl<'a, T: Component, TRACK> core::ops::Index<EntityId> for ViewMut<'a, T, TRACK> {
    type Output = T;
    #[inline]
    fn index(&self, entity: EntityId) -> &Self::Output {
        self.get(entity).unwrap()
    }
}

impl<'a, T: Component, TRACK> core::ops::IndexMut<EntityId> for ViewMut<'a, T, TRACK> {
    #[inline]
    fn index_mut(&mut self, entity: EntityId) -> &mut Self::Output {
        let index = self
            .index_of(entity)
            .ok_or_else(|| error::MissingComponent {
                id: entity,
                name: core::any::type_name::<T>(),
            })
            .unwrap();

        let SparseSet {
            data,
            modification_data,
            is_tracking_modification,
            ..
        } = self.sparse_set;

        if *is_tracking_modification {
            unsafe {
                *modification_data.get_unchecked_mut(index) = self.current;
            };
        }

        unsafe { data.get_unchecked_mut(index) }
    }
}
