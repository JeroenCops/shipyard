use crate::atomic_refcell::ARef;
use crate::borrow::Borrow;
use crate::error;
use crate::views::{AllStoragesView, AllStoragesViewMut};
use crate::world::World;
use crate::type_id::TypeId;

/// Allows a type to be borrowed by [`World::borrow`], [`World::run`] and workloads.
pub trait WorldBorrow {
    #[allow(missing_docs)]
    type WorldView<'a>;

    /// This function is where the actual borrowing happens.
    fn world_borrow(
        world: &World,
        system_id: Option<TypeId>,
        last_run: Option<u32>,
        current: u32,
    ) -> Result<Self::WorldView<'_>, error::GetStorage>;
}

impl<T: Borrow> WorldBorrow for T {
    type WorldView<'a> = <T as Borrow>::View<'a>;

    fn world_borrow(
        world: &World,
        system_id: Option<TypeId>,
        last_run: Option<u32>,
        current: u32,
    ) -> Result<Self::WorldView<'_>, error::GetStorage> {
        let (all_storages, all_borrow) = unsafe {
            ARef::destructure(
                world
                    .all_storages
                    .borrow()
                    .map_err(error::GetStorage::AllStoragesBorrow)?,
            )
        };

        T::borrow(all_storages, Some(all_borrow), system_id, last_run, current)
    }
}

impl WorldBorrow for AllStoragesView<'_> {
    type WorldView<'a> = AllStoragesView<'a>;

    fn world_borrow(
        world: &World,
        _system_id: Option<TypeId>,
        _last_run: Option<u32>,
        _current: u32,
    ) -> Result<Self::WorldView<'_>, error::GetStorage> {
        world
            .all_storages
            .borrow()
            .map(AllStoragesView)
            .map_err(error::GetStorage::AllStoragesBorrow)
    }
}

impl WorldBorrow for AllStoragesViewMut<'_> {
    type WorldView<'a> = AllStoragesViewMut<'a>;

    #[inline]
    fn world_borrow(
        world: &World,
        _system_id: Option<TypeId>,
        _last_run: Option<u32>,
        _current: u32,
    ) -> Result<Self::WorldView<'_>, error::GetStorage> {
        world
            .all_storages
            .borrow_mut()
            .map(AllStoragesViewMut)
            .map_err(error::GetStorage::AllStoragesBorrow)
    }
}
