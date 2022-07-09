#[cfg(feature = "thread_local")]
use super::non_send::NonSend;
#[cfg(feature = "thread_local")]
use super::non_send_sync::NonSendSync;
#[cfg(feature = "thread_local")]
use super::non_sync::NonSync;
use super::Mutability;
use crate::all_storages::AllStorages;
use crate::component::{Component, Unique, Local};
use crate::entities::Entities;
use crate::scheduler::TypeInfo;
use crate::sparse_set::SparseSet;
use crate::storage::StorageId;
use crate::type_id::TypeId;
use crate::unique::UniqueStorage;
use crate::local::LocalStorage;
use crate::view::{
    AllStoragesView, AllStoragesViewMut, EntitiesView, EntitiesViewMut, UniqueView, UniqueViewMut,
    LocalViewMut, View, ViewMut,
};
use alloc::vec::Vec;
use core::any::type_name;

/// Explains to a workload which storage are borrowed by a system.
///
/// # Safety
///
/// Must accurately list everything borrowed.
///
/// ### Example of manual implementation:
/// ```rust
/// use shipyard::{BorrowInfo, info::TypeInfo, View, UniqueView, type_id::TypeId};
///
/// # struct Camera {}
/// # impl shipyard::Unique for Camera {
/// #     type Tracking = shipyard::track::Untracked;
/// # }
/// # struct Position {}
/// # impl shipyard::Component for Position {
/// #     type Tracking = shipyard::track::Untracked;
/// # }
/// #
/// struct CameraView<'v> {
///     camera: UniqueView<'v, Camera>,
///     position: View<'v, Position>,
/// }
///
/// // SAFE: All storages info is recorded.
/// unsafe impl BorrowInfo for CameraView<'_> {
///     fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
///         <UniqueView<'_, Camera>>::borrow_info(info, None);
///         <View<'_, Position>>::borrow_info(info, None);
///     }
/// }
/// ```
pub unsafe trait BorrowInfo {
    /// This information is used during workload creation to determine which systems can run in parallel.
    ///
    /// A borrow error might happen if the information is not correct.
    fn borrow_info(info: &mut Vec<TypeInfo>, system_id: Option<TypeId>);
}

unsafe impl BorrowInfo for () {
    fn borrow_info(_: &mut Vec<TypeInfo>, _: Option<TypeId>) {}
}

unsafe impl<'a> BorrowInfo for AllStoragesView<'a> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<AllStorages>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<AllStorages>(),
            #[cfg(not(feature = "thread_local"))]
            thread_safe: true,
            #[cfg(feature = "thread_local")]
            thread_safe: false,
        });
    }
}

unsafe impl<'a> BorrowInfo for AllStoragesViewMut<'a> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<AllStorages>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<AllStorages>(),
            #[cfg(not(feature = "thread_local"))]
            thread_safe: true,
            #[cfg(feature = "thread_local")]
            thread_safe: false,
        });
    }
}

unsafe impl<'a> BorrowInfo for EntitiesView<'a> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<Entities>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<Entities>(),
            thread_safe: true,
        });
    }
}

unsafe impl<'a> BorrowInfo for EntitiesViewMut<'a> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<Entities>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<Entities>(),
            thread_safe: true,
        });
    }
}

unsafe impl<'a, T: Send + Sync + Component> BorrowInfo for View<'a, T> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<SparseSet<T, T::Tracking>>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<SparseSet<T, T::Tracking>>(),
            thread_safe: true,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Sync + Component> BorrowInfo for NonSend<View<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<SparseSet<T, T::Tracking>>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<SparseSet<T, T::Tracking>>(),
            thread_safe: true,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Send + Component> BorrowInfo for NonSync<View<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<SparseSet<T, T::Tracking>>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<SparseSet<T, T::Tracking>>(),
            thread_safe: false,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Component> BorrowInfo for NonSendSync<View<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<SparseSet<T, T::Tracking>>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<SparseSet<T, T::Tracking>>(),
            thread_safe: false,
        });
    }
}

unsafe impl<'a, T: Send + Sync + Component> BorrowInfo for ViewMut<'a, T> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<SparseSet<T, T::Tracking>>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<SparseSet<T, T::Tracking>>(),
            thread_safe: true,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Sync + Component> BorrowInfo for NonSend<ViewMut<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<SparseSet<T, T::Tracking>>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<SparseSet<T, T::Tracking>>(),
            thread_safe: false,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Send + Component> BorrowInfo for NonSync<ViewMut<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<SparseSet<T, T::Tracking>>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<SparseSet<T, T::Tracking>>(),
            thread_safe: true,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Component> BorrowInfo for NonSendSync<ViewMut<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<SparseSet<T, T::Tracking>>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<SparseSet<T, T::Tracking>>(),
            thread_safe: false,
        });
    }
}

unsafe impl<'a, T: Send + Sync + Unique> BorrowInfo for UniqueView<'a, T> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<UniqueStorage<T>>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<UniqueStorage<T>>(),
            thread_safe: true,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Sync + Unique> BorrowInfo for NonSend<UniqueView<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<UniqueStorage<T>>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<UniqueStorage<T>>(),
            thread_safe: true,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Send + Unique> BorrowInfo for NonSync<UniqueView<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<UniqueStorage<T>>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<UniqueStorage<T>>(),
            thread_safe: false,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Unique> BorrowInfo for NonSendSync<UniqueView<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<UniqueStorage<T>>().into(),
            mutability: Mutability::Shared,
            storage_id: StorageId::of::<UniqueStorage<T>>(),
            thread_safe: false,
        });
    }
}

unsafe impl<'a, T: Send + Sync + Unique> BorrowInfo for UniqueViewMut<'a, T> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<UniqueStorage<T>>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<UniqueStorage<T>>(),
            thread_safe: true,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Sync + Unique> BorrowInfo for NonSend<UniqueViewMut<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<UniqueStorage<T>>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<UniqueStorage<T>>(),
            thread_safe: false,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Send + Unique> BorrowInfo for NonSync<UniqueViewMut<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<UniqueStorage<T>>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<UniqueStorage<T>>(),
            thread_safe: true,
        });
    }
}

#[cfg(feature = "thread_local")]
unsafe impl<'a, T: Unique> BorrowInfo for NonSendSync<UniqueViewMut<'a, T>> {
    fn borrow_info(info: &mut Vec<TypeInfo>, _system_id: Option<TypeId>) {
        info.push(TypeInfo {
            name: type_name::<UniqueStorage<T>>().into(),
            mutability: Mutability::Exclusive,
            storage_id: StorageId::of::<UniqueStorage<T>>(),
            thread_safe: false,
        });
    }
}

unsafe impl<'a, T: Send + Sync + Local> BorrowInfo for LocalViewMut<'a, T> {
    fn borrow_info(info: &mut Vec<TypeInfo>, system_id: Option<TypeId>) {
        if let Some(system_id) = system_id {
            info.push(TypeInfo {
                name: type_name::<LocalStorage<T>>().into(),
                mutability: Mutability::Exclusive,
                storage_id: StorageId::local_of::<LocalStorage<T>>(system_id),
                thread_safe: false,
            });
        } else {
            panic!("No local storage found for type: {}", type_name::<LocalStorage<T>>())
        }
    }
}

unsafe impl<T: BorrowInfo> BorrowInfo for Option<T> {
    fn borrow_info(info: &mut Vec<TypeInfo>, system_id: Option<TypeId>) {
        T::borrow_info(info, system_id);
    }
}

macro_rules! impl_borrow_info {
    ($(($type: ident, $index: tt))+) => {
        unsafe impl<$($type: BorrowInfo),+> BorrowInfo for ($($type,)+) {
            fn borrow_info(info: &mut Vec<TypeInfo>, system_id: Option<TypeId>) {
                $(
                    $type::borrow_info(info, system_id);
                )+
            }
        }
    }
}

macro_rules! borrow_info {
    ($(($type: ident, $index: tt))*;($type1: ident, $index1: tt) $(($queue_type: ident, $queue_index: tt))*) => {
        impl_borrow_info![$(($type, $index))*];
        borrow_info![$(($type, $index))* ($type1, $index1); $(($queue_type, $queue_index))*];
    };
    ($(($type: ident, $index: tt))*;) => {
        impl_borrow_info![$(($type, $index))*];
    }
}

borrow_info![(A, 0); (B, 1) (C, 2) (D, 3) (E, 4) (F, 5) (G, 6) (H, 7) (I, 8) (J, 9)];
