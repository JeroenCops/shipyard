use crate::atomic_refcell::{ExclusiveBorrow, SharedBorrow};
use crate::component::Local;
use crate::tracking::is_track_within_bounds;
use crate::local::LocalStorage;
use core::fmt;
use core::ops::{Deref, DerefMut};

/// Exclusive view over a local component storage.
pub struct LocalViewMut<'a, T: Local> {
    pub(crate) local: &'a mut LocalStorage<T>,
    pub(crate) _borrow: Option<ExclusiveBorrow<'a>>,
    pub(crate) _all_borrow: Option<SharedBorrow<'a>>,
    pub(crate) last_insertion: u32,
    pub(crate) last_modification: u32,
    pub(crate) current: u32,
}

impl<T: Local> LocalViewMut<'_, T> {
    /// Returns `true` if the component was inserted before the last [`clear_inserted`] call.  
    ///
    /// [`clear_inserted`]: Self::clear_inserted
    #[inline]
    pub fn is_inserted(&self) -> bool {
        is_track_within_bounds(self.local.insert, self.last_insertion, self.current)
    }
    /// Returns `true` if the component was modified since the last [`clear_modified`] call.  
    ///
    /// [`clear_modified`]: Self::clear_modified
    #[inline]
    pub fn is_modified(&self) -> bool {
        is_track_within_bounds(
            self.local.modification,
            self.last_modification,
            self.current,
        )
    }
    /// Returns `true` if the component was inserted or modified since the last [`clear_inserted`] or [`clear_modified`] call.  
    ///
    /// [`clear_inserted`]: Self::clear_inserted
    /// [`clear_modified`]: Self::clear_modified
    #[inline]
    pub fn is_inserted_or_modified(&self) -> bool {
        self.is_inserted() || self.is_modified()
    }
    /// Removes the *inserted* flag on the component of this storage.
    #[inline]
    pub fn clear_inserted(self) {
        self.local.last_insert = self.current;
    }
    /// Removes the *modified* flag on the component of this storage.
    #[inline]
    pub fn clear_modified(self) {
        self.local.last_modification = self.current;
    }
    /// Removes the *inserted* and *modified* flags on the component of this storage.
    #[inline]
    pub fn clear_inserted_and_modified(self) {
        self.local.last_insert = self.current;
        self.local.last_modification = self.current;
    }
}

impl<T: Local> Deref for LocalViewMut<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.local.value
    }
}

impl<T: Local> DerefMut for LocalViewMut<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.local.modification = self.current;

        &mut self.local.value
    }
}

impl<T: Local> AsRef<T> for LocalViewMut<'_, T> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.local.value
    }
}

impl<T: Local> AsMut<T> for LocalViewMut<'_, T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        self.local.modification = self.current;

        &mut self.local.value
    }
}

impl<T: fmt::Debug + Local> fmt::Debug for LocalViewMut<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.local.value.fmt(f)
    }
}
