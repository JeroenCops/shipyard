use crate::track;

/// Indicates that a `struct` or `enum` can be stored in the `World`.
pub trait Component: Sized + 'static {
    /// Specify what this storage should track.
    /// Can be one of: [`track::Untracked`], [`track::Insertion`], [`track::Modification`], [`track::Removal`], [`track::All`].
    type Tracking: track::Tracking;
}

impl<T: Component> Component for Option<T> {
    type Tracking = <T as Component>::Tracking;
}

/// Indicates that a `struct` or `enum` can be stored a single time in the `World`.
pub trait Unique: Sized + 'static {
    /// Specify what this storage should track.
    /// Can be one of: [`track::Untracked`], [`track::Insertion`], [`track::Modification`], [`track::Removal`], [`track::All`].
    type Tracking: track::Tracking;
}

impl<T: Unique> Unique for Option<T> {
    type Tracking = <T as Unique>::Tracking;
}

/// Indicates that a `struct` or `enum` can be stored a single time in the `System`.
pub trait Local: Sized + 'static + Default {
    /// Specify what this storage should track.
    /// Can be one of: [`track::Untracked`], [`track::Insertion`], [`track::Modification`], [`track::Removal`], [`track::All`].
    type Tracking: track::Tracking;
}

impl<T: Local> Local for Option<T> {
    type Tracking = <T as Local>::Tracking;
}