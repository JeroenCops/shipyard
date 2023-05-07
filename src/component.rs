/// Indicates that a `struct` or `enum` can be store in the `World`.
pub trait Component: Sized + 'static {}

/// Indicates that a `struct` or `enum` can be store a single time in the `World`.
pub trait Unique: Sized + 'static {}

/// Indicates that a `struct` or `enum` can be stored a single time in the `System`.
pub trait Local: Sized + 'static + Default {}