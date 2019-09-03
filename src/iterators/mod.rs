mod iter;

use crate::entity::Key;
use crate::not::Not;
use crate::sparse_array::{Pack, PackInfo, RawViewMut, View, ViewMut};
pub use iter::*;
use std::any::TypeId;

// This trait exists because of conflicting implementations
// when using std::iter::IntoIterator
/// Trait used to create iterators.
///
/// `std::iter::Iterator` can't be used because of conflicting implementation.
/// This trait serves as substitute.
pub trait IntoIter {
    type IntoIter;
    #[cfg(feature = "parallel")]
    type IntoParIter;
    /// Returns an iterator over storages yielding only components meeting the requirements.
    ///
    /// Iterators can only be made inside [run] closure and systems.
    /// # Example
    /// ```
    /// # use shipyard::*;
    /// let world = World::new::<(usize, u32)>();
    /// world.run::<(EntitiesMut, &mut usize, &mut u32), _>(|(mut entities, mut usizes, mut u32s)| {
    ///     entities.add_entity((&mut usizes, &mut u32s), (0usize, 1u32));
    ///     entities.add_entity((&mut usizes, &mut u32s), (2usize, 3u32));
    ///     for (x, &y) in (usizes, &u32s).iter() {
    ///         *x += y as usize;
    ///     }
    /// });
    /// ```
    /// [run]: ../struct.World.html#method.run
    fn iter(self) -> Self::IntoIter;
    /// Returns a parallel iterator over storages yielding only components meeting the requirements.
    ///
    /// Iterators can only be made inside [run] closure and systems.
    /// # Example
    /// ```
    /// # use shipyard::*;
    /// use rayon::prelude::ParallelIterator;
    ///
    /// let world = World::new::<(usize, u32)>();
    /// world.run::<(EntitiesMut, &mut usize, &mut u32, ThreadPool), _>(|(mut entities, mut usizes, mut u32s, thread_pool)| {
    ///     entities.add_entity((&mut usizes, &mut u32s), (0usize, 1u32));
    ///     entities.add_entity((&mut usizes, &mut u32s), (2usize, 3u32));
    ///     thread_pool.install(|| {
    ///         (usizes, &u32s).par_iter().for_each(|(x, &y)| {
    ///             *x += y as usize;
    ///         });
    ///     })
    /// });
    /// ```
    /// [run]: ../struct.World.html#method.run
    #[cfg(feature = "parallel")]
    fn par_iter(self) -> Self::IntoParIter;
}

// Allows to make ViewMut's sparse and dense fields immutable
// This is necessary to index into them
#[doc(hidden)]
#[allow(clippy::len_without_is_empty)]
pub trait IntoAbstract {
    type AbsView: AbstractMut;
    type PackType;
    fn into_abstract(self) -> Self::AbsView;
    fn len(&self) -> Option<usize>;
    fn pack_info(&self) -> &PackInfo<Self::PackType>;
    fn type_id(&self) -> TypeId;
    fn modified(&self) -> usize;
}

impl<'a, T: 'static + Send + Sync> IntoAbstract for View<'a, T> {
    type AbsView = Self;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        self
    }
    fn len(&self) -> Option<usize> {
        Some(View::len(&self))
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a, T: 'static + Send + Sync> IntoAbstract for &View<'a, T> {
    type AbsView = Self;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        self
    }
    fn len(&self) -> Option<usize> {
        Some(View::len(&self))
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a, T: 'static + Send + Sync> IntoAbstract for ViewMut<'a, T> {
    type AbsView = RawViewMut<'a, T>;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        self.into_raw()
    }
    fn len(&self) -> Option<usize> {
        Some(ViewMut::len(&self))
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        match &self.pack_info.pack {
            Pack::Update(pack) => pack.inserted + pack.modified - 1,
            _ => std::usize::MAX,
        }
    }
}

impl<'a: 'b, 'b, T: 'static + Send + Sync> IntoAbstract for &'b ViewMut<'a, T> {
    type AbsView = View<'b, T>;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        self.as_non_mut()
    }
    fn len(&self) -> Option<usize> {
        Some(ViewMut::len(&self))
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a: 'b, 'b, T: 'static + Send + Sync> IntoAbstract for &'b mut ViewMut<'a, T> {
    type AbsView = RawViewMut<'b, T>;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        self.raw()
    }
    fn len(&self) -> Option<usize> {
        Some(ViewMut::len(&self))
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        match &self.pack_info.pack {
            Pack::Update(pack) => pack.inserted + pack.modified - 1,
            _ => std::usize::MAX,
        }
    }
}

impl<'a, T: 'static + Send + Sync> IntoAbstract for Not<View<'a, T>> {
    type AbsView = Self;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        self
    }
    fn len(&self) -> Option<usize> {
        None
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.0.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a, T: 'static + Send + Sync> IntoAbstract for &Not<View<'a, T>> {
    type AbsView = Self;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        self
    }
    fn len(&self) -> Option<usize> {
        None
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.0.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a, T: 'static + Send + Sync> IntoAbstract for Not<&View<'a, T>> {
    type AbsView = Self;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        self
    }
    fn len(&self) -> Option<usize> {
        None
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.0.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a, T: 'static + Send + Sync> IntoAbstract for Not<ViewMut<'a, T>> {
    type AbsView = Not<RawViewMut<'a, T>>;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        Not(self.0.into_raw())
    }
    fn len(&self) -> Option<usize> {
        None
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.0.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a: 'b, 'b, T: 'static + Send + Sync> IntoAbstract for &'b Not<ViewMut<'a, T>> {
    type AbsView = Not<View<'b, T>>;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        Not(self.0.as_non_mut())
    }
    fn len(&self) -> Option<usize> {
        None
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.0.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a: 'b, 'b, T: 'static + Send + Sync> IntoAbstract for &'b mut Not<ViewMut<'a, T>> {
    type AbsView = Not<RawViewMut<'b, T>>;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        Not(self.0.raw())
    }
    fn len(&self) -> Option<usize> {
        None
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.0.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a: 'b, 'b, T: 'static + Send + Sync> IntoAbstract for Not<&'b ViewMut<'a, T>> {
    type AbsView = Not<View<'b, T>>;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        Not(self.0.as_non_mut())
    }
    fn len(&self) -> Option<usize> {
        None
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.0.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a: 'b, 'b, T: 'static + Send + Sync> IntoAbstract for Not<&'b mut ViewMut<'a, T>> {
    type AbsView = Not<RawViewMut<'b, T>>;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        Not(self.0.raw())
    }
    fn len(&self) -> Option<usize> {
        None
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        &self.0.pack_info
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        std::usize::MAX
    }
}

impl<'a, T: 'static + Send + Sync> IntoAbstract for RawViewMut<'a, T> {
    type AbsView = RawViewMut<'a, T>;
    type PackType = T;
    fn into_abstract(self) -> Self::AbsView {
        self
    }
    fn len(&self) -> Option<usize> {
        Some(self.len)
    }
    fn pack_info(&self) -> &PackInfo<Self::PackType> {
        unsafe { &*self.pack_info }
    }
    fn type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }
    fn modified(&self) -> usize {
        match unsafe { &(*self.pack_info).pack } {
            Pack::Update(pack) => pack.inserted + pack.modified - 1,
            _ => std::usize::MAX,
        }
    }
}

// Abstracts different types of view to iterate over
// mutable and immutable views with the same iterator
#[doc(hidden)]
pub trait AbstractMut: Clone + Send {
    type Out;
    type Slice;
    // # Safety
    // The lifetime has to be valid
    unsafe fn abs_get(&mut self, entity: Key) -> Option<Self::Out>;
    // # Safety
    // The lifetime has to be valid
    unsafe fn abs_get_unchecked(&mut self, entity: Key) -> Self::Out;
    // # Safety
    // The lifetime has to be valid
    unsafe fn get_data(&mut self, index: usize) -> Self::Out;
    // # Safety
    // The lifetime has to be valid
    unsafe fn get_data_slice(&mut self, indices: std::ops::Range<usize>) -> Self::Slice;
    fn indices(&self) -> *const Key;
    unsafe fn mark_modified(&mut self, index: usize) -> Self::Out;
}

impl<'a, T: Send + Sync> AbstractMut for View<'a, T> {
    type Out = &'a T;
    type Slice = &'a [T];
    unsafe fn abs_get(&mut self, entity: Key) -> Option<Self::Out> {
        if self.contains(entity) {
            Some(
                self.data
                    .get_unchecked(*self.sparse.get_unchecked(entity.index())),
            )
        } else {
            None
        }
    }
    unsafe fn abs_get_unchecked(&mut self, entity: Key) -> Self::Out {
        self.data
            .get_unchecked(*self.sparse.get_unchecked(entity.index()))
    }
    unsafe fn get_data(&mut self, index: usize) -> Self::Out {
        &*self.data.as_ptr().add(index)
    }
    unsafe fn get_data_slice(&mut self, indices: std::ops::Range<usize>) -> Self::Slice {
        &std::slice::from_raw_parts(
            self.data.as_ptr().add(indices.start),
            indices.end - indices.start,
        )
    }
    fn indices(&self) -> *const Key {
        self.dense.as_ptr()
    }
    unsafe fn mark_modified(&mut self, index: usize) -> Self::Out {
        self.get_data(index)
    }
}

impl<'a, T: Send + Sync> AbstractMut for &View<'a, T> {
    type Out = &'a T;
    type Slice = &'a [T];
    unsafe fn abs_get(&mut self, entity: Key) -> Option<Self::Out> {
        if self.contains(entity) {
            Some(
                self.data
                    .get_unchecked(*self.sparse.get_unchecked(entity.index())),
            )
        } else {
            None
        }
    }
    unsafe fn abs_get_unchecked(&mut self, entity: Key) -> Self::Out {
        self.data
            .get_unchecked(*self.sparse.get_unchecked(entity.index()))
    }
    unsafe fn get_data(&mut self, index: usize) -> Self::Out {
        &*self.data.as_ptr().add(index)
    }
    unsafe fn get_data_slice(&mut self, indices: std::ops::Range<usize>) -> Self::Slice {
        std::slice::from_raw_parts(
            self.data.as_ptr().add(indices.start),
            indices.end - indices.start,
        )
    }
    fn indices(&self) -> *const Key {
        self.dense.as_ptr()
    }
    unsafe fn mark_modified(&mut self, index: usize) -> Self::Out {
        self.get_data(index)
    }
}

impl<'a, T: 'a + Send + Sync> AbstractMut for RawViewMut<'a, T> {
    type Out = &'a mut T;
    type Slice = &'a mut [T];
    unsafe fn abs_get(&mut self, entity: Key) -> Option<Self::Out> {
        if self.contains(entity) {
            Some(&mut *(self.data.add(*self.sparse.add(entity.index())) as *mut _))
        } else {
            None
        }
    }
    unsafe fn abs_get_unchecked(&mut self, entity: Key) -> Self::Out {
        &mut *(self.data.add(*self.sparse.add(entity.index())) as *mut _)
    }
    unsafe fn get_data(&mut self, index: usize) -> Self::Out {
        &mut *self.data.add(index)
    }
    unsafe fn get_data_slice(&mut self, indices: std::ops::Range<usize>) -> Self::Slice {
        std::slice::from_raw_parts_mut(self.data.add(indices.start), indices.end - indices.start)
    }
    fn indices(&self) -> *const Key {
        self.dense
    }
    unsafe fn mark_modified(&mut self, index: usize) -> Self::Out {
        match &mut (*self.pack_info).pack {
            Pack::Update(pack) => {
                if index >= pack.inserted + pack.modified {
                    std::ptr::swap(
                        self.dense.add(pack.inserted + pack.modified),
                        self.dense.add(index),
                    );
                    std::ptr::swap(
                        self.data.add(pack.inserted + pack.modified),
                        self.data.add(index),
                    );
                    *self
                        .sparse
                        .add((*self.dense.add(pack.inserted + pack.modified)).index()) = index;
                    *self.sparse.add((*self.dense.add(index)).index()) =
                        pack.inserted + pack.modified;
                    pack.modified += 1;
                    &mut *self.data.add(pack.inserted + pack.modified - 1)
                } else {
                    self.get_data(index)
                }
            }
            _ => self.get_data(index),
        }
    }
}

impl<'a, T: Send + Sync> AbstractMut for Not<View<'a, T>> {
    type Out = ();
    type Slice = ();
    unsafe fn abs_get(&mut self, entity: Key) -> Option<Self::Out> {
        if self.0.contains(entity) {
            None
        } else {
            Some(())
        }
    }
    unsafe fn abs_get_unchecked(&mut self, _: Key) -> Self::Out {}
    unsafe fn get_data(&mut self, _: usize) -> Self::Out {
        unreachable!()
    }
    unsafe fn get_data_slice(&mut self, _: std::ops::Range<usize>) -> Self::Slice {
        unreachable!()
    }
    fn indices(&self) -> *const Key {
        unreachable!()
    }
    unsafe fn mark_modified(&mut self, index: usize) -> Self::Out {
        self.get_data(index)
    }
}

impl<'a, T: Send + Sync> AbstractMut for &Not<View<'a, T>> {
    type Out = ();
    type Slice = ();
    unsafe fn abs_get(&mut self, entity: Key) -> Option<Self::Out> {
        if self.0.contains(entity) {
            None
        } else {
            Some(())
        }
    }
    unsafe fn abs_get_unchecked(&mut self, _: Key) -> Self::Out {}
    unsafe fn get_data(&mut self, _: usize) -> Self::Out {
        unreachable!()
    }
    unsafe fn get_data_slice(&mut self, _: std::ops::Range<usize>) -> Self::Slice {
        unreachable!()
    }
    fn indices(&self) -> *const Key {
        unreachable!()
    }
    unsafe fn mark_modified(&mut self, index: usize) -> Self::Out {
        self.get_data(index)
    }
}

impl<'a, T: Send + Sync> AbstractMut for Not<&View<'a, T>> {
    type Out = ();
    type Slice = ();
    unsafe fn abs_get(&mut self, entity: Key) -> Option<Self::Out> {
        if self.0.contains(entity) {
            None
        } else {
            Some(())
        }
    }
    unsafe fn abs_get_unchecked(&mut self, _: Key) -> Self::Out {}
    unsafe fn get_data(&mut self, _: usize) -> Self::Out {
        unreachable!()
    }
    unsafe fn get_data_slice(&mut self, _: std::ops::Range<usize>) -> Self::Slice {
        unreachable!()
    }
    fn indices(&self) -> *const Key {
        unreachable!()
    }
    unsafe fn mark_modified(&mut self, index: usize) -> Self::Out {
        self.get_data(index)
    }
}

impl<'a, T: Send + Sync> AbstractMut for Not<RawViewMut<'a, T>> {
    type Out = ();
    type Slice = ();
    unsafe fn abs_get(&mut self, entity: Key) -> Option<Self::Out> {
        if self.0.contains(entity) {
            None
        } else {
            Some(())
        }
    }
    unsafe fn abs_get_unchecked(&mut self, _: Key) -> Self::Out {}
    unsafe fn get_data(&mut self, _: usize) -> Self::Out {
        unreachable!()
    }
    unsafe fn get_data_slice(&mut self, _: std::ops::Range<usize>) -> Self::Slice {
        unreachable!()
    }
    fn indices(&self) -> *const Key {
        unreachable!()
    }
    unsafe fn mark_modified(&mut self, index: usize) -> Self::Out {
        self.get_data(index)
    }
}
