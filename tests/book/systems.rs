use super::U32;
use shipyard::{
    AddComponent, AllStoragesViewMut, Component, EntitiesViewMut, IntoIter, IntoWithId,
    IntoWorkload, SparseSet, View, ViewMut, Workload, World,
};

#[derive(Component)]
struct Position;
#[derive(Component)]
struct Health;

#[derive(Component)]
enum Season {
    Spring,
}

#[derive(Component)]
struct Precipitation(f32);

#[allow(unused)]
// ANCHOR: create_ints
fn create_ints(mut entities: EntitiesViewMut, mut u32s: ViewMut<U32>) {
    // -- snip --
}
// ANCHOR_END: create_ints

#[test]
#[rustfmt::skip]
fn run() {
// ANCHOR: run
let world = World::new();

world.run(create_ints);
// ANCHOR_END: run
}

#[test]
#[allow(unused)]
#[rustfmt::skip]
fn single_run_with_data() {
// ANCHOR: in_acid
fn in_acid(season: Season, positions: View<Position>, mut healths: ViewMut<Health>) {
    // -- snip --
}

let world = World::new();

world.run_with_data(in_acid, Season::Spring);
// ANCHOR_END: in_acid
}

#[test]
#[allow(unused)]
#[rustfmt::skip]
fn multiple_run_with_data() {
// ANCHOR: in_acid_multiple
fn in_acid(
    (season, precipitation): (Season, Precipitation),
    positions: View<Position>,
    mut healths: ViewMut<Health>,
) {
    // -- snip --
}

let world = World::new();

world
    .run_with_data(in_acid, (Season::Spring, Precipitation(0.1)));
// ANCHOR_END: in_acid_multiple
}

#[test]
#[allow(unused)]
#[rustfmt::skip]
fn workload() {
// ANCHOR: workload
fn create_ints(mut entities: EntitiesViewMut, mut u32s: ViewMut<U32>) {
    // -- snip --
}

fn delete_ints(mut u32s: ViewMut<U32>) {
    // -- snip --
}

let world = World::new();

Workload::new("Int cycle")
    .with_system(create_ints)
    .with_system(delete_ints)
    .add_to_world(&world)
    .unwrap();

world.run_workload("Int cycle").unwrap();
// ANCHOR_END: workload
}

#[test]
#[rustfmt::skip]
fn workload_nesting() {
// ANCHOR: nested_workload
#[derive(Component)]
struct Dead<T: 'static>(core::marker::PhantomData<T>);

fn increment(mut u32s: ViewMut<U32>) {
    for mut i in (&mut u32s).iter() {
        i.0 += 1;
    }
}

fn flag_deleted_u32s(u32s: View<U32>, mut deads: ViewMut<Dead<u32>>) {
    for (id, i) in u32s.iter().with_id() {
        if i.0 > 100 {
            deads.add_component_unchecked(id, Dead(core::marker::PhantomData));
        }
    }
}

fn clear_deleted_u32s(mut all_storages: AllStoragesViewMut) {
    all_storages.delete_any::<SparseSet<Dead<u32>>>();
}

fn filter_u32() -> Workload {
    (flag_deleted_u32s, clear_deleted_u32s).into_workload()
}

fn main_loop() -> Workload {
    (increment, filter_u32).into_workload()
}

let world = World::new();

world.add_workload(main_loop);

world.run_workload(main_loop).unwrap();
// ANCHOR_END: nested_workload
}
