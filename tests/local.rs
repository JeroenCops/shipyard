use shipyard::*;

#[derive(Default)]
struct USIZE(usize);

impl Component for USIZE {
    type Tracking = track::Untracked;
}
impl Local for USIZE {
    type Tracking = track::Untracked;
}

#[test]
fn init_local_storage() {
    let world = World::new_with_custom_lock::<parking_lot::RawRwLock>();

    fn sys1(x: LocalViewMut<USIZE>) {
        assert_eq!(x.0, 0);
        println!("T of local view should be initialized by default value: {:?}", x.0);
    }

    Workload::new("Test")
        .with_system(sys1)
        .add_to_world(&world)
        .unwrap();

    let err = world.run_workload("Test");

    assert_eq!(err.is_err(), false);
}

#[test]
fn multiple_add_local_storage() {
    let world = World::new_with_custom_lock::<parking_lot::RawRwLock>();

    fn sys1(mut x: LocalViewMut<USIZE>) {
        x.0 += 1;
        assert_eq!(x.0, 1);
        println!("First system value should be 1: {}", x.0);
    }

    fn sys2(x: LocalViewMut<USIZE>) {
        assert_eq!(x.0, 0);
        println!("Second system value should be 0: {}", x.0);
    }

    Workload::new("Test")
        .with_system(sys1)
        .with_system(sys2)
        .add_to_world(&world)
        .unwrap();


    let err = world.run_workload("Test");

    assert_eq!(err.is_err(), false);
}

#[test]
fn run_same_system_twice() {
    let world = World::new_with_custom_lock::<parking_lot::RawRwLock>();

    fn sys1(mut x: LocalViewMut<USIZE>) {
        x.0 += 1;
        println!("First system value should be 1: {}", x.0);
    }

    Workload::new("Test")
        .with_system(sys1)
        .with_system(sys1)
        .add_to_world(&world)
        .unwrap();


    let err = world.run_workload("Test");

    assert_eq!(err.is_err(), false);
}

#[test]
fn default_local_storage() {
    let world = World::new_with_custom_lock::<parking_lot::RawRwLock>();

    // Type must implement Default trait.
    struct Value {
        v: usize,
    }

    impl Component for Value {
        type Tracking = track::Untracked;
    }

    impl Local for Value {
        type Tracking = track::Untracked;
    }

    impl Default for Value {
        fn default() -> Self {
            Self {
                v: 2,
            }
        }
    }

    fn sys1(x: LocalViewMut<Value>) {
        assert_eq!(x.v, 2);
        println!("Should have value of default trait: {}", x.v);
    }

    Workload::new("Test")
        .with_system(sys1)
        .add_to_world(&world)
        .unwrap();


    let err = world.run_workload("Test");

    assert_eq!(err.is_err(), false);
}

#[test]
fn multiple_run_local_storage() {
    let world = World::new_with_custom_lock::<parking_lot::RawRwLock>();

    fn sys1(mut x: LocalViewMut<USIZE>) {
        x.0 += 1;
        println!("Value in system x should add one for each run {}", x.0);
    }

    Workload::new("Test")
        .with_system(sys1)
        .add_to_world(&world)
        .unwrap();


    let err = world.run_workload("Test");

    assert_eq!(err.is_err(), false);

    let err = world.run_workload("Test");

    assert_eq!(err.is_err(), false);
}
