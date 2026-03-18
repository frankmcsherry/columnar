fn main() {

    use columnar::adts::tree::{Tree, Trees};
    use columnar::common::Index;
    use columnar::Borrow;

    let mut tree = Tree { data: 0usize, kids: vec![] };
    for i in 0 .. 11 {
        let mut kids = Vec::with_capacity(i);
        for _ in 0 .. i {
            kids.push(tree.clone());
        }
        tree.data = i;
        tree.kids = kids;
    }

    let timer = std::time::Instant::now();
    let sum = tree.sum();
    let time = timer.elapsed();
    println!("{:?}\ttree summed: {:?}", time, sum);

    let timer = std::time::Instant::now();
    let _clone = tree.clone();
    let time = timer.elapsed();
    println!("{:?}\ttree cloned", time);

    let timer = std::time::Instant::now();
    let mut cols: Trees<Vec<usize>> = Default::default();
    cols.push_tree(tree);
    let time = timer.elapsed();
    println!("{:?}\tcols formed", time);

    let timer = std::time::Instant::now();
    let borrowed = cols.borrow();
    let _root = borrowed.get(0);
    let time = timer.elapsed();
    println!("{:?}\tindexed", time);

    let timer = std::time::Instant::now();
    let sum: usize = cols.values.iter().copied().sum();
    let time = timer.elapsed();
    println!("{:?}\tcols summed: {:?}", time, sum);

    let timer = std::time::Instant::now();
    let _ = cols.clone();
    let time = timer.elapsed();
    println!("{:?}\tcols cloned", time);
}
