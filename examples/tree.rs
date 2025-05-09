fn main() {

    use columnar::adts::tree::{Tree, Trees};

    let mut tree = Tree { data: 0, kids: vec![] };
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
    let clone = tree.clone();
    let time = timer.elapsed();
    println!("{:?}\ttree cloned", time);

    let timer = std::time::Instant::now();
    let mut cols = Trees::new();
    cols.push(tree);
    let time = timer.elapsed();
    println!("{:?}\tcols formed", time);

    let timer = std::time::Instant::now();
    if cols.index(0) != clone {
        println!("UNEQUAL!!!");
    }
    let time = timer.elapsed();
    println!("{:?}\tcompared", time);

    let timer = std::time::Instant::now();
    let sum = (&cols.values).iter().sum::<usize>();
    let time = timer.elapsed();
    println!("{:?}\tcols summed: {:?}", time, sum);

    let timer = std::time::Instant::now();
    let _ = cols.clone();
    let time = timer.elapsed();
    println!("{:?}\tcols cloned", time);
}
