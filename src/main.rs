fn main() {

    use columnar::{Push, HeapSize};

    let mut tree = tree::Tree { data: 0, kids: vec![] };
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
    let mut cols = tree::ColumnTree::new();
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
    let sum = cols.values.iter().sum::<usize>();
    let time = timer.elapsed();
    println!("{:?}\tcols summed: {:?}", time, sum);

    let timer = std::time::Instant::now();
    let _ = cols.clone();
    let time = timer.elapsed();
    println!("{:?}\tcols cloned", time);

    use std::fs::File;
    use serde_json::Value as Json;

    let timer = std::time::Instant::now();
    // let f = File::open("cities.json.txt").unwrap();
    let f = File::open("true.txt").unwrap();
    let records: Vec<Json> = serde_json::from_reader(f).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tread {} json records", time, records.len());

    let timer = std::time::Instant::now();
    let _ = records.clone();
    let time = timer.elapsed();
    println!("{:?}\tjson_vals cloned", time);

    let values = records.clone().into_iter().map(Value::from_json).collect::<Vec<_>>();

    let timer = std::time::Instant::now();
    let mut json_cols = ColumnValue::default();
    json_cols.extend(values.iter());
    let time = timer.elapsed();
    println!("{:?}\tjson_cols formed", time);
    println!("\t\tjson_cols heapsize: {:?}", json_cols.heap_size());
    println!("\t\tjson_cols.roots:    {:?}", json_cols.roots.heap_size());
    println!("\t\tjson_cols.numbers:  {:?}", json_cols.numbers.heap_size());
    println!("\t\tjson_cols.strings:  {:?}", json_cols.strings.heap_size());
    println!("\t\tjson_cols.strings.bounds:  {:?}", json_cols.strings.bounds.heap_size());
    println!("\t\tjson_cols.strings.values:  {:?}", json_cols.strings.values.heap_size());
    println!("\t\tjson_cols.arrays:   {:?}", json_cols.arrays.heap_size());
    println!("\t\tjson_cols.objects:  {:?}", json_cols.objects.heap_size());
    println!("\t\tjson_cols.objects.values.0:  {:?}", json_cols.objects.values.0.heap_size());
    println!("\t\tjson_cols.objects.values.1:  {:?}", json_cols.objects.values.1.heap_size());

    let timer = std::time::Instant::now();
    for (index, value) in values.iter().enumerate() {
        if json_cols.get(index) != *value {
            println!("Mismatch: {:?}: {:?}", index, value);
        }
    }
    let time = timer.elapsed();
    println!("{:?}\tcompared", time);

    let timer = std::time::Instant::now();
    let _ = json_cols.clone();
    let time = timer.elapsed();
    println!("{:?}\tjson_cols cloned", time);

    let timer = std::time::Instant::now();
    use serde::ser::Serialize;
    let mut encoded0 = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut encoded0).with_bytes(rmp_serde::config::BytesMode::ForceAll);
    values.serialize(&mut serializer).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_vals encode ({} bytes; msgpack)", time, encoded0.len());
    let timer = std::time::Instant::now();
    let decoded0: Vec<Value> = rmp_serde::from_slice(&encoded0[..]).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_vals decode", time);

    let timer = std::time::Instant::now();
    let mut encoded1 = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut encoded1).with_bytes(rmp_serde::config::BytesMode::ForceAll);
    json_cols.serialize(&mut serializer).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_cols encode ({} bytes; msgpack)", time, encoded1.len());
    let timer = std::time::Instant::now();
    let decoded1: ColumnValue = rmp_serde::from_slice(&encoded1[..]).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_cols decode", time);
    
    let timer = std::time::Instant::now();
    let encoded2: Vec<u8> = bincode::serialize(&json_cols).unwrap();
    let time = timer.elapsed();
    println!("{:?}\tjson_cols encode ({} bytes; bincode)", time, encoded2.len());

    assert_eq!(values, decoded0);
    assert_eq!(json_cols, decoded1);

}

pub mod tree {

    #[derive(Clone)]
    pub struct Tree<T> {
        pub data: T,
        pub kids: Vec<Tree<T>>,
    }

    impl Tree<usize> {
        pub fn sum(&self) -> usize {
            self.data + self.kids.iter().map(|x| x.sum()).sum::<usize>()
        }
    }

    /// A stand-in for `Vec<Tree<T>>`
    #[derive(Clone)]
    pub struct ColumnTree<T> {
        pub groups: Vec<usize>,     // inserted tree delimiters.
        pub bounds: Vec<usize>,     // node child delimiters.
        pub values: Vec<T>,         // node values. 
    }

    /// A stand-in for `&Tree<T>`
    pub struct ColumnTreeRef<'a, T> {
        value: &'a T,
        lower: usize,
        upper: usize,
        nodes: &'a ColumnTree<T>
    }

    impl<'a, T> Clone for ColumnTreeRef<'a, T> { 
        fn clone(&self) -> Self { *self }
    }
    impl<'a, T> Copy for ColumnTreeRef<'a, T> { }

    impl<'a, T> ColumnTreeRef<'a, T> {
        pub fn value(&self) -> &T { self.value }
        pub fn child(&self, index: usize) -> ColumnTreeRef<'a, T> {
            assert!(index < self.upper - self.lower);
            let child = self.lower + index;
            ColumnTreeRef {
                value: &self.nodes.values[child],
                lower: self.nodes.bounds[child],
                upper: self.nodes.bounds[child+1],
                nodes: self.nodes,
            }
        }
        pub fn kids(&self) -> usize {
            self.upper - self.lower
        }
    }

    impl<'a, T: PartialEq> PartialEq<Tree<T>> for ColumnTreeRef<'a, T> {
        fn eq(&self, other: &Tree<T>) -> bool {
            let mut todo = vec![(*self, other)];
            while let Some((this, that)) = todo.pop() {
                if this.value != &that.data { 
                    return false; 
                } else if (this.upper - this.lower) != that.kids.len() {
                    return false;
                } else {
                    for (index, child) in that.kids.iter().enumerate() {
                        todo.push((this.child(index), child));
                    }
                }
            }
            true
        }
    }

    impl<T> ColumnTree<T> {
        // Pushes a tree containing data onto `self`.
        pub fn push(&mut self, tree: Tree<T>) {

            // Our plan is to repeatedly transcribe tree nodes, enqueueing
            // any child nodes for transcription. When we do, we'll need to
            // know where they will be written, to leave a forward reference,
            // and to this end we'll "allocate" space as we go, with a counter.
            let mut todo = std::collections::VecDeque::default();
            todo.push_back(tree);
            while let Some(node) = todo.pop_front() {
                // children will land at positions in `self.values` determined
                // by its current length, plus `todo.len()`, plus one (for node).
                let cursor = self.values.len() + todo.len() + 1;
                self.values.push(node.data);
                self.bounds.push(cursor + node.kids.len());
                for child in node.kids.into_iter() {
                    todo.push_back(child);
                }
            }

            self.groups.push(self.values.len());
        }

        pub fn index(&self, index: usize) -> ColumnTreeRef<'_, T> {
            let root = self.groups[index];
            ColumnTreeRef {
                value: &self.values[root],
                lower: self.bounds[root]+1, // +1 because the root .. references itself at the moment.
                upper: self.bounds[root+1],
                nodes: self,
            }
        }

        pub fn new() -> Self {
            Self {
                groups: vec![0],
                bounds: vec![0],
                values: Vec::default(),
            }
        }
    }
}

pub use json::{ColumnValue, ColumnValueRef, Value};
pub mod json {

    use serde_json::Value as Json;

    use columnar::{Push, Len, Index, HeapSize};
    use columnar::{ColumnVec, ColumnString, ColumnLookback};

    /// Stand in for JSON, from `serde_json`.
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum Value {
        Null,
        Bool(bool),
        Number(serde_json::Number),
        String(String),
        Array(Vec<Value>),
        Object(Vec<(String, Value)>),
    }

    impl Value {
        pub fn from_json(json: Json) -> Self {
            match json {
                Json::Null => { Value::Null },
                Json::Bool(b) => { Value::Bool(b) },
                Json::Number(n) => { Value::Number(n) },
                Json::String(s) => { Value::String(s) },
                Json::Array(a) => { Value::Array(a.into_iter().map(Value::from_json).collect()) },
                Json::Object(o) => { 
                    let mut list: Vec<_> = o.into_iter().map(|(s,j)| (s, Value::from_json(j))).collect();
                    list.sort_by(|x,y| x.0.cmp(&y.0));
                    Value::Object(list) 
                },
            }
        }
    }

    /// Sum type indicating where to find the data for each variant.
    #[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum ValueIdx {
        Null,
        Bool(bool),
        Number(usize),
        String(usize),
        Array(usize),
        Object(usize),
    }

    impl HeapSize for ValueIdx {
        fn heap_size(&self) -> (usize, usize) { (0, 0) }
    }

    /// Stand-in for `Vec<Value>`.
    ///
    /// The `roots` vector indicates the root of each stored `Value`.
    /// The (transitive) contents of each `Value` are stored throughout,
    /// at locations that may not necessarily be found in `roots`.
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct ColumnValue {
        pub roots: Vec<ValueIdx>,               // Any `ValueIdx` container.
        // pub nulls: Vec<()>,                  // No need to store null values.
        // pub bools: Vec<bool>,                // No need to store bool values.
        pub numbers: Vec<serde_json::Number>,   // Any `Number` container.
        pub strings: ColumnString,
        pub arrays: ColumnVec<Vec<ValueIdx>>,
        pub objects: ColumnVec<(ColumnLookback<ColumnString>, Vec<ValueIdx>)>,
    }

    impl HeapSize for ColumnValue {
        fn heap_size(&self) -> (usize, usize) {
            let (l0, c0) = self.roots.heap_size();
            let (l1, c1) = self.numbers.heap_size();
            let (l2, c2) = self.strings.heap_size();
            let (l3, c3) = self.arrays.heap_size();
            let (l4, c4) = self.objects.heap_size();
            (l0 + l1 + l2 + l3 + l4, c0 + c1 + c2 + c3 + c4)
        }
    }

    /// Stand-in for `&'a Value`. 
    #[derive(Debug)]
    pub enum ColumnValueRef<'a> {
        Null,
        Bool(bool),
        Number(serde_json::Number),
        String(&'a str),
        Array(ArrRef<'a>),
        Object(ObjRef<'a>),
    }

    /// Stand-in for `&'a [Value]`
    #[derive(Debug)]
    pub struct ArrRef<'a> {
        /// Reference into `store.arrays`.
        pub index: usize,
        pub store: &'a ColumnValue,
    }

    /// Stand-in for `&'a [(String, Value)]`.
    #[derive(Debug)]
    pub struct ObjRef<'a> {
        /// Reference into `store.objects`.
        pub index: usize,
        pub store: &'a ColumnValue,
    }

    impl<'a> PartialEq<Value> for ColumnValueRef<'a> {
        fn eq(&self, other: &Value) -> bool {
            match (self, other) {
                (ColumnValueRef::Null, Value::Null) => { true },
                (ColumnValueRef::Bool(b0), Value::Bool(b1)) => { b0 == b1 },
                (ColumnValueRef::Number(n0), Value::Number(n1)) => { n0 == n1 },
                (ColumnValueRef::String(s0), Value::String(s1)) => { *s0 == s1 },
                (ColumnValueRef::Array(a0), Value::Array(a1)) => { 
                    let slice: columnar::Slice<&Vec<ValueIdx>> = a0.store.arrays.get(a0.index);
                    if slice.len() != a1.len() { println!("arr len mismatch: {:?} v {:?}", slice.len(), a1.len()); }
                    slice.len() == a1.len() && slice.iter().zip(a1).all(|(a,b)| a0.store.dereference(*a).eq(b))
                },
                (ColumnValueRef::Object(o0), Value::Object(o1)) => { 
                    let slice = o0.store.objects.get(o0.index);
                    if slice.len() != o1.len() { println!("obj len mismatch: {:?} v {:?}", slice.len(), o1.len()); }
                    slice.len() == o1.len() && slice.iter().zip(o1).all(|((xs, xv),(ys, yv))| xs == ys && o0.store.dereference(*xv).eq(yv))
                },
                _ => { false }
            }
        }
    }

    impl Push<Value> for ColumnValue {
        fn push(&mut self, value: Value) {
            let mut worker = ValueQueues::new_from(self);
            let value_idx = worker.copy(&value);
            worker.store.roots.push(value_idx);
            worker.finish();
        }
        // It would be nice to implement `extend`, but lifetimes seem to prevent this.
        // Because the iterator produces owned content, we would need to collect the values
        // so that their lifetimes can outlive the `ValueQueues` instance.
    }
    impl<'a> Push<&'a Value> for ColumnValue {
        fn push(&mut self, value: &'a Value) {
            let mut worker = ValueQueues::new_from(self);
            let value_idx = worker.copy(value);
            worker.store.roots.push(value_idx);
            worker.finish();
        }
        fn extend(&mut self, values: impl IntoIterator<Item=&'a Value>) {
            let mut worker = ValueQueues::new_from(self);
            for value in values {
                let value_idx = worker.copy(value);
                worker.store.roots.push(value_idx);
                worker.finish();
            }
        }
    }

    // NOTE: currently produce an ICE (internal compiler error):
    // impl columnar::Index for ColumnValue {
    //     type Ref<'a> = ColumnValueRef<'a>;
    //     fn get(&self, index: usize) -> Self::Ref<'_> {
    //         self.dereference(self.roots[index])
    //     }
    // }

    impl ColumnValue {
        pub fn get(&self, index: usize) -> ColumnValueRef<'_> {
            self.dereference(self.roots[index])
        }
        pub fn dereference(&self, index: ValueIdx) -> ColumnValueRef<'_> {
            match index {
                ValueIdx::Null => ColumnValueRef::Null,
                ValueIdx::Bool(i) => ColumnValueRef::Bool(i),
                ValueIdx::Number(i) => ColumnValueRef::Number(self.numbers.get(i).clone()),
                ValueIdx::String(i) => ColumnValueRef::String(self.strings.get(i)),
                ValueIdx::Array(i) => {
                    ColumnValueRef::Array(ArrRef {
                        index: i,
                        store: self,
                    })
                },
                ValueIdx::Object(i) => {
                    ColumnValueRef::Object(ObjRef {
                        index: i,
                        store: self,
                    })
                }
            }
        }
    }

    struct ValueQueues<'a> {
        arr_todo: std::collections::VecDeque<&'a [Value]>,
        obj_todo: std::collections::VecDeque<&'a [(String, Value)]>,
        store: &'a mut ColumnValue,
    }

    impl<'a> ValueQueues<'a> {
        /// Creates a new `ValueQueues` from a `ColumnValue`.
        fn new_from(store: &'a mut ColumnValue) -> Self {
            Self {
                arr_todo: Default::default(),
                obj_todo: Default::default(),
                store,
            }
        }

        /// Copies a value, into either the store or a queue.
        fn copy(&mut self, value: &'a Value) -> ValueIdx {
            match value {
                Value::Null => ValueIdx::Null,
                Value::Bool(b) => ValueIdx::Bool(*b),
                Value::Number(n) => {
                    self.store.numbers.push(n.clone());            
                    ValueIdx::Number(self.store.numbers.len() - 1)
                },
                Value::String(s) => {
                    self.store.strings.push(s);            
                    ValueIdx::String(self.store.strings.len() - 1)
                },
                Value::Array(a) => {
                    self.arr_todo.push_back(a);
                    ValueIdx::Array(self.store.arrays.len() + self.arr_todo.len() - 1)
                },
                Value::Object(o) => {
                    self.obj_todo.push_back(o);
                    ValueIdx::Object(self.store.objects.len() + self.obj_todo.len() - 1)
                },
            }
        }
        /// Drains all queues, so that `store` is fully populated.
        fn finish(&mut self) {
            let mut temp = Vec::default();
            while !self.arr_todo.is_empty() || !self.obj_todo.is_empty() {
                // Odd logic, but: need the queue to retain the element so that `self.copy` produces 
                // the correct indexes for any nested arrays.
                while let Some(values) = self.arr_todo.front().cloned() {
                    Extend::extend(&mut temp, values.iter().map(|v| self.copy(v)));
                    self.arr_todo.pop_front();
                    self.store.arrays.push_iter(temp.drain(..));
                }
                // Odd logic, but: need the queue to retain the element so that `self.copy` produces 
                // the correct indexes for any nested objects.
                while let Some(pairs) = self.obj_todo.front().cloned() {
                    Extend::extend(&mut temp, pairs.iter().map(|(_,v)| self.copy(v)));
                    self.obj_todo.pop_front();
                    self.store.objects.push_iter(temp.drain(..).zip(pairs).map(|(v,(s,_))| (s, v)));
                }
            }
        }
    }
}
