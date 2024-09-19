use serde_json::Value as JsonJson;

use crate::{Push, Len, IndexOwn, HeapSize};
use crate::{Vecs, Strings, Lookbacks};

/// Stand in for JSON, from `serde_json`.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Json {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl HeapSize for Json {
    fn heap_size(&self) -> (usize, usize) {
        match self {
            Json::Null => (0, 0),
            Json::Bool(_) => (0, 0),
            Json::Number(_) => (0, 0),
            Json::String(s) => (0, s.len()),
            Json::Array(a) => a.heap_size(),
            Json::Object(o) => o.heap_size(),
        }
    }
}

impl Json {
    pub fn from_json(json: JsonJson) -> Self {
        match json {
            JsonJson::Null => { Json::Null },
            JsonJson::Bool(b) => { Json::Bool(b) },
            JsonJson::Number(n) => { Json::Number(n) },
            JsonJson::String(s) => { Json::String(s) },
            JsonJson::Array(a) => { Json::Array(a.into_iter().map(Json::from_json).collect()) },
            JsonJson::Object(o) => {
                let mut list: Vec<_> = o.into_iter().map(|(s,j)| (s, Json::from_json(j))).collect();
                list.sort_by(|x,y| x.0.cmp(&y.0));
                Json::Object(list)
            },
        }
    }
}

/// Sum type indicating where to find the data for each variant.
#[derive(Copy, Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JsonIdx {
    Null,
    Bool(bool),
    Number(usize),
    String(usize),
    Array(usize),
    Object(usize),
}

impl HeapSize for JsonIdx {
    fn heap_size(&self) -> (usize, usize) { (0, 0) }
}

/// Stand-in for `Vec<Json>`.
///
/// This approach uses `indexes` which contains discriminants, which should allow
/// an efficient representation of offset information. Unfortunately, both `arrays`
/// and `objects` just list their intended offsets directly, rather than encode the
/// offsets using unary degree sequences, which seemed hard to thread through the 
/// other abstractions. Their `Vec<usize>` container can probably be made smarter,
/// in particular by an `Option<usize>` container where `None` indicates increment.
// struct Jsons {
//     pub indexes: Vec<JsonDiscriminant>,     // Container for `JsonDiscriminant`.
//     pub numbers: Vec<serde_json::Number>,   // Any `Number` container.
//     pub strings: Strings,                   // Any `String` container.
//     pub arrays: Vecs<Vec<usize>>,           // Any `Vec<usize>` container.
//     pub objects Vecs<(Lookbacks<Strings>, Vec<usize>)>,
// }

/// Stand-in for `Vec<Json>`.
///
/// The `roots` vector indicates the root of each stored `Json`.
/// The (transitive) contents of each `Json` are stored throughout,
/// at locations that may not necessarily be found in `roots`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Jsons {
    pub roots: Vec<JsonIdx>,               // Any `JsonIdx` container.
    // pub nulls: Vec<()>,                  // No need to store null Jsons.
    // pub bools: Vec<bool>,                // No need to store bool Jsons.
    pub numbers: Vec<serde_json::Number>,   // Any `Number` container.
    pub strings: Lookbacks<Strings>,
    pub arrays: Vecs<Vec<JsonIdx>>,
    pub objects: Vecs<(Lookbacks<Strings>, Vec<JsonIdx>)>,
}

impl HeapSize for Jsons {
    fn heap_size(&self) -> (usize, usize) {
        let (l0, c0) = self.roots.heap_size();
        let (l1, c1) = self.numbers.heap_size();
        let (l2, c2) = self.strings.heap_size();
        let (l3, c3) = self.arrays.heap_size();
        let (l4, c4) = self.objects.heap_size();
        (l0 + l1 + l2 + l3 + l4, c0 + c1 + c2 + c3 + c4)
    }
}

/// Stand-in for `&'a Json`.
#[derive(Debug)]
pub enum JsonsRef<'a> {
    Null,
    Bool(bool),
    Number(&'a serde_json::Number),
    String(&'a str),
    Array(ArrRef<'a>),
    Object(ObjRef<'a>),
}

/// Stand-in for `&'a [Json]`
#[derive(Debug)]
pub struct ArrRef<'a> {
    /// Reference into `store.arrays`.
    pub index: usize,
    pub store: &'a Jsons,
}

/// Stand-in for `&'a [(String, Json)]`.
#[derive(Debug)]
pub struct ObjRef<'a> {
    /// Reference into `store.objects`.
    pub index: usize,
    pub store: &'a Jsons,
}

impl<'a> PartialEq<Json> for JsonsRef<'a> {
    #[inline(always)] fn eq(&self, other: &Json) -> bool {
        match (self, other) {
            (JsonsRef::Null, Json::Null) => { true },
            (JsonsRef::Bool(b0), Json::Bool(b1)) => { b0 == b1 },
            (JsonsRef::Number(n0), Json::Number(n1)) => { *n0 == n1 },
            (JsonsRef::String(s0), Json::String(s1)) => { *s0 == s1 },
            (JsonsRef::Array(a0), Json::Array(a1)) => {
                let slice: crate::Slice<&Vec<JsonIdx>> = (&a0.store.arrays).get(a0.index);
                slice.len() == a1.len() && slice.iter().zip(a1).all(|(a,b)| a0.store.dereference(*a).eq(b))
            },
            (JsonsRef::Object(o0), Json::Object(o1)) => {
                let slice: crate::Slice<&(_, _)> = (&o0.store.objects).get(o0.index);
                slice.len() == o1.len() && slice.iter().zip(o1).all(|((xs, xv),(ys, yv))| xs == ys && o0.store.dereference(*xv).eq(yv))
            },
            _ => { false }
        }
    }
}

impl Push<Json> for Jsons {
    fn push(&mut self, json: Json) {
        let mut worker = JsonQueues::new_from(self);
        let json_idx = worker.copy(&json);
        worker.store.roots.push(json_idx);
        worker.finish();
    }
    // It would be nice to implement `extend`, but lifetimes seem to prevent this.
    // Because the iterator produces owned content, we would need to collect the Jsons
    // so that their lifetimes can outlive the `JsonQueues` instance.
}
impl<'a> Push<&'a Json> for Jsons {
    fn push(&mut self, json: &'a Json) {
        let mut worker = JsonQueues::new_from(self);
        let json_idx = worker.copy(json);
        worker.store.roots.push(json_idx);
        worker.finish();
    }
    fn extend(&mut self, jsons: impl IntoIterator<Item=&'a Json>) {
        let mut worker = JsonQueues::new_from(self);
        for json in jsons {
            let json_idx = worker.copy(json);
            worker.store.roots.push(json_idx);
            worker.finish();
        }
    }
}

impl Len for Jsons {
    fn len(&self) -> usize {
        self.roots.len()
    }
}

// impl IndexGat for Jsons {
//     type Ref<'a> = JsonsRef<'a>;
//     fn get(&self, index: usize) -> Self::Ref<'_> {
//         self.dereference(self.roots[index])
//     }
// }
impl<'a> IndexOwn for &'a Jsons {
    type Ref = JsonsRef<'a>;
    #[inline(always)] fn get(&self, index: usize) -> Self::Ref {
        self.dereference(self.roots[index])
    }
}

impl Jsons {
    #[inline(always)] pub fn dereference(&self, index: JsonIdx) -> JsonsRef<'_> {
        match index {
            JsonIdx::Null => JsonsRef::Null,
            JsonIdx::Bool(i) => JsonsRef::Bool(i),
            JsonIdx::Number(i) => JsonsRef::Number((&self.numbers).get(i)),
            JsonIdx::String(i) => JsonsRef::String((&self.strings).get(i)),
            JsonIdx::Array(i) => {
                JsonsRef::Array(ArrRef {
                    index: i,
                    store: self,
                })
            },
            JsonIdx::Object(i) => {
                JsonsRef::Object(ObjRef {
                    index: i,
                    store: self,
                })
            }
        }
    }
}

struct JsonQueues<'a> {
    arr_todo: std::collections::VecDeque<&'a [Json]>,
    obj_todo: std::collections::VecDeque<&'a [(String, Json)]>,
    store: &'a mut Jsons,
}

impl<'a> JsonQueues<'a> {
    /// Creates a new `JsonQueues` from a `Jsons`.
    fn new_from(store: &'a mut Jsons) -> Self {
        Self {
            arr_todo: Default::default(),
            obj_todo: Default::default(),
            store,
        }
    }

    /// Copies a Json, into either the store or a queue.
    fn copy(&mut self, json: &'a Json) -> JsonIdx {
        match json {
            Json::Null => JsonIdx::Null,
            Json::Bool(b) => JsonIdx::Bool(*b),
            Json::Number(n) => {
                self.store.numbers.push(n.clone());
                JsonIdx::Number(self.store.numbers.len() - 1)
            },
            Json::String(s) => {
                self.store.strings.push(s);
                JsonIdx::String(self.store.strings.len() - 1)
            },
            Json::Array(a) => {
                self.arr_todo.push_back(a);
                JsonIdx::Array(self.store.arrays.len() + self.arr_todo.len() - 1)
            },
            Json::Object(o) => {
                self.obj_todo.push_back(o);
                JsonIdx::Object(self.store.objects.len() + self.obj_todo.len() - 1)
            },
        }
    }
    /// Drains all queues, so that `store` is fully populated.
    fn finish(&mut self) {
        let mut temp = Vec::default();
        while !self.arr_todo.is_empty() || !self.obj_todo.is_empty() {
            // Odd logic, but: need the queue to retain the element so that `self.copy` produces
            // the correct indexes for any nested arrays.
            while let Some(json) = self.arr_todo.front().cloned() {
                Extend::extend(&mut temp, json.iter().map(|v| self.copy(v)));
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
