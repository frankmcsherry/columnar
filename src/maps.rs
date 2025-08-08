//! Containers for `Vec<(K, V)>` that form columns by `K` keys.

use crate::{Len, Push};
use crate::Options;

/// A container for `Vec<(K, V)>` items.
///
/// Each inserted map is expected to have one `val` for any `key`.
/// Each is stored with `None` variants for absent keys. As such,
/// this type is not meant for large sparse key spaces.
#[allow(dead_code)]
pub struct KeyMaps<CK, CV> {
    _keys: CK,
    vals: Vec<CV>,
}

impl<CK, CV: Len> Len for KeyMaps<CK, CV> {
    fn len(&self) -> usize {
        // This .. behaves badly if we have no keys.
        self.vals[0].len()
    }
}

// Should this implementation preserve the order of the key-val pairs?
// That might want an associated `Vec<usize>` for each, to order the keys.
// If they are all identical, it shouldn't take up any space, though.
impl<K: PartialOrd, V, CV: Push<K>> Push<Vec<(K, V)>> for KeyMaps<Vec<K>, CV> {
    #[inline]
    fn push(&mut self, _item: Vec<(K, V)>) {

    }
}

/// A container for `Vec<K>` items sliced by index.
///
/// The container puts each `item[i]` element into the `i`th column.
#[allow(dead_code)]
pub struct ListMaps<CV> {
    vals: Vec<Options<CV>>,
}

impl<CV> Default for ListMaps<CV> {
    fn default() -> Self {
        ListMaps { vals: Default::default() }
    }
}

impl<CV: Len> Len for ListMaps<CV> {
    fn len(&self) -> usize {
        self.vals[0].len()
    }
}

impl<'a, V, CV: Push<&'a V> + Len + Default> Push<&'a Vec<V>> for ListMaps<CV> {
    #[inline]
    fn push(&mut self, item: &'a Vec<V>) {
        let mut item_len = item.len();
        let self_len = if self.vals.is_empty() { 0 } else { self.vals[0].len() };
        while self.vals.len() < item_len {
            let mut new_store: Options<CV> = Default::default();
            for _ in 0..self_len {
                new_store.push(None);
            }
            self.vals.push(new_store);
        }
        for (store, i) in self.vals.iter_mut().zip(item) {
            store.push(Some(i));
        }
        while item_len < self.vals.len() {
            self.vals[item_len].push(None);
            item_len += 1;
        }
    }
}

#[cfg(test)]
mod test {

    use crate::common::{Len, Push};
    use crate::{Results, Strings};

    #[test]
    fn round_trip_listmap() {

        // Each record is a list, of first homogeneous elements, and one heterogeneous.
        let records = (0 .. 1024).map(|i|
            vec![
                Ok(i),
                Err(format!("{:?}", i)),
                if i % 2 == 0 { Ok(i) } else { Err(format!("{:?}", i)) },
            ]
        );

        // We'll stash all the records in the store, which expects them.
        let mut store: super::ListMaps<Results<Vec<i32>, Strings>> = Default::default();
        for record in records {
            store.push(&record);
        }

        // Demonstrate type-safe restructuring.
        // We expect the first two columns to be homogenous, and the third to be mixed.
        let field0: Option<&[i32]> = if store.vals[0].somes.oks.len() == store.vals[0].len() {
            Some(&store.vals[0].somes.oks)
        } else { None };

        let field1: Option<&Strings> = if store.vals[1].somes.errs.len() == store.vals[1].len() {
            Some(&store.vals[1].somes.errs)
        } else { None };

        let field2: Option<&[i32]> = if store.vals[2].somes.oks.len() == store.vals[2].len() {
            Some(&store.vals[2].somes.oks)
        } else { None };

        assert!(field0.is_some());
        assert!(field1.is_some());
        assert!(field2.is_none());
    }
}
