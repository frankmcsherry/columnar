//! Columnar representations of tree-structured data.
//!
//! A `Tree<T>` is a node with a value of type `T` and a list of children.
//! `Trees<TC>` stores a collection of trees with columnar storage for node values.
use alloc::{vec::Vec, string::String};

use crate::{Borrow, Index, IndexAs, Len, Clear, Push};

/// A tree node with a value and children.
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

/// A stand-in for `Vec<Tree<T>>`, with columnar storage for node values.
///
/// Nodes are stored in BFS order. `groups` tracks tree boundaries (cumulative
/// node counts with a leading 0). `bounds` tracks child ranges per node
/// (also with a leading 0 sentinel).
#[derive(Copy, Clone)]
pub struct Trees<TC, BC = Vec<u64>> {
    /// Cumulative node counts: tree `i` starts at node `groups[i]`.
    pub groups: BC,
    /// Child delimiters: node `j`'s children are at `bounds[j]..bounds[j+1]`
    /// (root nodes use `bounds[j]+1..bounds[j+1]` to skip themselves).
    pub bounds: BC,
    /// Columnar container for node values in BFS order.
    pub values: TC,
}

impl<TC: Default> Default for Trees<TC> {
    fn default() -> Self {
        Self {
            groups: vec![0u64],
            bounds: vec![0u64],
            values: TC::default(),
        }
    }
}

/// A reference to a single node within a `Trees` container.
///
/// Holds a copy of the borrowed values and bounds containers,
/// plus the node's index and child range. Navigation to children
/// constructs new `TreesRef` values.
pub struct TreesRef<V, B> {
    index: usize,
    lower: usize,
    upper: usize,
    values: V,
    bounds: B,
}

impl<V: Copy, B: Copy> Clone for TreesRef<V, B> {
    fn clone(&self) -> Self { *self }
}
impl<V: Copy, B: Copy> Copy for TreesRef<V, B> {}

impl<V: Index, B: IndexAs<u64>> TreesRef<V, B> {
    /// The value at this node.
    #[inline(always)]
    pub fn value(&self) -> V::Ref {
        self.values.get(self.index)
    }
    /// The number of children of this node.
    #[inline(always)]
    pub fn kids(&self) -> usize {
        self.upper - self.lower
    }
}
impl<V: Index + Copy, B: IndexAs<u64> + Copy> TreesRef<V, B> {
    /// A reference to the `index`-th child of this node.
    #[inline(always)]
    pub fn child(&self, index: usize) -> Self {
        assert!(index < self.upper - self.lower);
        let child = self.lower + index;
        TreesRef {
            index: child,
            lower: self.bounds.index_as(child) as usize,
            upper: self.bounds.index_as(child + 1) as usize,
            values: self.values,
            bounds: self.bounds,
        }
    }
}

impl<TC, BC: Len> Len for Trees<TC, BC> {
    #[inline(always)]
    fn len(&self) -> usize { self.groups.len() - 1 }
}

impl<TC: Index + Copy, BC: IndexAs<u64> + Len + Copy> Index for Trees<TC, BC> {
    type Ref = TreesRef<TC, BC>;
    #[inline(always)]
    fn get(&self, index: usize) -> Self::Ref {
        let root = self.groups.index_as(index) as usize;
        TreesRef {
            index: root,
            lower: self.bounds.index_as(root) as usize + 1,
            upper: self.bounds.index_as(root + 1) as usize,
            values: self.values,
            bounds: self.bounds,
        }
    }
}

impl<'a, TC, BC: IndexAs<u64> + Len> Index for &'a Trees<TC, BC>
where
    &'a TC: Index,
    &'a BC: IndexAs<u64>,
{
    type Ref = TreesRef<&'a TC, &'a BC>;
    #[inline(always)]
    fn get(&self, index: usize) -> Self::Ref {
        let root = self.groups.index_as(index) as usize;
        TreesRef {
            index: root,
            lower: self.bounds.index_as(root) as usize + 1,
            upper: self.bounds.index_as(root + 1) as usize,
            values: &self.values,
            bounds: &self.bounds,
        }
    }
}

impl<TC: Borrow> Borrow for Trees<TC> {
    type Ref<'a> = TreesRef<TC::Borrowed<'a>, &'a [u64]> where TC: 'a;
    type Borrowed<'a> = Trees<TC::Borrowed<'a>, &'a [u64]> where TC: 'a;
    #[inline(always)]
    fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
        Trees {
            groups: &self.groups[..],
            bounds: &self.bounds[..],
            values: self.values.borrow(),
        }
    }
    #[inline(always)]
    fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where TC: 'a {
        Trees {
            groups: thing.groups,
            bounds: thing.bounds,
            values: TC::reborrow(thing.values),
        }
    }
    #[inline(always)]
    fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a {
        TreesRef {
            index: thing.index,
            lower: thing.lower,
            upper: thing.upper,
            values: TC::reborrow(thing.values),
            bounds: thing.bounds,
        }
    }
}

impl<TC: Clear> Clear for Trees<TC> {
    fn clear(&mut self) {
        self.groups.clear();
        self.groups.push(0u64);
        self.bounds.clear();
        self.bounds.push(0u64);
        self.values.clear();
    }
}

impl<TC: Len> Trees<TC> {
    /// Pushes a tree into the container, storing nodes in BFS order.
    pub fn push_tree<T>(&mut self, tree: Tree<T>) where TC: for<'a> Push<&'a T> {
        let mut todo = alloc::collections::VecDeque::default();
        todo.push_back(tree);
        while let Some(node) = todo.pop_front() {
            let cursor = self.values.len() + todo.len() + 1;
            self.values.push(&node.data);
            self.bounds.push((cursor + node.kids.len()) as u64);
            for child in node.kids.into_iter() {
                todo.push_back(child);
            }
        }
        self.groups.push(self.values.len() as u64);
    }
}

impl<'a, TC: crate::AsBytes<'a>, BC: crate::AsBytes<'a>> crate::AsBytes<'a> for Trees<TC, BC> {
    #[inline(always)]
    fn as_bytes(&self) -> impl Iterator<Item=(u64, &'a [u8])> {
        let iter = self.groups.as_bytes();
        let iter = crate::chain(iter, self.bounds.as_bytes());
        crate::chain(iter, self.values.as_bytes())
    }
}

impl<'a, TC: crate::FromBytes<'a>, BC: crate::FromBytes<'a>> crate::FromBytes<'a> for Trees<TC, BC> {
    const SLICE_COUNT: usize = BC::SLICE_COUNT + BC::SLICE_COUNT + TC::SLICE_COUNT;
    #[inline(always)]
    fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
        Self {
            groups: crate::FromBytes::from_bytes(bytes),
            bounds: crate::FromBytes::from_bytes(bytes),
            values: crate::FromBytes::from_bytes(bytes),
        }
    }
    #[inline(always)]
    fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
        Self {
            groups: BC::from_store(store, offset),
            bounds: BC::from_store(store, offset),
            values: TC::from_store(store, offset),
        }
    }
    fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
        BC::element_sizes(sizes)?;
        BC::element_sizes(sizes)?;
        TC::element_sizes(sizes)?;
        Ok(())
    }
}

/// LOUDS (level ordered unary degree sequence) is a succinct tree representation.
mod louds {

    // The tree is encoded by traversing it in a BFS order, with each node producing
    // as many `1`s as it has children, followed by a `0`. There is also a `1` at the
    // beginning of the sequence, for the root.
    //
    // The logic to determine the child of a node is as follows:
    //
    //      child(x, i) = select0(rank1(x) + i − 1) + 1
    //
    // It is possible that `i` here starts at 1, which we should fix to be `0`.

}

#[cfg(test)]
mod test {

    use alloc::{vec, vec::Vec, string::ToString};
    use crate::common::{Index, Len, Clear};
    use crate::{Borrow, AsBytes, FromBytes};
    use super::{Tree, Trees};

    fn leaf<T>(data: T) -> Tree<T> {
        Tree { data, kids: vec![] }
    }
    fn branch<T>(data: T, kids: Vec<Tree<T>>) -> Tree<T> {
        Tree { data, kids }
    }

    #[test]
    fn push_and_index() {
        let mut trees: Trees<Vec<u64>> = Default::default();
        // Tree: 10 -> [20, 30 -> [40]]
        let tree = branch(10u64, vec![leaf(20), branch(30, vec![leaf(40)])]);
        trees.push_tree(tree);

        assert_eq!(trees.len(), 1);

        let borrowed = trees.borrow();
        let root = borrowed.get(0);
        assert_eq!(*root.value(), 10);
        assert_eq!(root.kids(), 2);

        let c0 = root.child(0);
        assert_eq!(*c0.value(), 20);
        assert_eq!(c0.kids(), 0);

        let c1 = root.child(1);
        assert_eq!(*c1.value(), 30);
        assert_eq!(c1.kids(), 1);

        let c1_0 = c1.child(0);
        assert_eq!(*c1_0.value(), 40);
        assert_eq!(c1_0.kids(), 0);
    }

    #[test]
    fn multiple_trees() {
        let mut trees: Trees<Vec<u64>> = Default::default();
        trees.push_tree(branch(1u64, vec![leaf(2), leaf(3)]));
        trees.push_tree(leaf(100u64));
        trees.push_tree(branch(200u64, vec![leaf(300)]));

        assert_eq!(trees.len(), 3);

        let borrowed = trees.borrow();

        let t0 = borrowed.get(0);
        assert_eq!(*t0.value(), 1);
        assert_eq!(t0.kids(), 2);
        assert_eq!(*t0.child(0).value(), 2);
        assert_eq!(*t0.child(1).value(), 3);

        let t1 = borrowed.get(1);
        assert_eq!(*t1.value(), 100);
        assert_eq!(t1.kids(), 0);

        let t2 = borrowed.get(2);
        assert_eq!(*t2.value(), 200);
        assert_eq!(t2.kids(), 1);
        assert_eq!(*t2.child(0).value(), 300);
    }

    #[test]
    fn ref_index() {
        let mut trees: Trees<Vec<u64>> = Default::default();
        trees.push_tree(branch(1u64, vec![leaf(2)]));

        let root = (&trees).get(0);
        assert_eq!(*root.value(), 1);
        assert_eq!(*root.child(0).value(), 2);
    }

    #[test]
    fn clear_and_reuse() {
        let mut trees: Trees<Vec<u64>> = Default::default();
        trees.push_tree(leaf(42u64));
        assert_eq!(trees.len(), 1);

        trees.clear();
        assert_eq!(trees.len(), 0);

        trees.push_tree(leaf(99u64));
        assert_eq!(trees.len(), 1);
        assert_eq!(*trees.borrow().get(0).value(), 99);
    }

    #[test]
    fn as_from_bytes() {
        let mut trees: Trees<Vec<u64>> = Default::default();
        trees.push_tree(branch(10u64, vec![leaf(20), branch(30, vec![leaf(40)])]));
        trees.push_tree(leaf(100u64));

        let borrowed = trees.borrow();
        let rebuilt = Trees::<&[u64], &[u64]>::from_bytes(
            &mut borrowed.as_bytes().map(|(_, bytes)| bytes)
        );
        assert_eq!(rebuilt.len(), 2);

        let root = rebuilt.get(0);
        assert_eq!(*root.value(), 10);
        assert_eq!(root.kids(), 2);
        assert_eq!(*root.child(0).value(), 20);
        assert_eq!(*root.child(1).value(), 30);
        assert_eq!(*root.child(1).child(0).value(), 40);

        let t1 = rebuilt.get(1);
        assert_eq!(*t1.value(), 100);
        assert_eq!(t1.kids(), 0);
    }

    #[test]
    fn columnar_strings() {
        use crate::Strings;

        let mut trees: Trees<Strings> = Default::default();
        trees.push_tree(branch(
            "root".to_string(),
            vec![leaf("left".to_string()), leaf("right".to_string())],
        ));

        let borrowed = trees.borrow();
        let root = borrowed.get(0);
        assert_eq!(root.value(), b"root");
        assert_eq!(root.kids(), 2);
        assert_eq!(root.child(0).value(), b"left");
        assert_eq!(root.child(1).value(), b"right");
    }

    #[test]
    fn deep_tree() {
        let mut trees: Trees<Vec<u64>> = Default::default();
        // Build a chain: 0 -> 1 -> 2 -> 3 -> 4
        let mut tree = leaf(4u64);
        for i in (0..4).rev() {
            tree = branch(i, vec![tree]);
        }
        trees.push_tree(tree);

        let borrowed = trees.borrow();
        let mut node = borrowed.get(0);
        for i in 0..5u64 {
            assert_eq!(*node.value(), i);
            if i < 4 {
                assert_eq!(node.kids(), 1);
                node = node.child(0);
            } else {
                assert_eq!(node.kids(), 0);
            }
        }
    }
}
