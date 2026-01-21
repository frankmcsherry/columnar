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
pub struct Trees<T> {
    pub groups: Vec<usize>,     // inserted tree delimiters.
    pub bounds: Vec<usize>,     // node child delimiters.
    pub values: Vec<T>,         // node values.
}

/// A stand-in for `&Tree<T>`
pub struct TreesRef<'a, T> {
    value: &'a T,
    lower: usize,
    upper: usize,
    nodes: &'a Trees<T>
}

impl<'a, T> Clone for TreesRef<'a, T> {
    fn clone(&self) -> Self { *self }
}
impl<'a, T> Copy for TreesRef<'a, T> { }

impl<'a, T> TreesRef<'a, T> {
    pub fn value(&self) -> &T { self.value }
    pub fn child(&self, index: usize) -> TreesRef<'a, T> {
        assert!(index < self.upper - self.lower);
        let child = self.lower + index;
        TreesRef {
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

impl<'a, T: PartialEq> PartialEq<Tree<T>> for TreesRef<'a, T> {
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

impl<T> Default for Trees<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Trees<T> {
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

    pub fn index(&self, index: usize) -> TreesRef<'_, T> {
        let root = self.groups[index];
        TreesRef {
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
