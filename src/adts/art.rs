//! Adaptive Radix Trees (https://db.in.tum.de/~leis/papers/ART.pdf).
//!
//! This ADT represents an unordered collection of byte sequences as a tree.
//! Like a trie, the paths down the tree correspond to byte sequences, and 
//! the membership of a byte sequence is determined by the a viable path.

/// An ART node exists in the context of a sequence of bytes, and indicates
/// the possible options based on the next byte in the sequence.
pub enum ArtNode {
    /// Some of the bytes continue to further nodes, but many do not.
    Some(Box<[(u8, ArtNode)]>),
    /// Many of the bytes continue to further nodes, although some may not.
    /// If a node is `None`, it didn't actually go anywhere.
    /// The representation exists to be more economical than the `Some` variant.
    /// This is especially true if the associated `ArtNode` size is small, which
    /// it is not in this case, but will be once we flatten it.
    Many(Box<[ArtNode; 256]>),
    /// Indicates that there are no branching points for the next few bytes,
    /// after which the next node is provided.
    Path(Box<[u8]>, Box<ArtNode>),
    /// Nothing to see here.
    None,
}

/// A mock-up of what you would store for each `ArtNode` above, when columnar.
pub enum ArtIdx {
    Some(usize),
    Many(usize),
    Path(usize),
    None,
}


pub struct ArtNodes {
    // pub inner: Results<Results< , >, Results< , >>,
}