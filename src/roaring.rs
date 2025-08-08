//! Roaring bitmap (and similar) containers.

use crate::Results;

/// A container for `bool` that uses techniques from Roaring bitmaps.
///
/// These techniques are to block the bits into blocks of 2^16 bits,
/// and to encode each block based on its density. Either a bitmap
/// for dense blocks or a list of set bits for sparse blocks.
///
/// Additionally, other representations encode runs of set bits.
pub struct RoaringBits {
    _inner: Results<[u64; 1024], Vec<u16>>,
}
