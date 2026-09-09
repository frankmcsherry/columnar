//! Containers for enumerations ("sum types") that store variants separately.
//!
//! The main work of these types is storing a discriminant and index efficiently,
//! as containers for each of the variant types can hold the actual data.

/// Stores for maintaining discriminants, and associated sequential indexes.
///
/// The sequential indexes are not explicitly maintained, but are supported
/// by a `rank(index)` function that indicates how many of a certain variant
/// precede the given index. While this could potentially be done with a scan
/// of all preceding discriminants, the stores maintain running accumulations
/// that make the operation constant time (using additional amortized memory).
pub mod rank_select {

    use alloc::{vec::Vec, string::String};
    use crate::primitive::Bools;

    use crate::{Borrow, Len, Index, IndexAs, Push, Clear};

    /// Number of 64-bit words per cumulative-popcount chunk. Smaller values
    /// reduce the worst-case word scan in `select` (and the catch-up scan
    /// in `rank`) at the cost of more `counts` entries (~6% overhead per
    /// halving). 16 → 1024 bits/chunk is the historical default; 8 → 512
    /// bits gives faster random `select` for ~12% counts memory.
    const WORDS_PER_CHUNK: usize = 16;
    const BITS_PER_CHUNK: usize = 64 * WORDS_PER_CHUNK;

    /// A store for maintaining `Vec<bool>` with fast `rank` and `select` access.
    ///
    /// The design is to have `u64` running counts for each block of 1024 bits,
    /// which are roughly the size of a cache line. This is roughly 6% overhead,
    /// above the bits themselves, which seems pretty solid.
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct RankSelect<CC = Vec<u64>, VC = Vec<u64>, WC = [u64; 2]> {
        /// Counts of the number of cumulative set (true) bits, *after* each block of 1024 bits.
        pub counts: CC,
        /// The bits themselves.
        pub values: Bools<VC, WC>,
    }

    impl<CC: crate::common::BorrowIndexAs<u64>, VC: crate::common::BorrowIndexAs<u64>> RankSelect<CC, VC> {
        #[inline(always)]
        pub fn borrow<'a>(&'a self) -> RankSelect<CC::Borrowed<'a>, VC::Borrowed<'a>, &'a [u64]> {
            RankSelect {
                counts: self.counts.borrow(),
                values: self.values.borrow(),
            }
        }
        #[inline(always)]
        pub fn reborrow<'b, 'a: 'b>(thing: RankSelect<CC::Borrowed<'a>, VC::Borrowed<'a>, &'a [u64]>) -> RankSelect<CC::Borrowed<'b>, VC::Borrowed<'b>, &'b [u64]> {
            RankSelect {
                counts: CC::reborrow(thing.counts),
                values: Bools::<VC, [u64; 2]>::reborrow(thing.values),
            }
        }
    }

    impl<'a, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for RankSelect<CC, VC, &'a [u64]> {
        const SLICE_COUNT: usize = CC::SLICE_COUNT + <Bools<VC, &'a [u64]> as crate::AsBytes<'a>>::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            debug_assert!(index < Self::SLICE_COUNT);
            if index < CC::SLICE_COUNT {
                self.counts.get_byte_slice(index)
            } else {
                self.values.get_byte_slice(index - CC::SLICE_COUNT)
            }
        }
    }
    impl<'a, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for RankSelect<CC, VC, &'a [u64]> {
        const SLICE_COUNT: usize = CC::SLICE_COUNT + <crate::primitive::Bools<VC, &'a [u64]>>::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                counts: crate::FromBytes::from_bytes(bytes),
                values: crate::FromBytes::from_bytes(bytes),
            }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            Self {
                counts: CC::from_store(store, offset),
                values: <crate::primitive::Bools<VC, &'a [u64]>>::from_store(store, offset),
            }
        }
        #[inline(always)]
        fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
            CC::element_sizes(sizes)?;
            <crate::primitive::Bools<VC, &'a [u64]>>::element_sizes(sizes)?;
            Ok(())
        }
    }


    impl<CC, VC: Len + IndexAs<u64>, WC: IndexAs<u64>> RankSelect<CC, VC, WC> {
        #[inline(always)]
        pub fn get(&self, index: usize) -> bool {
            Index::get(&self.values, index)
        }
    }
    impl<CC: Len + IndexAs<u64>, VC: Len + IndexAs<u64>, WC: IndexAs<u64>> RankSelect<CC, VC, WC> {
        /// The number of set bits *strictly* preceding `index`.
        ///
        /// This number is accumulated first by reading out of `self.counts` at the correct position,
        /// then by summing the ones in strictly prior `u64` entries, then by counting the ones in the
        /// masked `u64` in which the bit lives.
        pub fn rank(&self, index: usize) -> usize {
            let bit = index % 64;
            let block = index / 64;
            let chunk = block / WORDS_PER_CHUNK;
            let mut count = if chunk > 0 { self.counts.index_as(chunk - 1) as usize } else { 0 };
            for pos in (WORDS_PER_CHUNK * chunk) .. block {
                count += self.values.values.index_as(pos).count_ones() as usize;
            }
            // TODO: Panic if out of bounds?
            let intra_word = if block == self.values.values.len() { self.values.tail.index_as(0) } else { self.values.values.index_as(block) };
            count += (intra_word & ((1 << bit) - 1)).count_ones() as usize;
            count
        }
        /// The position of the `rank`-th set bit (0-indexed), if it exists.
        ///
        /// `select(0)` returns the position of the first set bit. In general,
        /// `select(rank(p)) == p` when `p` is the position of a set bit, mirroring
        /// the convention of [`Self::rank`] (which counts bits strictly before
        /// its argument).
        #[inline]
        pub fn select(&self, rank: u64) -> Option<usize> {
            // Step one: find the BITS_PER_CHUNK-bit chunk containing the rank-th set bit.
            // We want the smallest `chunk` for which `counts[chunk] > rank` — the
            // chunk whose cumulative count first exceeds `rank`. Equivalent to a
            // partition point over `counts` on the predicate `counts[i] <= rank`.
            let chunk = {
                let mut lo = 0;
                let mut hi = self.counts.len();
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    if self.counts.index_as(mid) <= rank { lo = mid + 1; } else { hi = mid; }
                }
                lo
            };
            // Number of set bits strictly before `chunk`'s BITS_PER_CHUNK bits.
            let mut count = if chunk > 0 { self.counts.index_as(chunk - 1) } else { 0 };

            // Step two: find the 64-bit word within `chunk` containing the rank-th set bit.
            let mut block = WORDS_PER_CHUNK * chunk;
            while block < self.values.values.len() {
                let pop = self.values.values.index_as(block).count_ones() as u64;
                if count + pop > rank { break; }
                count += pop;
                block += 1;
            }

            // Step three: locate the bit within the chosen word, or return `None`
            // if `rank` is past the total set-bit count.
            let (last_word, last_bits) = if block == self.values.values.len() {
                (self.values.tail.index_as(0), self.values.tail.index_as(1) as usize)
            } else {
                (self.values.values.index_as(block), 64)
            };
            // Mask off any padding past the last valid bit (only relevant for the tail).
            let masked = if last_bits == 64 { last_word } else { last_word & ((1u64 << last_bits) - 1) };
            let k = (rank - count) as u32;
            let shift = select_in_word(masked, k);
            if shift >= last_bits as u32 { None } else { Some(64 * block + shift as usize) }
        }
    }

    /// Position of the `k`-th set bit (0-indexed) within a 64-bit word. Returns 64 if
    /// the word has fewer than `k + 1` set bits. Portable, ~6 conditional shifts based
    /// on byte/half popcounts — independent of bit density, unlike a linear scan.
    #[inline]
    fn select_in_word(mut w: u64, mut k: u32) -> u32 {
        if k >= w.count_ones() { return 64; }
        let mut pos = 0u32;
        // Halve the search range repeatedly. At each step, popcount of the low half
        // tells us whether the k-th bit is in the low half (recurse) or the high half
        // (subtract that count and shift).
        let pop = (w & 0xFFFF_FFFF).count_ones();
        if k >= pop { k -= pop; pos += 32; w >>= 32; }
        let pop = (w & 0xFFFF).count_ones();
        if k >= pop { k -= pop; pos += 16; w >>= 16; }
        let pop = (w & 0xFF).count_ones();
        if k >= pop { k -= pop; pos += 8; w >>= 8; }
        let pop = (w & 0xF).count_ones();
        if k >= pop { k -= pop; pos += 4; w >>= 4; }
        let pop = (w & 0x3).count_ones();
        if k >= pop { k -= pop; pos += 2; w >>= 2; }
        let pop = w & 0x1;
        if (k as u64) >= pop { pos += 1; }
        pos
    }

    /// Forward cursor over a [`RankSelect`].
    ///
    /// Use this instead of repeated `rank`/`select` calls when you want to traverse
    /// the bitvector in order. The cursor caches a current word and running rank,
    /// so a single word load serves many subsequent operations — no re-probing
    /// `counts` or rescanning words.
    ///
    /// At a high level the cursor maintains an *invariant pair* (`pos`, `rank`):
    /// `pos` is the next bit to consider, and `rank` is the number of 1-bits in
    /// `[0, pos)`. Every operation maintains this pair so that callers can read
    /// either coordinate freely.
    ///
    /// Pick the method that matches the question you want to answer:
    ///
    /// | Operation         | Use when you want…                                        |
    /// |-------------------|-----------------------------------------------------------|
    /// | [`next_one`][n1]  | "Where is the next 1-bit?" — emits 1-bit positions in order. |
    /// | [`step`][s]       | "What is the bit at the current position?" — read-by-bit traversal. |
    /// | [`seek_to_pos`][stp] | "Jump to bit position p; what is its rank?" — random forward seek. |
    /// | [`seek_to_rank`][str] | "Jump to the k-th 1-bit; where is it?" — equivalent to `select`. |
    ///
    /// The cursor is **forward-only**: every operation advances `pos` and `rank`
    /// monotonically. Trying to seek backward triggers a debug-assertion failure.
    ///
    /// [n1]: Cursor::next_one
    /// [s]: Cursor::step
    /// [stp]: Cursor::seek_to_pos
    /// [str]: Cursor::seek_to_rank
    pub struct Cursor<'a, CC, VC, WC> {
        rs: &'a RankSelect<CC, VC, WC>,
        /// Index of the current 64-bit word within `values` (or `== values.len()` for tail).
        word_idx: usize,
        /// 1-bits remaining in the current word at positions ≥ `bit_pos % 64`.
        /// Bits at positions strictly below `bit_pos % 64` are cleared.
        word_remaining: u64,
        /// Position of the next bit to consider.
        bit_pos: usize,
        /// Number of 1-bits in `[0, bit_pos)`.
        rank: u64,
        /// Cached total bit count of the underlying RankSelect.
        total_bits: usize,
    }

    impl<CC: Len + IndexAs<u64>, VC: Len + IndexAs<u64>, WC: IndexAs<u64>> RankSelect<CC, VC, WC> {
        /// Create a forward cursor positioned at bit 0.
        pub fn cursor(&self) -> Cursor<'_, CC, VC, WC> {
            let total_bits = self.len();
            let mut c = Cursor {
                rs: self,
                word_idx: 0,
                word_remaining: 0,
                bit_pos: 0,
                rank: 0,
                total_bits,
            };
            c.load_word();
            c
        }
    }

    impl<'a, CC: Len + IndexAs<u64>, VC: Len + IndexAs<u64>, WC: IndexAs<u64>> Cursor<'a, CC, VC, WC> {
        /// Position of the next bit to consider.
        #[inline] pub fn pos(&self) -> usize { self.bit_pos }
        /// Number of 1-bits strictly before `pos()`.
        #[inline] pub fn rank(&self) -> u64 { self.rank }
        /// Total number of bits in the underlying vector.
        #[inline] pub fn total_bits(&self) -> usize { self.total_bits }

        /// Load the word at `self.word_idx`, masked to its valid bits, and align
        /// `bit_pos` to the start of that word. No-op past the end.
        fn load_word(&mut self) {
            let vlen = self.rs.values.values.len();
            if self.word_idx < vlen {
                self.word_remaining = self.rs.values.values.index_as(self.word_idx);
                self.bit_pos = self.word_idx * 64;
            } else if self.word_idx == vlen {
                let raw = self.rs.values.tail.index_as(0);
                let valid = self.rs.values.tail.index_as(1);
                let mask = if valid >= 64 { !0u64 } else if valid == 0 { 0 } else { (1u64 << valid) - 1 };
                self.word_remaining = raw & mask;
                self.bit_pos = self.word_idx * 64;
            } else {
                self.word_remaining = 0;
            }
        }

        /// Emit the next 1-bit position in order; advance past it.
        ///
        /// Equivalent to `select(self.rank())` followed by stepping one past that
        /// position, but amortized: a single word load serves up to 64 results.
        ///
        /// **Use this for streaming through every set bit.** The canonical example
        /// is recovering monotone Vecs bounds from an RS-encoded unary bitvector:
        /// each call returns the next bound's bit position, no per-call re-probing
        /// of `counts`. Returns `None` once all set bits have been emitted.
        pub fn next_one(&mut self) -> Option<usize> {
            loop {
                if self.word_remaining != 0 {
                    let bit_in_word = self.word_remaining.trailing_zeros() as usize;
                    let pos = self.word_idx * 64 + bit_in_word;
                    self.word_remaining &= self.word_remaining - 1;
                    self.bit_pos = pos + 1;
                    self.rank += 1;
                    return Some(pos);
                }
                if self.bit_pos >= self.total_bits { return None; }
                self.word_idx += 1;
                self.load_word();
                if self.word_idx > self.rs.values.values.len() { return None; }
            }
        }

        /// Advance one bit. Return its value: `Some(true)` for 1, `Some(false)` for 0,
        /// or `None` if past the end.
        ///
        /// **Use this when you need to visit every bit in order, regardless of value.**
        /// Common pattern: the lookup-walk loop in a hash table that probes consecutive
        /// slots, deciding what to do based on whether each slot is occupied. Each
        /// call is a single bit-test + an occasional word-boundary crossing.
        pub fn step(&mut self) -> Option<bool> {
            if self.bit_pos >= self.total_bits { return None; }
            let bit_in_word = self.bit_pos & 63;
            let is_one = (self.word_remaining >> bit_in_word) & 1 == 1;
            if is_one {
                self.word_remaining &= !(1u64 << bit_in_word);
                self.rank += 1;
            }
            self.bit_pos += 1;
            if (self.bit_pos & 63) == 0 && self.bit_pos < self.total_bits {
                self.word_idx += 1;
                self.load_word();
            }
            Some(is_one)
        }

        /// Jump forward to bit position `target` and report its `rank` (via the
        /// cursor's state) without consuming the bit there.
        ///
        /// After return, `pos() == target` and `rank() == rank(target)` (the count
        /// of 1-bits strictly before `target`). A subsequent [`step`][Self::step]
        /// reads the bit *at* `target`.
        ///
        /// **Use this when you have a known target position and want to read or
        /// continue from there.** Typical use is hash-table lookup: a query's slot
        /// position is computed from its hash; you want to jump there and then
        /// walk forward through the probe chain.
        ///
        /// Fast path: if `target` lies within the cursor's current word, this is a
        /// popcount + mask. Otherwise it pays one `RankSelect::rank` call to
        /// re-anchor.
        ///
        /// `target` must be `>= self.pos()` (forward-only; debug-asserts otherwise).
        /// Returns `false` if `target` is past the end of the bitvector.
        pub fn seek_to_pos(&mut self, target: usize) -> bool {
            debug_assert!(target >= self.bit_pos, "seek_to_pos is forward-only");
            if target >= self.total_bits {
                self.bit_pos = self.total_bits;
                self.word_remaining = 0;
                return false;
            }
            let target_word = target / 64;
            if target_word == self.word_idx {
                // Same word: count 1-bits we skip past, then mask the word.
                let cur_bit = (self.bit_pos & 63) as u32;
                let tgt_bit = (target & 63) as u32;
                let skipped_mask = if tgt_bit == 64 { !0u64 } else { (1u64 << tgt_bit) - 1 };
                let skipped = self.word_remaining & skipped_mask;
                self.rank += skipped.count_ones() as u64;
                // Clear consumed low bits (positions < target).
                self.word_remaining &= !skipped_mask;
                let _ = cur_bit;
                self.bit_pos = target;
                return true;
            }
            // Different word: re-anchor rank via the standard rank() formula, then
            // load the target word and mask off bits below `target`.
            self.rank = self.rs.rank(target) as u64;
            self.word_idx = target_word;
            self.load_word();
            let tgt_bit = target & 63;
            let mask = if tgt_bit == 0 { !0u64 } else { !((1u64 << tgt_bit) - 1) };
            self.word_remaining &= mask;
            self.bit_pos = target;
            true
        }

        /// Jump forward to the `target`-th set bit (0-indexed) and return its
        /// position. Equivalent to [`RankSelect::select`] for the cursor's
        /// current state.
        ///
        /// After return, `rank() == target + 1` (the target bit has been consumed).
        ///
        /// **Use this when you have a target rank and want the position.** Typical
        /// use is "give me bound[k]" against an RS-encoded Vecs-bounds bitvector,
        /// possibly skipping forward over a stretch of consecutive bounds you
        /// don't care about.
        ///
        /// Fast path: if the target's bit lies within the cursor's current word,
        /// this is a `select_in_word` + bit-clear. Otherwise it pays a binary
        /// search over `counts`, mirroring `RankSelect::select`.
        ///
        /// `target` must be `>= self.rank()` (forward-only; debug-asserts otherwise).
        /// Returns `None` if there are fewer than `target + 1` set bits in total.
        pub fn seek_to_rank(&mut self, target: u64) -> Option<usize> {
            debug_assert!(target >= self.rank, "seek_to_rank is forward-only");
            // Cheap path: target is within the current word's remaining 1-bits.
            let here_pop = self.word_remaining.count_ones() as u64;
            if target < self.rank + here_pop {
                let k = (target - self.rank) as u32;
                let bit_in_word = select_in_word(self.word_remaining, k) as usize;
                let pos = self.word_idx * 64 + bit_in_word;
                // Consume up to and including the target bit: clear the lowest k+1 set bits.
                let mut w = self.word_remaining;
                for _ in 0..=k { w &= w.wrapping_sub(1); }
                self.word_remaining = w;
                self.rank = target + 1;
                self.bit_pos = pos + 1;
                return Some(pos);
            }
            // Otherwise: jump via chunk binary search, mirroring `select`.
            let counts = &self.rs.counts;
            let chunk = {
                let mut lo = 0;
                let mut hi = counts.len();
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    if counts.index_as(mid) <= target { lo = mid + 1; } else { hi = mid; }
                }
                lo
            };
            let mut count = if chunk > 0 { counts.index_as(chunk - 1) } else { 0 };
            let vlen = self.rs.values.values.len();
            let mut block = WORDS_PER_CHUNK * chunk;
            while block < vlen {
                let pop = self.rs.values.values.index_as(block).count_ones() as u64;
                if count + pop > target { break; }
                count += pop;
                block += 1;
            }
            self.word_idx = block;
            self.load_word();
            self.rank = count;
            // Now finish within the freshly loaded word.
            if target >= count + self.word_remaining.count_ones() as u64 {
                // Exhausted.
                self.bit_pos = self.total_bits;
                self.word_remaining = 0;
                return None;
            }
            let k = (target - count) as u32;
            let bit_in_word = select_in_word(self.word_remaining, k) as usize;
            let pos = self.word_idx * 64 + bit_in_word;
            let mut w = self.word_remaining;
            for _ in 0..=k { w &= w.wrapping_sub(1); }
            self.word_remaining = w;
            self.rank = target + 1;
            self.bit_pos = pos + 1;
            Some(pos)
        }
    }

    impl<CC, VC: Len, WC: IndexAs<u64>> RankSelect<CC, VC, WC> {
        pub fn len(&self) -> usize {
            self.values.len()
        }
    }

    // This implementation probably only works for `Vec<u64>` and `Vec<u64>`, but we could fix that.
    // Partly, it's hard to name the `Index` flavor that allows one to get back a `u64`.
    impl<CC: for<'a> Push<&'a u64> + Len + IndexAs<u64>, VC: for<'a> Push<&'a u64> + Len + IndexAs<u64>> RankSelect<CC, VC> {
        #[inline]
        pub fn push(&mut self, bit: bool) {
            self.values.push(&bit);
            while self.counts.len() < self.values.len() / BITS_PER_CHUNK {
                let mut count = self.counts.last().unwrap_or(0);
                let lower = WORDS_PER_CHUNK * self.counts.len();
                let upper = lower + WORDS_PER_CHUNK;
                for i in lower .. upper {
                    count += self.values.values.index_as(i).count_ones() as u64;
                }
                self.counts.push(&count);
            }
        }
    }
    impl<CC: Clear, VC: Clear> Clear for RankSelect<CC, VC> {
        fn clear(&mut self) {
            self.counts.clear();
            self.values.clear();
        }
    }
    impl crate::Truncate for RankSelect {
        #[inline]
        fn truncate(&mut self, len: usize) {
            self.values.truncate(len);
            // `push` maintains one count per complete chunk of words.
            self.counts.truncate(self.values.values.len() / WORDS_PER_CHUNK);
        }
    }

    #[cfg(test)]
    mod tests {
        use alloc::{vec, vec::Vec};
        use super::RankSelect;

        fn build(bits: &[bool]) -> RankSelect {
            let mut rs: RankSelect = RankSelect::default();
            for &b in bits { rs.push(b); }
            rs
        }

        /// All true bits are recovered by `select(rank(p))` for every set position `p`,
        /// and `rank` agrees with a naive count.
        fn check_round_trip(bits: &[bool]) {
            let rs = build(bits);
            let mut expected = 0u64;
            for (i, &b) in bits.iter().enumerate() {
                assert_eq!(rs.rank(i), expected as usize, "rank({}) on pattern of len {}", i, bits.len());
                if b {
                    let pos = rs.select(expected).unwrap_or_else(|| panic!("select({}) returned None for set bit at {}", expected, i));
                    assert_eq!(pos, i, "select({}) on pattern of len {}", expected, bits.len());
                    expected += 1;
                }
            }
            // Out-of-range select returns None.
            assert!(rs.select(expected).is_none());
        }

        #[test]
        fn select_first_bit() {
            // Bit 0 set, nothing else.
            let mut bits = vec![false; 2048];
            bits[0] = true;
            check_round_trip(&bits);
        }

        #[test]
        fn select_small_dense() {
            // First five bits set in a 2048-bit vector (spans two chunks).
            let mut bits = vec![false; 2048];
            for i in 0..5 { bits[i] = true; }
            check_round_trip(&bits);
        }

        #[test]
        fn select_chunk_boundary() {
            // Set bits exactly at the chunk boundary positions 1023 and 1024.
            let mut bits = vec![false; 4096];
            bits[1023] = true;
            bits[1024] = true;
            check_round_trip(&bits);
        }

        #[test]
        fn select_in_tail() {
            // Bits 0..3 set, then nothing through bit 1100. Pattern length 1100 puts
            // the final bits in the tail (not a complete 1024-bit chunk).
            let mut bits = vec![false; 1100];
            bits[0] = true;
            bits[1099] = true;
            check_round_trip(&bits);
        }

        #[test]
        fn select_every_other() {
            // Dense, multiple chunks.
            let bits: Vec<bool> = (0..3000).map(|i| i % 2 == 0).collect();
            check_round_trip(&bits);
        }

        #[test]
        fn select_sparse_multi_chunk() {
            // One set bit per 1024-bit chunk, six chunks.
            let mut bits = vec![false; 6 * 1024];
            for c in 0..6 { bits[1024 * c + 17] = true; }
            check_round_trip(&bits);
        }

        #[test]
        fn select_out_of_range() {
            let rs = build(&[true, false, true]);
            assert_eq!(rs.select(0), Some(0));
            assert_eq!(rs.select(1), Some(2));
            assert_eq!(rs.select(2), None);
            assert_eq!(rs.select(1000), None);
        }

        #[test]
        fn cursor_next_one_matches_select() {
            // For each set bit in a 3000-bit pattern, walking next_one gives the
            // same positions as repeated select calls.
            let bits: Vec<bool> = (0..3000).map(|i| i % 7 == 0).collect();
            let rs = build(&bits);
            let mut cur = rs.cursor();
            let mut k = 0u64;
            while let Some(pos) = cur.next_one() {
                assert_eq!(Some(pos), rs.select(k));
                assert_eq!(cur.rank(), k + 1);
                assert_eq!(cur.pos(), pos + 1);
                k += 1;
            }
            assert_eq!(k, rs.rank(rs.len()) as u64);
        }

        #[test]
        fn cursor_step_walks_every_bit() {
            let bits: Vec<bool> = (0..2050).map(|i| i % 3 == 0).collect();
            let rs = build(&bits);
            let mut cur = rs.cursor();
            let mut expected_rank = 0u64;
            for (i, &b) in bits.iter().enumerate() {
                assert_eq!(cur.pos(), i);
                assert_eq!(cur.rank(), expected_rank);
                assert_eq!(cur.step(), Some(b));
                if b { expected_rank += 1; }
            }
            assert_eq!(cur.step(), None);
        }

        #[test]
        fn cursor_seek_to_rank_skips_far_forward() {
            // 3 chunks worth of bits, with set bits clustered. Seek directly to
            // the 100-th set bit from a fresh cursor; result matches select(100).
            let bits: Vec<bool> = (0..3200).map(|i| i % 3 == 1).collect();
            let rs = build(&bits);
            let mut cur = rs.cursor();
            let pos = cur.seek_to_rank(100).unwrap();
            assert_eq!(Some(pos), rs.select(100));
            assert_eq!(cur.rank(), 101);
            // A subsequent next_one continues correctly.
            let next = cur.next_one().unwrap();
            assert_eq!(Some(next), rs.select(101));
        }

        #[test]
        fn cursor_seek_to_rank_within_current_word() {
            // Set bits in a known pattern within the first word; cursor should
            // satisfy seek_to_rank from the current-word fast path without re-probing.
            let bits: Vec<bool> = (0..64).map(|i| matches!(i, 1 | 5 | 13 | 30 | 50)).collect();
            let rs = build(&bits);
            let mut cur = rs.cursor();
            assert_eq!(cur.seek_to_rank(0), Some(1));
            assert_eq!(cur.seek_to_rank(2), Some(13));
            assert_eq!(cur.seek_to_rank(4), Some(50));
            assert_eq!(cur.next_one(), None);
        }

        #[test]
        fn cursor_seek_to_pos_same_word_and_cross_word() {
            let bits: Vec<bool> = (0..256).map(|i| matches!(i % 5, 0 | 2)).collect();
            let rs = build(&bits);
            // Same-word seek.
            let mut cur = rs.cursor();
            assert!(cur.seek_to_pos(10));
            assert_eq!(cur.pos(), 10);
            assert_eq!(cur.rank(), rs.rank(10) as u64);
            assert!(cur.seek_to_pos(40));
            assert_eq!(cur.rank(), rs.rank(40) as u64);
            // Cross-word seek.
            assert!(cur.seek_to_pos(200));
            assert_eq!(cur.pos(), 200);
            assert_eq!(cur.rank(), rs.rank(200) as u64);
            // Step reads the bit at the target.
            assert_eq!(cur.step(), Some(bits[200]));
        }

        #[test]
        fn cursor_seek_to_pos_out_of_range() {
            let bits: Vec<bool> = (0..100).map(|_| true).collect();
            let rs = build(&bits);
            let mut cur = rs.cursor();
            assert!(!cur.seek_to_pos(1000));
            assert_eq!(cur.step(), None);
        }

        #[test]
        fn cursor_seek_to_rank_out_of_range() {
            let bits: Vec<bool> = (0..200).map(|i| i % 10 == 0).collect();
            let rs = build(&bits);
            let mut cur = rs.cursor();
            assert_eq!(cur.seek_to_rank(10_000), None);
        }

        #[test]
        fn select_in_word_basic() {
            use super::select_in_word;
            // 0b10110: set bits at positions 1, 2, 4.
            assert_eq!(select_in_word(0b10110, 0), 1);
            assert_eq!(select_in_word(0b10110, 1), 2);
            assert_eq!(select_in_word(0b10110, 2), 4);
            assert_eq!(select_in_word(0b10110, 3), 64);
            // Edges of the word.
            assert_eq!(select_in_word(1u64 << 63, 0), 63);
            assert_eq!(select_in_word(u64::MAX, 0), 0);
            assert_eq!(select_in_word(u64::MAX, 63), 63);
            assert_eq!(select_in_word(u64::MAX, 64), 64);
            assert_eq!(select_in_word(0, 0), 64);
        }
    }
}

pub mod result {

    use alloc::{vec::Vec, string::String};

    use crate::{Clear, Columnar, Container, Len, IndexMut, Index, IndexAs, Push, Borrow};
    use crate::RankSelect;

    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct Results<SC, TC, CC=Vec<u64>, VC=Vec<u64>, WC=[u64; 2]> {
        /// Bits set to `true` correspond to `Ok` variants.
        pub indexes: RankSelect<CC, VC, WC>,
        pub oks: SC,
        pub errs: TC,
    }

    impl<S: Columnar, T: Columnar> Columnar for Result<S, T> {
        fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
            match (&mut *self, other) {
                (Ok(x), Ok(y)) => x.copy_from(y),
                (Err(x), Err(y)) => x.copy_from(y),
                (_, other) => { *self = Self::into_owned(other); },
            }
        }
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
            match other {
                Ok(y) => Ok(S::into_owned(y)),
                Err(y) => Err(T::into_owned(y)),
            }
        }
        type Container = Results<S::Container, T::Container>;
    }

    impl<SC: Borrow, TC: Borrow> Borrow for Results<SC, TC> {
        type Ref<'a> = Result<SC::Ref<'a>, TC::Ref<'a>> where SC: 'a, TC: 'a;
        type Borrowed<'a> = Results<SC::Borrowed<'a>, TC::Borrowed<'a>, &'a [u64], &'a [u64], &'a [u64]> where SC: 'a, TC: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Results {
                indexes: self.indexes.borrow(),
                oks: self.oks.borrow(),
                errs: self.errs.borrow(),
            }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where SC: 'a, TC: 'a {
            Results {
                indexes: RankSelect::<Vec<u64>, Vec<u64>>::reborrow(thing.indexes),
                oks: SC::reborrow(thing.oks),
                errs: TC::reborrow(thing.errs),
            }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a {
            match thing {
                Ok(y) => Ok(SC::reborrow_ref(y)),
                Err(y) => Err(TC::reborrow_ref(y)),
            }
        }
    }

    impl<SC: Container, TC: Container> Container for Results<SC, TC> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
            if !range.is_empty() {
                // Starting offsets of each variant in `other`.
                let oks_start = other.indexes.rank(range.start);
                let errs_start = range.start - oks_start;

                // Count the number of `Ok` and `Err` variants as we push, to determine the range.
                // TODO: This could probably be `popcnt` somehow.
                let mut oks = 0;
                for index in range.clone() {
                    let bit = other.indexes.get(index);
                    self.indexes.push(bit);
                    if bit { oks += 1; }
                }
                let errs = range.len() - oks;

                self.oks.extend_from_self(other.oks, oks_start .. oks_start + oks);
                self.errs.extend_from_self(other.errs, errs_start .. errs_start + errs);
            }
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            // TODO: reserve room in `self.indexes`.
            self.oks.reserve_for(selves.clone().map(|x| x.oks));
            self.errs.reserve_for(selves.map(|x| x.errs));
        }
    }

    impl<'a, SC: crate::AsBytes<'a>, TC: crate::AsBytes<'a>, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for Results<SC, TC, CC, VC, &'a [u64]> {
        const SLICE_COUNT: usize = <RankSelect<CC, VC, &'a [u64]> as crate::AsBytes<'a>>::SLICE_COUNT + SC::SLICE_COUNT + TC::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            debug_assert!(index < Self::SLICE_COUNT);
            let idx_count = <RankSelect<CC, VC, &'a [u64]> as crate::AsBytes<'a>>::SLICE_COUNT;
            if index < idx_count {
                self.indexes.get_byte_slice(index)
            } else if index < idx_count + SC::SLICE_COUNT {
                self.oks.get_byte_slice(index - idx_count)
            } else {
                self.errs.get_byte_slice(index - idx_count - SC::SLICE_COUNT)
            }
        }
    }
    impl<'a, SC: crate::FromBytes<'a>, TC: crate::FromBytes<'a>, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Results<SC, TC, CC, VC, &'a [u64]> {
        const SLICE_COUNT: usize = <RankSelect<CC, VC, &'a [u64]>>::SLICE_COUNT + SC::SLICE_COUNT + TC::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                indexes: crate::FromBytes::from_bytes(bytes),
                oks: crate::FromBytes::from_bytes(bytes),
                errs: crate::FromBytes::from_bytes(bytes),
            }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            Self {
                indexes: crate::FromBytes::from_store(store, offset),
                oks: SC::from_store(store, offset),
                errs: TC::from_store(store, offset),
            }
        }
        #[inline(always)]
        fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
            <RankSelect<CC, VC, &'a [u64]>>::element_sizes(sizes)?;
            SC::element_sizes(sizes)?;
            TC::element_sizes(sizes)?;
            Ok(())
        }
    }

    impl<SC, TC, CC, VC: Len, WC: IndexAs<u64>> Len for Results<SC, TC, CC, VC, WC> {
        #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
    }

    impl<SC, TC, CC, VC, WC> Index for Results<SC, TC, CC, VC, WC>
    where
        SC: Index,
        TC: Index,
        CC: IndexAs<u64> + Len,
        VC: IndexAs<u64> + Len,
        WC: IndexAs<u64>,
    {
        type Ref = Result<SC::Ref, TC::Ref>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            if self.indexes.get(index) {
                Ok(self.oks.get(self.indexes.rank(index)))
            } else {
                Err(self.errs.get(index - self.indexes.rank(index)))
            }
        }
    }
    impl<'a, SC, TC, CC, VC, WC> Index for &'a Results<SC, TC, CC, VC, WC>
    where
        &'a SC: Index,
        &'a TC: Index,
        CC: IndexAs<u64> + Len,
        VC: IndexAs<u64> + Len,
        WC: IndexAs<u64>,
    {
        type Ref = Result<<&'a SC as Index>::Ref, <&'a TC as Index>::Ref>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            if self.indexes.get(index) {
                Ok((&self.oks).get(self.indexes.rank(index)))
            } else {
                Err((&self.errs).get(index - self.indexes.rank(index)))
            }
        }
    }

    // NB: You are not allowed to change the variant, but can change its contents.
    impl<SC: IndexMut, TC: IndexMut, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len> IndexMut for Results<SC, TC, CC, VC> {
        type IndexMut<'a> = Result<SC::IndexMut<'a>, TC::IndexMut<'a>> where SC: 'a, TC: 'a, CC: 'a, VC: 'a;
        #[inline(always)]
        fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            if self.indexes.get(index) {
                Ok(self.oks.get_mut(self.indexes.rank(index)))
            } else {
                Err(self.errs.get_mut(index - self.indexes.rank(index)))
            }
        }
    }

    impl<S, SC: Push<S>, T, TC: Push<T>> Push<Result<S, T>> for Results<SC, TC> {
        #[inline]
        fn push(&mut self, item: Result<S, T>) {
            match item {
                Ok(item) => {
                    self.indexes.push(true);
                    self.oks.push(item);
                }
                Err(item) => {
                    self.indexes.push(false);
                    self.errs.push(item);
                }
            }
        }
    }
    impl<'a, S, SC: Push<&'a S>, T, TC: Push<&'a T>> Push<&'a Result<S, T>> for Results<SC, TC> {
        #[inline]
        fn push(&mut self, item: &'a Result<S, T>) {
            match item {
                Ok(item) => {
                    self.indexes.push(true);
                    self.oks.push(item);
                }
                Err(item) => {
                    self.indexes.push(false);
                    self.errs.push(item);
                }
            }
        }
    }

    impl<SC: Clear, TC: Clear> Clear for Results<SC, TC> {
        fn clear(&mut self) {
            self.indexes.clear();
            self.oks.clear();
            self.errs.clear();
        }
    }

    impl<SC, TC, CC, VC, WC> Results<SC, TC, CC, VC, WC> {
        /// Returns ok values if no errors exist.
        pub fn unwrap(self) -> SC where TC: Len {
            assert!(self.errs.is_empty());
            self.oks
        }
        /// Returns error values if no oks exist.
        pub fn unwrap_err(self) -> TC where SC: Len {
            assert!(self.oks.is_empty());
            self.errs
        }
        /// Returns ok values if no errors exist, or `None`.
        pub fn try_unwrap(self) -> Option<SC> where TC: Len {
            if self.errs.is_empty() { Some(self.oks) } else { None }
        }
        /// Returns error values if no oks exist, or `None`.
        pub fn try_unwrap_err(self) -> Option<TC> where SC: Len {
            if self.oks.is_empty() { Some(self.errs) } else { None }
        }
    }
    #[cfg(test)]
    mod test {
        #[test]
        fn round_trip() {

            use crate::common::{Index, Push, Len};

            let mut column: crate::ContainerOf<Result<u64, u64>> = Default::default();
            for i in 0..100 {
                column.push(Ok::<u64, u64>(i));
                column.push(Err::<u64, u64>(i));
            }

            assert_eq!(column.len(), 200);

            for i in 0..100 {
                assert_eq!(column.get(2*i+0), Ok(i as u64));
                assert_eq!(column.get(2*i+1), Err(i as u64));
            }

            let mut column: crate::ContainerOf<Result<u64, u8>> = Default::default();
            for i in 0..100 {
                column.push(Ok::<u64, u8>(i as u64));
                column.push(Err::<u64, u8>(i as u8));
            }

            assert_eq!(column.len(), 200);

            for i in 0..100 {
                assert_eq!(column.get(2*i+0), Ok(i as u64));
                assert_eq!(column.get(2*i+1), Err(i as u8));
            }
        }
    }
}

pub mod option {

    use alloc::{vec::Vec, string::String};

    use crate::{Clear, Columnar, Container, Len, IndexMut, Index, IndexAs, Push, Borrow};
    use crate::RankSelect;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Copy, Clone, Debug, Default, PartialEq)]
    pub struct Options<TC, CC=Vec<u64>, VC=Vec<u64>, WC=[u64; 2]> {
        /// Uses two bits for each item, one to indicate the variant and one (amortized)
        /// to enable efficient rank determination.
        pub indexes: RankSelect<CC, VC, WC>,
        pub somes: TC,
    }

    impl<T: Columnar> Columnar for Option<T> {
        fn copy_from<'a>(&mut self, other: crate::Ref<'a, Self>) {
            match (&mut *self, other) {
                (Some(x), Some(y)) => { x.copy_from(y); }
                (_, other) => { *self = Self::into_owned(other); }
            }
        }
        fn into_owned<'a>(other: crate::Ref<'a, Self>) -> Self {
            other.map(|x| T::into_owned(x))
        }
        type Container = Options<T::Container>;
    }

    impl<TC: Borrow> Borrow for Options<TC> {
        type Ref<'a> = Option<TC::Ref<'a>> where TC: 'a;
        type Borrowed<'a> = Options<TC::Borrowed<'a>, &'a [u64], &'a [u64], &'a [u64]> where TC: 'a;
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Options {
                indexes: self.indexes.borrow(),
                somes: self.somes.borrow(),
            }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> where TC: 'a {
            Options {
                indexes: RankSelect::<Vec<u64>, Vec<u64>>::reborrow(thing.indexes),
                somes: TC::reborrow(thing.somes),
            }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> where Self: 'a {
            thing.map(TC::reborrow_ref)
        }
    }

    impl<TC: Container> Container for Options<TC> {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: core::ops::Range<usize>) {
            if !range.is_empty() {
                // Starting offsets of `Some` variants in `other`.
                let somes_start = other.indexes.rank(range.start);

                // Count the number of `Some` variants as we push, to determine the range.
                // TODO: This could probably be `popcnt` somehow.
                let mut somes = 0;
                for index in range {
                    let bit = other.indexes.get(index);
                    self.indexes.push(bit);
                    if bit { somes += 1; }
                }

                self.somes.extend_from_self(other.somes, somes_start .. somes_start + somes);
            }
        }

        fn reserve_for<'a, I>(&mut self, selves: I) where Self: 'a, I: Iterator<Item = Self::Borrowed<'a>> + Clone {
            // TODO: reserve room in `self.indexes`.
            self.somes.reserve_for(selves.map(|x| x.somes));
        }
    }

    impl<'a, TC: crate::AsBytes<'a>, CC: crate::AsBytes<'a>, VC: crate::AsBytes<'a>> crate::AsBytes<'a> for Options<TC, CC, VC, &'a [u64]> {
        const SLICE_COUNT: usize = <RankSelect<CC, VC, &'a [u64]> as crate::AsBytes<'a>>::SLICE_COUNT + TC::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            debug_assert!(index < Self::SLICE_COUNT);
            let idx_count = <RankSelect<CC, VC, &'a [u64]> as crate::AsBytes<'a>>::SLICE_COUNT;
            if index < idx_count {
                self.indexes.get_byte_slice(index)
            } else {
                self.somes.get_byte_slice(index - idx_count)
            }
        }
    }

    impl <'a, TC: crate::FromBytes<'a>, CC: crate::FromBytes<'a>, VC: crate::FromBytes<'a>> crate::FromBytes<'a> for Options<TC, CC, VC, &'a [u64]> {
        const SLICE_COUNT: usize = <RankSelect<CC, VC, &'a [u64]>>::SLICE_COUNT + TC::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            Self {
                indexes: crate::FromBytes::from_bytes(bytes),
                somes: crate::FromBytes::from_bytes(bytes),
            }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            Self {
                indexes: crate::FromBytes::from_store(store, offset),
                somes: TC::from_store(store, offset),
            }
        }
        #[inline(always)]
        fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
            <RankSelect<CC, VC, &'a [u64]>>::element_sizes(sizes)?;
            TC::element_sizes(sizes)?;
            Ok(())
        }
    }

    impl<T, CC, VC: Len, WC: IndexAs<u64>> Len for Options<T, CC, VC, WC> {
        #[inline(always)] fn len(&self) -> usize { self.indexes.len() }
    }

    impl<TC: Index, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len, WC: IndexAs<u64>> Index for Options<TC, CC, VC, WC> {
        type Ref = Option<TC::Ref>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            if self.indexes.get(index) {
                Some(self.somes.get(self.indexes.rank(index)))
            } else {
                None
            }
        }
    }
    impl<'a, TC, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len, WC: IndexAs<u64>> Index for &'a Options<TC, CC, VC, WC>
    where &'a TC: Index
    {
        type Ref = Option<<&'a TC as Index>::Ref>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            if self.indexes.get(index) {
                Some((&self.somes).get(self.indexes.rank(index)))
            } else {
                None
            }
        }
    }
    impl<TC: IndexMut, CC: IndexAs<u64> + Len, VC: IndexAs<u64> + Len> IndexMut for Options<TC, CC, VC> {
        type IndexMut<'a> = Option<TC::IndexMut<'a>> where TC: 'a, CC: 'a, VC: 'a;
        #[inline(always)]
        fn get_mut(&mut self, index: usize) -> Self::IndexMut<'_> {
            if self.indexes.get(index) {
                Some(self.somes.get_mut(self.indexes.rank(index)))
            } else {
                None
            }
        }
    }

    impl<T, TC: Push<T> + Len> Push<Option<T>> for Options<TC> {
        #[inline]
        fn push(&mut self, item: Option<T>) {
            match item {
                Some(item) => {
                    self.indexes.push(true);
                    self.somes.push(item);
                }
                None => {
                    self.indexes.push(false);
                }
            }
        }
    }
    impl<'a, T, TC: Push<&'a T> + Len> Push<&'a Option<T>> for Options<TC> {
        #[inline]
        fn push(&mut self, item: &'a Option<T>) {
            match item {
                Some(item) => {
                    self.indexes.push(true);
                    self.somes.push(item);
                }
                None => {
                    self.indexes.push(false);
                }
            }
        }
    }

    impl<TC, CC, VC, WC> Options<TC, CC, VC, WC> {
        /// Returns the inner container if all elements are `Some`, or `None`.
        pub fn try_unwrap(self) -> Option<TC> where TC: Len, VC: Len, WC: IndexAs<u64> {
            if self.somes.len() == self.indexes.len() { Some(self.somes) } else { None }
        }
        /// True if all elements are `None`.
        pub fn is_all_none(&self) -> bool where TC: Len {
            self.somes.is_empty()
        }
    }

    impl<TC: Clear> Clear for Options<TC> {
        fn clear(&mut self) {
            self.indexes.clear();
            self.somes.clear();
        }
    }

    #[cfg(test)]
    mod test {
        use alloc::vec::Vec;

        use crate::Columnar;
        use crate::common::{Index, Len};
        use crate::Options;

        #[test]
        fn round_trip_some() {
            // Type annotation is important to avoid some inference overflow.
            let store: Options<Vec<i32>> = Columnar::into_columns((0..100).map(Some));
            assert_eq!(store.len(), 100);
            assert!((&store).index_iter().zip(0..100).all(|(a, b)| a == Some(&b)));
        }

        #[test]
        fn round_trip_none() {
            let store = Columnar::into_columns((0..100).map(|_x| None::<i32>));
            assert_eq!(store.len(), 100);
            let foo = &store;
            assert!(foo.index_iter().zip(0..100).all(|(a, _b)| a == None));
        }

        #[test]
        fn round_trip_mixed() {
            // Type annotation is important to avoid some inference overflow.
            let store: Options<Vec<i32>>  = Columnar::into_columns((0..100).map(|x| if x % 2 == 0 { Some(x) } else { None }));
            assert_eq!(store.len(), 100);
            assert!((&store).index_iter().zip(0..100).all(|(a, b)| a == if b % 2 == 0 { Some(&b) } else { None }));
        }
    }
}

pub mod discriminant {

    use alloc::{vec::Vec, string::String};
    use crate::{Clear, Container, Len, Index, IndexAs, Borrow};

    /// Tracks variant discriminants and offsets for enum containers.
    ///
    /// Uses two arrays (`variant` and `offset`) with three states:
    /// - **Empty**: both arrays empty, length is 0.
    /// - **Homogeneous**: `variant` is empty, `offset` holds `[tag, count]` where
    ///   `tag = variant_index + 1`. All elements share a single variant with
    ///   identity offsets (element `i` maps to offset `i`).
    /// - **Heterogeneous**: `variant` has per-element discriminants (`u8`),
    ///   `offset` has per-element offsets into variant containers (`u64`).
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct Discriminant<CVar = Vec<u8>, COff = Vec<u64>> {
        /// Per-element variant discriminants; empty when homogeneous.
        pub variant: CVar,
        /// Per-element offsets (heterogeneous), or `[tag, count]` (homogeneous), or empty.
        pub offset: COff,
    }

    impl<CVar: Copy, COff: Copy> Copy for Discriminant<CVar, COff> {}

    impl Discriminant {
        /// Push a variant discriminant and the offset into its variant container.
        #[inline]
        pub fn push(&mut self, variant: u8, offset: u64) {
            let tag = variant as u64 + 1;
            if self.variant.is_empty() {
                if self.offset.is_empty() {
                    // Empty → start homogeneous: offset = [tag, 1].
                    self.offset.push(tag);
                    self.offset.push(1);
                } else if self.offset[0] == tag {
                    // Same variant; stay homogeneous, increment count.
                    self.offset[1] += 1;
                } else {
                    // Different variant; transition to heterogeneous.
                    let prev = (self.offset[0] - 1) as u8;
                    let count = self.offset[1];
                    self.variant.reserve(count as usize + 1);
                    self.offset.clear();
                    self.offset.reserve(count as usize + 1);
                    for i in 0..count {
                        self.variant.push(prev);
                        self.offset.push(i);
                    }
                    self.variant.push(variant);
                    self.offset.push(offset);
                }
            } else {
                // Already heterogeneous.
                self.variant.push(variant);
                self.offset.push(offset);
            }
        }

        /// Pre-allocate for the given borrowed discriminants.
        pub fn reserve_for<'a>(&mut self, selves: impl Iterator<Item = Discriminant<&'a [u8], &'a [u64]>> + Clone) {
            self.variant.reserve_for(selves.clone().map(|x| x.variant));
            self.offset.reserve_for(selves.map(|x| x.offset));
        }
    }

    impl<CVar: Len, COff: Len> Discriminant<CVar, COff> {
        /// True if elements have mixed variants, with per-element discriminants and offsets.
        #[inline]
        pub fn is_heterogeneous(&self) -> bool {
            !self.variant.is_empty()
        }
        /// Returns `Some(variant)` if all elements share a single variant.
        #[inline]
        pub fn homogeneous(&self) -> Option<u8> where COff: IndexAs<u64> {
            if self.variant.is_empty() && self.offset.len() >= 2 {
                Some((self.offset.index_as(0) - 1) as u8)
            } else {
                None
            }
        }
        /// Returns `(variant, offset)` for the element at `index`.
        #[inline(always)]
        pub fn get(&self, index: usize) -> (u8, u64) where CVar: IndexAs<u8>, COff: IndexAs<u64> {
            if self.is_heterogeneous() {
                (self.variant.index_as(index), self.offset.index_as(index))
            } else {
                let tag: u64 = self.offset.index_as(0);
                ((tag - 1) as u8, index as u64)
            }
        }
    }

    impl<CVar: Len, COff: Len + IndexAs<u64>> Len for Discriminant<CVar, COff> {
        #[inline(always)]
        fn len(&self) -> usize {
            if self.is_heterogeneous() { self.variant.len() }
            else if self.offset.len() >= 2 { self.offset.index_as(1) as usize }
            else { 0 }
        }
    }

    // Index for the borrowed form: returns (variant, offset).
    impl<'a> Index for Discriminant<&'a [u8], &'a [u64]> {
        type Ref = (u8, u64);
        #[inline(always)]
        fn get(&self, index: usize) -> (u8, u64) {
            if self.is_heterogeneous() {
                (self.variant.index_as(index), self.offset.index_as(index))
            } else {
                ((self.offset[0] - 1) as u8, index as u64)
            }
        }
    }

    // Borrow
    impl Borrow for Discriminant {
        type Ref<'a> = (u8, u64);
        type Borrowed<'a> = Discriminant<&'a [u8], &'a [u64]>;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Discriminant {
                variant: &self.variant[..],
                offset: &self.offset[..],
            }
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: Self::Borrowed<'a>) -> Self::Borrowed<'b> {
            Discriminant {
                variant: thing.variant,
                offset: thing.offset,
            }
        }
        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b> { thing }
    }

    impl<CVar: Clear, COff: Clear> Clear for Discriminant<CVar, COff> {
        #[inline(always)]
        fn clear(&mut self) {
            self.variant.clear();
            self.offset.clear();
        }
    }


    // AsBytes for Discriminant, generic over container types.
    impl<'a, CVar: crate::AsBytes<'a>, COff: crate::AsBytes<'a>> crate::AsBytes<'a> for Discriminant<CVar, COff> {
        const SLICE_COUNT: usize = CVar::SLICE_COUNT + COff::SLICE_COUNT;
        #[inline]
        fn get_byte_slice(&self, index: usize) -> (u64, &'a [u8]) {
            debug_assert!(index < Self::SLICE_COUNT);
            if index < CVar::SLICE_COUNT {
                self.variant.get_byte_slice(index)
            } else {
                self.offset.get_byte_slice(index - CVar::SLICE_COUNT)
            }
        }
    }

    // FromBytes for borrowed form
    impl<'a> crate::FromBytes<'a> for Discriminant<&'a [u8], &'a [u64]> {
        const SLICE_COUNT: usize = <&'a [u8]>::SLICE_COUNT + <&'a [u64]>::SLICE_COUNT;
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item=&'a [u8]>) -> Self {
            let variant = crate::FromBytes::from_bytes(bytes);
            let offset = crate::FromBytes::from_bytes(bytes);
            Self { variant, offset }
        }
        #[inline(always)]
        fn from_store(store: &crate::bytes::indexed::DecodedStore<'a>, offset: &mut usize) -> Self {
            let variant = crate::FromBytes::from_store(store, offset);
            let offset_field = crate::FromBytes::from_store(store, offset);
            Self { variant, offset: offset_field }
        }
        #[inline(always)]
        fn element_sizes(sizes: &mut Vec<usize>) -> Result<(), String> {
            <&[u8]>::element_sizes(sizes)?;
            <&[u64]>::element_sizes(sizes)?;
            Ok(())
        }
    }

    #[cfg(test)]
    mod test {
        use crate::Len;

        #[test]
        fn homogeneous_push() {
            let mut d = super::Discriminant::default();
            d.push(2, 0);
            d.push(2, 1);
            d.push(2, 2);
            assert_eq!(d.len(), 3);
            assert_eq!(d.homogeneous(), Some(2));
            assert!(d.variant.is_empty());
            // offset holds [tag, count] = [3, 3] in homogeneous mode.
            assert_eq!(d.offset, vec![3, 3]);
        }

        #[test]
        fn heterogeneous_transition() {
            let mut d = super::Discriminant::default();
            d.push(0, 0);
            d.push(0, 1);
            d.push(1, 0); // transition
            assert_eq!(d.len(), 3);
            assert_eq!(d.homogeneous(), None);
            assert_eq!(d.variant, vec![0, 0, 1]);
            assert_eq!(d.offset, vec![0, 1, 0]);
        }

        #[test]
        fn clear_resets() {
            use crate::Clear;
            let mut d = super::Discriminant::default();
            d.push(1, 0);
            d.push(1, 1);
            d.clear();
            assert_eq!(d.len(), 0);
            // After clear, first push starts homogeneous again.
            d.push(3, 0);
            assert_eq!(d.homogeneous(), Some(3));
            assert_eq!(d.len(), 1);
        }

        #[test]
        fn borrow_index() {
            use crate::Borrow;
            let mut d = super::Discriminant::default();
            d.push(2, 0);
            d.push(2, 1);
            d.push(2, 2);
            let b = d.borrow();
            assert_eq!(b.get(0), (2, 0));
            assert_eq!(b.get(1), (2, 1));
            assert_eq!(b.get(2), (2, 2));
        }

        #[test]
        fn borrow_index_heterogeneous() {
            use crate::Borrow;
            let mut d = super::Discriminant::default();
            d.push(0, 0);
            d.push(1, 0);
            d.push(0, 1);
            let b = d.borrow();
            assert_eq!(b.get(0), (0, 0));
            assert_eq!(b.get(1), (1, 0));
            assert_eq!(b.get(2), (0, 1));
        }
    }
}
