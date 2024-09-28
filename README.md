# Columnar

A crate to convert from arrays of (complex) structs to structs of (simple) arrays.

The crate provides containers for types built out of product (`struct`), sum (`enum`), and list (`Vec`) combinators (and examples for structurally recursive types, like trees).
The containers represent a sequence of the input types, but are backed by only a small number of Rust allocations (vectors or slices).
Each of the Rust allocations contains only primitive types, and are easily converted to and from (correctly aligned) binary slices `&[u8]`.

The container supports efficient random access for reads, and limited forms of random access for writes.

## An example

Starting from a sequence of complicated types, the `Columnar::as_columns` method converts them to a columnar container.
```rust
// A sequence of complex, nested, and variously typed records.
let records =
(0 .. 1024u64).map(|i| {
    (0 .. i).map(|j| {
        if (i - j) % 2 == 0 {
            Ok((j, "grawwwwrr!".to_string()))
        } else {
            Err(Some(vec![(); 1 << 40]))
        }
    }).collect::<Vec<_>>()
});

// An appendable replacement for `&[T]`: indexable, shareable.
// Layout in memory is a small number of contiguous buffers,
// even though `records` contains many small allocations.
let columns = Columnar::as_columns(records.clone());
```

The contents of `columns` now match the items of `records`.
However, confirming this can be annoying because the reference types returned by `columns` don't exactly match those of `records`.
Here is the code you might write in place of `assert_eq!(columns, records)`:

```rust
// Each item in `columns` matches the original in `records`.
// Equality testing is awkward, because the reference types don't match.
// For example, `Option<&T>` cannot be equated to `&Option<T>` without help,
// and tuples `(&A, &B, &C)` cannot be equated to `&(A, B, C)` without help.
assert_eq(columns.len(), records.clone().count());
for (a, b) in columns.into_iter().zip(records.clone()) {
    assert_eq!(a.len(), b.len());
    for (a, b) in a.into_iter().zip(b) {
        match (a, b) {
            (Ok(a), Ok(b)) => {
                assert_eq!(a.0, &b.0);
                assert_eq!(a.1, b.1);
            },
            (Err(a), Err(b)) => {
                match (a, b) {
                    (Some(a), Some(b)) => { assert_eq!(a.len(), b.len()); },
                    (None, None) => { },
                    _ => { panic!("Variant mismatch"); }
                }
            },
            _ => { panic!("Variant mismatch"); }
        }
    }
}
```

Having transformed the records, `columns` can now report on the allocations backing it.
This prints out fourteen lines, describing the relatively few allocations required (for a half million items).
```rust
// Report the small number of large buffers backing `columns`.
for (align, bytes) in columns.as_bytes() {
    println!("align: {:?}, bytes.len(): {:?}", align, bytes.len());
}
```

Columnar containers are backed by allocations of primitive types that can be converted to binary slices, and back again if their alignment is correct.
This is "zero copy", performing no allocations, copies, or data manipulation.
```rust
// Borrow bytes from `columns`, and reconstruct a borrowed `columns`.
// In practice, we would use serialized bytes from somewhere else.
// Function defined to get type support, relating `T` to `T::Borrowed`.
fn round_trip<T: AsBytes>(container: &T) -> T::Borrowed<'_> {
    // Grab a reference to underlying bytes, as if serialized.
    let mut bytes_iter = container.as_bytes().map(|(_, bytes)| bytes);
    FromBytes::from_bytes(&mut bytes_iter)
}

let borrowed = round_trip(&columns);
```

Columnar containers support zero-cost reshaping of their data.
Here we extract all of the `j` values from the first field of `Ok` variants across all lists.
The sum is performed across one allocation, and benefits from efficient cache access and SIMD support.

```rust
// Project down to columns and variants using only field accessors.
// This gets all Ok(j, _) numbers from across all lists.
let values: &[u64] = borrowed.values.oks.0;
let total = values.iter().sum::<u64>();
println!("Present values summed: {:?}", total);
```

## Advantages

Columnar containers can use **fewer, larger allocations**.
This reduces work for the allocator, both in allocating and deallocating memory.
The memory is also more easily reused, as any small allocations are only delimited regions of the large allocations, without needing to be sized by the allocator.

Columnar containers can use **less memory**.
The data are repacked into contiguous allocations with no padding bytes, which removes waste.
Several container wrappers compress the input, for example using variable sized integers or dictionary references for common values.

Columnar containers support **zero-copy serialization and deserialization**.
The slices of primitive types can be safely converted to `&[u8]` at zero cost, and converted back if the slices are correctly aligned.
It is relatively easy to move data across physical and logical device boundaries, and the ABI is simplified by using few allocations of primitive types.

There are other advantages waiting to be discovered!

## Disadvantages

Columnar containers **may not provide access via the inserted type**.
A container for `(S, T)` provides access to elements using a type `(&S, &T)`, rather than through a `&(S, T)`. 
Similarly, a `Result<S, T>` is provided back as a `Result<&S, &T>`, and other combinators are similar.
This can make the resulting types inoperable for many use cases (for example, as keys in a map).

Columnar containers **may remove locality**.
Containers for `(S, T)` store `S` and `T` separately, and if you wanted to operate on both of them at the same time, you would need to perform two memory accesses rather than one.
The conversion also takes effort to translate the data, for example de-interleaving `(u32, u32)` data into two lists of `u32` data.
While there may be benefits to doing this, there are also costs.

Columnar containers **may not support in-place mutation**.
Containers for `Result<S, T>` may allow you to mutate the `S` or `T`, but do not allow you to change which variant is present.
Containers for `Vec<T>` may allow you to mutate the underlying `T`, but not to resize the lists.

There are other disadvantages waiting to be discovered!

## Trying it out

Once you've got the repository and Rust you should be able to type `cargo build`. 
This shouldn't do very much. 
Typing `cargo bench` will spin up the benchmarking subsystem, and should print some throughputs for a few different types. 
At the moment, the outputs look like:
```
running 18 tests
test empty_clone      ... bench:         636.77 ns/iter (+/- 87.64)
test empty_copy       ... bench:         457.98 ns/iter (+/- 10.35)
test option_clone     ... bench:   3,789,695.85 ns/iter (+/- 562,651.67)
test option_copy      ... bench:   2,277,087.50 ns/iter (+/- 55,481.30)
test string10_clone   ... bench:  54,043,895.80 ns/iter (+/- 678,803.32)
test string10_copy    ... bench:   2,546,083.40 ns/iter (+/- 86,534.62)
test string20_clone   ... bench:  29,324,433.30 ns/iter (+/- 894,392.04)
test string20_copy    ... bench:   2,617,216.60 ns/iter (+/- 22,249.13)
test u32x2_clone      ... bench:     683,007.81 ns/iter (+/- 114,579.60)
test u32x2_copy       ... bench:   1,046,524.47 ns/iter (+/- 57,854.81)
test u64_clone        ... bench:     668,677.09 ns/iter (+/- 158,279.31)
test u64_copy         ... bench:     693,802.61 ns/iter (+/- 458,044.64)
test u8_u64_clone     ... bench:     687,896.89 ns/iter (+/- 78,442.76)
test u8_u64_copy      ... bench:     525,179.18 ns/iter (+/- 47,201.65)
test vec_u_s_clone    ... bench:  57,849,700.00 ns/iter (+/- 1,467,152.48)
test vec_u_s_copy     ... bench:   2,755,285.40 ns/iter (+/- 53,676.63)
test vec_u_vn_s_clone ... bench:  61,311,075.00 ns/iter (+/- 936,117.96)
test vec_u_vn_s_copy  ... bench:   3,328,358.40 ns/iter (+/- 38,148.74)

test result: ok. 0 passed; 0 failed; 0 ignored; 18 measured; 0 filtered out; finished in 89.14s
```

The numbers come in pairs, and for each pair represent the time to process some typed vector.
The `_clone` numbers amount to cloning the typed vector, as you might if you needed to store it in a `Vec`.
The `_copy` numbers copy a reference into a columnar representation, also capturing the data but changing its layout.

## Columnar what? ##

Columnarization is the term I know for the transformation of vectors of structured types to a collection of vectors of base types.
Other terms might include "flattening" or ["struct of arrays"](https://en.wikipedia.org/wiki/AoS_and_SoA).
For simple structures, you can think of it as 'transposing' the data representation, so that there is a vector for each field of the structure, each with a length equal to the initial number of records. 
For more complicated structures, those involving enumerations, vectors, or recursive references to the type, there is additional structural information to track.

Informally, the transformation proceeds through structural induction on the type.
1. A vector of a base type remain as a vector of that base type.
2. A vector of a product type becomes a product of vectors of its simpler types.
3. A vector of a sum type becomes a product of vectors of its simpler types, and a discriminant for each item.
4. A vector of a list type becomes a product of a vector of offsets and a vector of its simpler type.
5. A vector of a recursive type is more complicated, but undergoes the same structural changes with recursive uses replaced by integers.

The transformation is currently for tuples, the `Result` and `Ok` enumerations, and `Vec`.
It is reasonable to imagine `derive` macros, but they are non-trivial for enumerations with more than two variants.

## Implementation details ##

The crate primarily describes columnar container combinators, and traits over them.

The container combinators reflect the ideas just above, and stand in for vectors of products, sums, and lists.
Each combinator is generic over the simpler containers it relies on, though they are generally either owning (like `Vec<T>`) or borrowed (like `&[T]`).
The combinators can be nested; for example, the container for the example above is
```rust
Vecs<Results<Options<(Vec<u64>, Strings)>, Vecs<Empties>>>
```
Names are pluralizations of the type they contain (here `Vec<Result<Option<(u64, String)>, Vec<()>>>`).

The crate is based around two traits, `Push` and `Index`.

The `Push` trait allows a container to describe the types it accepts, and how it will absorb them.
```rust
/// A type that can accept items of type `T`.
pub trait Push<T> {
    /// Pushes an item onto `self`.
    fn push(&mut self, item: T);
}
```
The trait is implemented for containers and combinators for a broad set of pushed types `T`.
Containers implement `Push<T>` for many distinct `T`, for example `String`, `&String`, and `&str`.

The `Index` trait allows a container to describe the type it reports when indexed by a `usize`.
```rust
/// A type that can be indexed by `usize`.
pub trait Index {
    /// The type returned by the `get` method.
    type Ref;
    /// Accesses a specific item by index.
    fn get(&self, index: usize) -> Self::Ref;
}
```
The trait is implemented for owning containers of `Copy` types, and for borrowing containers returning references.
The `Ref` type varies by combinator, but generally reflects the contained type: tuples of references for tuple containers, `Result` of references for the `Results` container, etc.
The trait does not use a GAT `Ref<'a>`, and instead is implemented for `&'a Container` types instead, which may end up being a mistake.

## Bonus

There are several container combinators that implement largely unsurprising compression techniques.
For example, one can contain `usize` values in a `Results` container of `u8`, `u16`, `u32`, and `u64`, using the variant appropriate to the contained value, at the storage cost of one additional bit per byte.
There is a dictionary coding container that represents new elements in `Ok` variants, and references into the 256 most recent elements using an `Err(u8)` variant.

Several other familiar abstract datatypes, like [rank-select bitvectors](https://en.wikipedia.org/wiki/Succinct_data_structure) and [Roaring bitmaps](https://roaringbitmap.org), can be represented with low overhead.
Work is underway on columnar [JSON](https://www.json.org/) representation, and [adaptive radix trees](https://db.in.tum.de/~leis/papers/ART.pdf), though these are even more hobbies than the crate itself.