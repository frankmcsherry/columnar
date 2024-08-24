# Columnar

A crate to convert from arrays of (complex) structs to (simple) structs of arrays.

The crate flattens non-trivial types including those built out of product (`struct`), sum (`enum`), and list (`Vec`) combinators.
The result of flattening is a type that contains a small number of Rust vectors, independent of the number of flattened elements.
Each of the Rust vectors contains only primitive types.

## Trying it out

Once you've got the repository and Rust you should be able to type `cargo build`. 
This shouldn't do very much. 
Typing `cargo bench` will spin up the benchmarking subsystem, and should print some throughputs for a few different types. 
At the moment, the outputs look like:
```
running 16 tests
test empty_clone      ... bench:         688.73 ns/iter (+/- 460.78)
test empty_copy       ... bench:         494.13 ns/iter (+/- 7.09)
test string10_clone   ... bench:  53,945,912.50 ns/iter (+/- 1,794,059.94)
test string10_copy    ... bench:   2,844,987.50 ns/iter (+/- 57,926.98)
test string20_clone   ... bench:  29,072,633.30 ns/iter (+/- 543,691.19)
test string20_copy    ... bench:   2,764,954.20 ns/iter (+/- 82,258.34)
test u32x2_clone      ... bench:     671,958.07 ns/iter (+/- 118,292.74)
test u32x2_copy       ... bench:   1,075,690.61 ns/iter (+/- 30,711.97)
test u64_clone        ... bench:     720,584.90 ns/iter (+/- 248,134.69)
test u64_copy         ... bench:     230,607.33 ns/iter (+/- 59,759.27)
test u8_u64_clone     ... bench:     703,132.30 ns/iter (+/- 70,911.66)
test u8_u64_copy      ... bench:     544,089.58 ns/iter (+/- 17,622.37)
test vec_u_s_clone    ... bench:  57,322,495.80 ns/iter (+/- 397,200.82)
test vec_u_s_copy     ... bench:   3,296,437.50 ns/iter (+/- 16,236.46)
test vec_u_vn_s_clone ... bench:  60,941,275.10 ns/iter (+/- 609,691.60)
test vec_u_vn_s_copy  ... bench:   3,738,352.05 ns/iter (+/- 88,405.26)

test result: ok. 0 passed; 0 failed; 0 ignored; 16 measured; 0 filtered out; finished in 95.39s
```

The numbers come in pairs, and for each pair represent the time to process some typed vector.
The `_clone` numbers amount to cloning the typed vector, as you might if you needed to store it in a `Vec`.
The `_copy` numbers copy a reference into a columnar representation, also capturing the data but changing its layout.

## Columnar what? ##

Columnarization is the term I know for the transformation of vectors of structured types to a collection of vectors of base types.
Other terms might include "flattening" or ["struct of arrays"](https://en.wikipedia.org/wiki/AoS_and_SoA).
For simple structures, you can think of it as 'transposing' the data representation, so that there is a vector for each field of the structure, each with a length equal to the initial number of records. 
For structures involving vectors there is an additional recursive flattening step to avoid having vectors of vectors.

Informally, the transformation proceeds through structural induction on the type.
1. A vector of a base type remain as a vector of that base type.
2. A vector of a product type is transformed to a product of vectors of simpler types.
3. A vector of a sum type is transformed to a product of vectors of simpler types, and a discriminant for each item.
4. A vector of a list type is transformed to a product of a vector of offsets and a vector of the simpler type.

## Implementation details ##

The transformation is based around a trait `Columnar<T>` for types that can represent `T` columnarly.
The trait has several methods, but the most significant are called out here:

```rust
pub trait Columnar<T> {

    /// Copy a reference to an item into `self`.
    fn copy(&mut self, item: &T);

    /// Type returned by the indexing operation.
    /// Meant be similar to `&'a T`, but not the same.
    type Index<'a> where Self: 'a;
    /// A reference to the element at the indicated position.
    fn index(&self, index: usize) -> Self::Index<'_>;

    // other methods exist ...
}
```

The crate then implements `Columnar<T>` for various `T`, including those `T` that are the result of product, sum, and list combinators.
