# Columnar Encoding/Decoding #

This is a pretty simple start to columnar encoding and decoding in Rust. For the moment it just works on uints, pairs, vectors, and combinations thereof. Some extensions are pretty obvious (to other base types, tuples of other arities), but I'll need to get smarter to handle enumerations, user defined structs, and such.

## Columnar What? ##

Columnarization is a transformation of vectors of structured types to a collection of vectors of base types and lengths. Roughly speaking, it will take a `Vec<(T1, T2)>` and transform it to a `(Vec<T1>, Vec<T2>)` each component of which can be more easily serialized and deserialized. If `T1` and `T2` are not both base types then there is more work to do.

If a nested type is a pair, or some other structure, you can just apply the rule again and we'll just get more vectors out. When we re-assemble them we'll need to pay attention to what goes where, but it isn't very complicated.


If a nested type is a `Vec`, we transform `Vec<Vec<T>>` into `(Vec<uint>, Vec<T>)`, indicating the lengths of the arrays and the concatenated data, separately. We may need to continue recursively on the second vector if it is not yet a base type.

## Trying it out ##

Once you've got the repository, Rust and Cargo, just type `cargo run --release` and it should start showing you throughput for encoding, decoding, and verifying the results. Popping open `main.rs` should let you try to encode different sorts of objects. For example, for the type `(uint, (uint, uint))` we see the pretty sweet results:
```
Encoding/decoding/validating at 5.221653GB/s
Encoding/decoding/validating at 5.095162GB/s
Encoding/decoding/validating at 5.125992GB/s
```
If we do a more complicated type, `((uint, (uint, uint)), Vec<Vec<uint>>)`, we get less sweet results:
```
Encoding/decoding/validating at 0.937136GB/s
Encoding/decoding/validating at 0.919903GB/s
Encoding/decoding/validating at 0.917874GB/s
```
Right now Vec types are a bit slow because of allocations required. There is a test type `ColVec` which deserializes from the same binary representation as a Vec, but is essentially a slice of a larger allocation. Those go a bit faster, but require that your deserialized type contain them.
