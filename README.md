# Columnar Encoding/Decoding #

This is a pretty simple start to columnar encoding and decoding in Rust. For the moment it just works on uints, pairs, vectors, and combinations thereof. Some extensions are pretty obvious (to other base types, tuples of other arities), but I'll need to get smarter to handle enumerations, user defined structs, and such.

## Columnar What? ##

Columnarization is a transformation of vectors of structured types to a collection of vectors of base types and lengths. Roughly speaking, there are three types of rules we use, depending on what types we are presented with:

`Vec<uint>` : For vectors of base types, we just stop. We can't do any more, and this will encode/decode fast enough.

`Vec<(T1, T2)>` : We transform vectors of pairs to pairs of vectors `(Vec<T1>, Vec<T2>)` and recursively process both of the vectors.

`Vec<Vec<T>>` : Vectors of vectors are transformed to a pair `(Vec<uint>, Vec<T>)` indicating the lengths and concatenated payloads. We then recursively process both of the vectors.

These rules (and variants of them for other base types, structures, and collections, respectively) allow fast encoding and decoding of a rich set of datatypes.

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
