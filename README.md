# Columnar Encoding/Decoding #

This is a pretty simple start to columnar encoding and decoding in Rust. For the moment it just works on uints, pairs, vectors, and combinations thereof. Some extensions are pretty obvious (to other base types, tuples of other arities), but I'll need to get smarter to handle enumerations, user defined structs, and such.

## Trying it out ##

Once you've got the repository, Rust and Cargo, just type `cargo run --release` and it should start showing you throughput for encoding, decoding, and verifying the results. Popping open `main.rs` should let you try to encode different sorts of objects. For example, for `(uint, (uint, uint))`s we see

```
Encoding/decoding/validating at 5.221653GB/s
Encoding/decoding/validating at 5.095162GB/s
Encoding/decoding/validating at 5.125992GB/s
```

If we do a more complicated `((uint, (uint, uint)), Vec<Vec<uint>>)` we get something more like

```
Encoding/decoding/validating at 0.937136GB/s
Encoding/decoding/validating at 0.919903GB/s
Encoding/decoding/validating at 0.917874GB/s
```

Right now Vec types are a bit slow because of allocations required. There is a test type `ColVec` which deserializes from the same binary representation as a Vec, but is essentially a slice of a larger allocation. Those go a bit faster, but require that your deserialized type contain them.
