# Columnar Encoding/Decoding #

This is a pretty simple start to columnar encoding and decoding in Rust. For the moment it just works on uints, pairs, vectors, and combinations thereof. Some extensions are pretty obvious (to other base types, tuples of other arities), but I'll need to get smarter to handle enumerations, user defined structs, and such.

## Trying it out ##

Once you've got the repository, Rust and Cargo, just type `cargo run --release` and it should start showing you throughput for encoding, decoding, and verifying the results. Popping open `main.rs` should let you try to encode different sorts of objects.

## Limitations ##

Right now Vec types are a bit slow because of allocations required. There is a test type `ColVec` which deserializes from the same binary representation as a Vec, but is essentially a slice of a larger allocation. Those go a bit faster, but require that your deserialized type contain them.
