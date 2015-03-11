# Columnar Encoding/Decoding #

This is a pretty simple start to columnar encoding and decoding in Rust. For the moment it just works on integers (unsigned, signed, and of varying widths), pairs, vectors, options, and combinations thereof. Some extensions are pretty obvious (to other base types, tuples of other arities), and you can implement the trait for your own structs and enumerations with just a bit of copy/paste, but I'll need to get smarter to handle these automatically.


## Trying it out ##

Once you've got the repository, Rust and Cargo, you should be able to type `cargo build`. This shouldn't do very much. Typing `cargo bench` will spin up the benchmarking subsystem, and should print some throughputs for a few different types. At the moment, the outputs look like
```
% cargo bench
     Running target/release/columnar-1f4357aec01a1091

running 5 tests
test option_u64         ... bench:      4833 ns/iter (+/- 1072) = 2125 MB/s
test u64                ... bench:      2960 ns/iter (+/- 504) = 5540 MB/s
test u64_vec_string_u64 ... bench:     51064 ns/iter (+/- 12140) = 737 MB/s
test u64_x3             ... bench:     16256 ns/iter (+/- 2562) = 3026 MB/s
test vec_vec_u64        ... bench:     17803 ns/iter (+/- 6069) = 1153 MB/s

test result: ok. 0 passed; 0 failed; 0 ignored; 5 measured
```

These numbers are throughputs for a full round trip from typed Rust vectors (`Vec<T>`) to binary (`Vec<u8>`) and back again, for a variety of types. The throughputs depend on the type of data (some require more per-record logic than others), and the amount of data moved around.

## Columnar what? ##

Columnarization is a transformation of vectors of structured types to a collection of vectors of base types. For simple structures, you can think of it as 'transposing' the data representation, so that there is a vector for each field of the structure, each with a length equal to the initial number of records. For structures involving vectors there is an additional recursive flattening step to avoid having vectors of vectors.

One way to view columnarization, close to the implemented code, is as a transformation on `Vec<T>`, where the transformation depends on the structure of the type `T`. The transformations continue recursively, until we have only vectors of base types. There are three types of rules we use:

`Vec<uint>` : We leave vectors of base types as they are.

`Vec<(T1, T2)>` : We transform to `(Vec<T1>, Vec<T2>)` and recursively process both of the vectors.

`Vec<Vec<T>>` : We transform to `(Vec<uint>, Vec<T>)`, containing the vector lengths and concatenated payloads, and recursively process the second vector.

These transformations can be relatively efficient because each of the element moves is of typed data with known size, into a vector of identically typed elements. Once transformed, the data are easily serialized because the vectors of base types can be easily re-cast as vectors of bytes.

## Implementation details ##

The columnarization is based around a fairly simple trait, `ColumnarStack<T>` implementing the methods `push` and `pop`, but also the ability to `encode` the contents to an arbitrary binary writer (any type implementing the `Write` trait), and `decode` from an arbitrary binary reader (any type implementing the `Read` trait).

```rust
pub trait ColumnarStack<T> {
    fn push(&mut self, T);
    fn pop(&mut self) -> Option<T>;

    fn encode<W: Write>(&mut self, &mut W) -> Result<()>;
    fn decode<R: Read>(&mut self, &mut R) -> Result<()>;
}
```

Each of the three cases above have their own implementations, and that is really all there is to the code. Let's take a look at each of them now.

### u64 and base types ###

The `ColumnarStack<u64>` implementation is simply a `Vec<u64>` whose calls to `push` and `pop` fall through. When we need to `encode` and `decode`, we unsafely cast the `Vec<u64>` to a `Vec<u8>` and either write or read a length and binary contents. This approach is safe for several of Rust's base types; I've used the constraint `T: Copy`, which indicates types for which it is safe to just copy the binary representation of the data.

### Pairs and tuples ###

Importantly, because the record passed in to `push` is now owned by the `ColumnarStack` we can destructure it and push its elements into separate typed arrays. For example, the `ColumnarStack<(T1, T2)>` is a pair of `R1: ColumnarStack<T1>` and `R2: ColumnarStack<T2>`:

```rust
impl<T1, R1: ColumnarStack<T1>, T2, R2: ColumnarStack<T2>> ColumnarStack<(T1, T2)> for (R1, R2) {
    #[inline(always)]
    fn push(&mut self, (x, y): (T1, T2)) {
        self.0.push(x);
        self.1.push(y);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<(T1, T2)> {
        match (self.0.pop(), self.1.pop()) {
            (Some(x), Some(y)) => Some((x, y)),
            (None, None)       => None,
            _                  => panic!("malformed data"),
        }
    }

    fn encode<W: Write>(&mut self, writer: &mut W) -> Result<()> {
        self.0.encode(writer);
        self.1.encode(writer);
        Ok(())
    }
    fn decode<R: Read>(&mut self, reader: &mut R) {
        self.0.decode(reader);
        self.1.decode(reader);
        Ok(())
    }
}
```

### Vectors and collections ###

One subtle but important point is that `push` takes ownership of the record that comes in. This not only allows us to rip apart the record, but also to retain any memory it has allocated, which can be super helpful to avoid chatting with the allocator when we need to produce output vectors in `pop`. Consider `push` and `pop` in our implementation of `ColumnarStack<Vec<T>>`, which could have just been a pair of `R1: ColumnarStack<u64>` and `R2: ColumnarStack<T>`, but which we augment with a `Vec<Vec<T>>` to stash empty-but-allocated arrays:

```rust
impl<T, R1: ColumnarStack<u64>, R2: ColumnarStack<T>> ColumnarStack<Vec<T>> for (R1, R2, Vec<Vec<T>>) {
    #[inline(always)]
    fn push(&mut self, mut vector: Vec<T>) {
        self.0.push(vector.len() as u64);
        while let Some(record) = vector.pop() { self.1.push(record); }
        self.2.push(vector);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Vec<T>> {
        if let Some(count) = self.0.pop() {
            let mut vector = self.2.pop().unwrap_or(Vec::new());
            for _ in range(0, count) { vector.push(self.1.pop().unwrap()); }
            Some(vector)
        }
        else { None }
    }

    // encode and decode just call encode and decode on R1 and R2 fields
    // ...
}
```

Not only do we flatten down all of the `Vec<T>` vectors to one `Vec<T>`, we also stash the now-empty `Vec<T>`s for later re-use. This means in steady state of encoding and decoding (for example, sending to and receiving from your peers) we don't need to interact very much with the allocator, generally a good state to be in.

### Columnar trait

Finally, there is a `Columnar` trait which is implemented for types with a specific type of `ColumnarStack` supporting their type. This is because there are some types `T`, for example `(u64, u64)`, with multiple implementations of `ColumnarStack<T>`. In the case of `ColumnarStack<(u64, u64)>`, it is implemented both by `(Vec<u64>, Vec<u64>)` and `Vec<(u64, u64)>`, using the pair destructuring or the observation that `(u64, u64): Copy`. Because there are multiple implementations, we need the type `(u64, u64)` to indicate which `ColumnarStack` to use, and this information lives in the `Columnar` trait.
