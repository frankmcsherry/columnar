# Columnar Encoding/Decoding #

This is a pretty simple start to columnar encoding and decoding in Rust. For the moment it just works on integers (unsigned, signed, and of varying widths), pairs, vectors, options, and combinations thereof. Some extensions are pretty obvious (to other base types, tuples of other arities), and you can implement the trait for your own structs and enumerations with just a bit of copy/paste, but I'll need to get smarter to handle these automatically.


## Trying it out ##

Once you've got the repository, Rust and Cargo, you should be able to type `cargo build`. This shouldn't do very much. Typing `cargo test` will spin up the testing subsystem and build `examples/example.rs`, which does some measurement. Unfortunately, at the moment cargo only builds tests without optimizations, so this thoughtful example, which you can run using `./target/examples/example` will show some pretty dodgy numbers. Instead, you can trust me when I say:

For example, for the type `(uint, (uint, uint))` we see the pretty sweet results:
```
Encoding/decoding at 5.389231GB/s
```
If we do a more complicated type, `Vec<Vec<uint>>`, we get less sweet results:
```
Encoding/decoding at 2.456752GB/s
```
Vectors require a bit more logic than structures of basic types, but the fact that we get throughput this high is one of the cooler features of columnarization.

As a caveat, and as printed out by the code, it is likely that the measured numbers are more optimistic than you should expect, because they are produced in a test harness that Rust and LLVM can see and optimize around. Efforts have been made to minimize this, but at these throughputs even one additional copy (staging the outputs for transmission, for example) can have a big impact.

## Columnar what? ##

Columnarization is a transformation of vectors of structured types to a collection of vectors of base types. For simple structures, you can think of it as 'rotating' the data representation, so that there is a vector for each field of the structure, each with a length equal to the initial number of records. For structures involving vectors there is an additional recursive flattening step to avoid having vectors of vectors.

One way to view columnarization, close to the implemented code, is as a transformation on `Vec<T>`, where the transformation depends on the structure of the type `T`. The transformations continue recursively, until we have only vectors of base types. There are three types of rules we use:

`Vec<uint>` : We leave vectors of base types as they are.

`Vec<(T1, T2)>` : We transform to `(Vec<T1>, Vec<T2>)` and recursively process both of the vectors.

`Vec<Vec<T>>` : We transform to `(Vec<uint>, Vec<T>)`, containing the vector lengths and concatenated payloads, and recursively process the second vector.

These transformations can be relatively efficient because each of the element moves is of typed data with known size, into a vector of identically typed elements. Once transformed, the data are easily serialized because the vectors of base types can be easily re-cast as vectors of bytes.

## Implementation details ##

The columnarization is based around a fairly simple trait, `ColumnarVec<T>` implementing the methods `push` and `pop` from Rust's `Vec<T>`, but also the ability to `encode` the contents to a sequence of raw binary arrays, and `decode` a sequence of binary arrays to reload them into the `ColumnarVec`.

```rust
pub trait ColumnarVec<T>
{
    fn push(&mut self, T);
    fn pop(&mut self) -> Option<T>;

    fn encode(&mut self, &mut Vec<Vec<u8>>);
    fn decode(&mut self, &mut Vec<Vec<u8>>);
}
```

Each of the three cases above have their own implementations, and that is really all there is to the code. Let's take a look at each of them now.

### uint and base types ###

The `ColumnarVec<uint>` implementation is simply a `Vec<uint>` whose calls to `push` and `pop` fall through. When we need to `encode` and `decode`, we unsafely cast between `Vec<uint>` and a `Vec<u8>` either stashing the result in the list, or popping from the list and installing as the `Vec<uint>`. Any appropriate types (ones where it is safe to use Rust's `from_raw_parts` to assemble a new `Vec` from existing parts) can be implemented this way. I don't actually know what these types are, or if there are any guarantees.

### Pairs and tuples ###

Importantly, because the record passed in to `push` is now owned by the `ColumnarVec` we can destructure it and push its elements into
separate typed arrays. For example, the `ColumnarVec<(T1, T2)>` is a pair of `R1: ColumnarVec<T1>` and `R2: ColumnarVec<T2>`:

```rust
impl<T1, R1, T2, R2> ColumnarVec<(T1, T2)> for (R1, R2)
where R1: ColumnarVec<T1>,
      R2: ColumnarVec<T2>,
{
    #[inline(always)]
    fn push(&mut self, (x, y): (T1, T2))
    {
        self.mut0().push(x);
        self.mut1().push(y);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<(T1, T2)>
    {
        // panic! if the second does not exist (malformed input).
        self.mut0().pop().map(|x| (x, self.mut1().pop().unwrap()))
    }

    fn encode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.mut0().encode(buffers);
        self.mut1().encode(buffers);
    }
    fn decode(&mut self, buffers: &mut Vec<Vec<u8>>)
    {
        self.mut1().decode(buffers);
        self.mut0().decode(buffers);
    }
}
```

### Vectors and collections ###

One subtle but important point is that `push` takes ownership of the record that comes in. This not only allows us to rip apart the record, but also to retain any memory it has allocated, which can be super helpful to avoid chatting with the allocator when we need to produce output vectors in `pop`. Consider `push` and `pop` in our implementation of `ColumnarVec<Vec<T>>`, which could have just been a pair of `R1: ColumnarVec<uint>` and `R2: ColumnarVec<T>`, but which we augment with a `Vec<Vec<T>>` to stash empty-but-allocated arrays:

```rust
impl<T, R1, R2> ColumnarVec<Vec<T>> for (R1, R2, Vec<Vec<T>>)
where R1: ColumnarVec<uint>,
      R2: ColumnarVec<T>,
{
    #[inline(always)]
    fn push(&mut self, mut vector: Vec<T>)
    {
        self.mut0().push(vector.len());
        while let Some(record) = vector.pop() { self.mut1().push(record); }
        self.mut2().push(vector);
    }

    #[inline(always)]
    fn pop(&mut self) -> Option<Vec<T>>
    {
        if let Some(count) = self.mut0().pop()
        {
            let mut vector = self.mut2().pop().unwrap_or(Vec::new());
            for _ in range(0, count) { vector.push(self.mut1().pop().unwrap()); }
            Some(vector)
        }
        else { None }
    }

    // encode and decode just call encode and decode on R1 and R2 fields
    // ...
}
```

Not only do we flatten down all of the `Vec<T>` vectors to one `Vec<T>`, we also stash the now-empty `Vec<T>`s for later re-use. This means in steady state of encoding and decoding (for example, sending to and receiving from your peers) we don't need to interact very much with the allocator, generally a good state to be in.

## What's next? ##

I'm going to start using it. I'll almost certainly need to add support for a few more types (e.g. `Option<T>`, which has landed), and with enough interest in procedural macros (Rust's codegen) I may try automating implementations for user-defined structs and enums.

I should go ahead and add support for the always popular `String` type, and probably make a default implementation of `Encodable` and `Decodable` for anything implementing the `ColumnarVec` trait.

If you have any other thoughts, let me know!
