use columnar::{ColumnarEncoder, ColumnarDecoder};

impl<T: ColumnarEncode<T>> ColumnarEncode<ColVec<T>> for ColVec<T>
{
    #[inline(always)]
    fn encode<'a, K: Iterator<&'a ColVec<T>>>(writers: &mut Vec<Vec<u8>>, count: uint, iterator: || -> K)
    {
        let mut total = 0u;
        for i in iterator() { total += i.length; }
        ColumnarEncode::encode(writers, count, || iterator().map(|ref x| &x.length));
        ColumnarEncode::encode(writers, total, || iterator().next().expect("").buffer.iter());
    }
}
impl<T: ColumnarDecode<T, K>, K: Iterator<T>> ColumnarDecode<ColVec<T>, ColVectorIterator<T>> for ColVec<T>
{
    #[inline(always)]
    fn decode(buffers: &mut Vec<Vec<u8>>, _count: uint, hint: &ColVec<T>) -> ColVectorIterator<T>
    {
        // determine the number of elements for each vector.
        let reader = buffers.pop().expect("missing reader");
        let counts = unsafe { to_typed_vec(reader) };

        let mut total = 0u;
        for i in counts.iter() { total += *i; }

        let iter: K = ColumnarDecode::decode(buffers, total, &Default::default());

        let result: ColVectorIterator<T> = ColVectorIterator::new(iter, counts, total);

        result
    }
}

pub struct ColVectorIterator<T>
{
    counts: Vec<uint>,
    buffer: Rc<Vec<T>>,
    offset: uint,
    finger: uint,
}

impl<T:'static> ColVectorIterator<T>
{
    #[inline(always)]
    fn new<K: Iterator<T>>(mut iter: K, counts: Vec<uint>, total: uint) -> ColVectorIterator<T>
    {
        let mut buffer = Vec::with_capacity(total);
        for i in iter
        {
            buffer.push(i);
        }

        ColVectorIterator { counts: counts, buffer: Rc::new(buffer), offset: 0, finger: 0 }
    }
}

impl<T:'static> Iterator<ColVec<T>> for ColVectorIterator<T>
{
    #[inline(always)]
    fn next(&mut self) -> Option<ColVec<T>>
    {
        if self.finger < self.counts.len()
        {
            let result = ColVec { buffer: self.buffer.clone(), offset: self.offset, length: self.counts[self.finger] };

            self.offset += self.counts[self.finger];
            self.finger += 1;

            Some(result)
        }
        else
        {
            None
        }
    }
}

#[deriving(Default, Clone)]
pub struct ColVec<T>
{
    pub buffer: Rc<Vec<T>>,
    pub offset: uint,
    pub length: uint,
}

impl<T> Index<uint, T> for ColVec<T>
{
    fn index<'a>(&'a self, index: &uint) -> &'a T
    {
        &(self.buffer[self.offset + *index])
    }
}
