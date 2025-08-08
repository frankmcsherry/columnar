//! Chain iterators, or iterators and an item. Iterators that might improve inlining, at the
//! expense of not providing iterator maker traits.

/// Chain two iterators together. The result first iterates over `a`, then `b`, until both are
/// exhausted.
///
/// This addresses a quirk where deep iterators would not be optimized to their full potential.
/// Here, functions are marked with `#[inline(always)]` to indicate that the compiler should
/// try hard to inline the iterators.
#[inline(always)]
pub fn chain<A: IntoIterator, B: IntoIterator<Item=A::Item>>(a: A, b: B) -> Chain<A::IntoIter, B::IntoIter> {
    Chain { a: Some(a.into_iter()), b: Some(b.into_iter()) }
}

pub struct Chain<A, B> {
    a: Option<A>,
    b: Option<B>,
}

impl<A, B> Iterator for Chain<A, B>
where
    A: Iterator,
    B: Iterator<Item=A::Item>,
{
    type Item = A::Item;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(a) = self.a.as_mut() {
            let x = a.next();
            if x.is_none() {
                self.a = None;
            } else {
                return x;
            }
        }
        if let Some(b) = self.b.as_mut() {
            let x = b.next();
            if x.is_none() {
                self.b = None;
            } else {
                return x;
            }
        }
        None
    }

    #[inline]
    fn fold<Acc, F>(self, mut acc: Acc, mut f: F) -> Acc
    where
        F: FnMut(Acc, Self::Item) -> Acc,
    {
        if let Some(a) = self.a {
            acc = a.fold(acc, &mut f);
        }
        if let Some(b) = self.b {
            acc = b.fold(acc, f);
        }
        acc
    }
}

/// Chain a single item to an iterator. The resulting iterator first iterates over `a`,
/// then `b`. The resulting iterator is marked as `#[inline(always)]`, which in some situations
/// causes better inlining behavior with current Rust versions.
#[inline(always)]
pub fn chain_one<A: IntoIterator>(a: A, b: A::Item) -> ChainOne<A::IntoIter> {
    ChainOne { a: Some(a.into_iter()), b: Some(b) }
}

pub struct ChainOne<A: Iterator> {
    a: Option<A>,
    b: Option<A::Item>,
}

impl<A: Iterator> Iterator for ChainOne<A> {
    type Item = A::Item;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(a) = self.a.as_mut() {
            let x = a.next();
            if x.is_none() {
                self.a = None;
                self.b.take()
            } else {
                x
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_chain() {
        let a = [1, 2, 3];
        let b = [4, 5, 6];
        let mut chain = chain(a, b);
        assert_eq!(chain.next(), Some(1));
        assert_eq!(chain.next(), Some(2));
        assert_eq!(chain.next(), Some(3));
        assert_eq!(chain.next(), Some(4));
        assert_eq!(chain.next(), Some(5));
        assert_eq!(chain.next(), Some(6));
        assert_eq!(chain.next(), None);
    }

    #[test]
    fn test_chain_one() {
        let a = [1, 2, 3];
        let b = 4;
        let mut chain = chain_one(a, b);
        assert_eq!(chain.next(), Some(1));
        assert_eq!(chain.next(), Some(2));
        assert_eq!(chain.next(), Some(3));
        assert_eq!(chain.next(), Some(4));
        assert_eq!(chain.next(), None);
    }
}
