/// A constant with all possible boolean values.
pub const BOOLEAN_DOMAIN: [bool; 2] = [true, false];

/// Testing helpers for types that implement [[Clone]].
pub trait CloneExt: Sized {
    /// Execute `thunk` with a copy of `self`.
    ///
    /// Convenient for creating scoped clone.
    fn with_copy<F>(&self, thunk: F)
    where
        F: FnOnce(Self);
}

impl<T> CloneExt for T
where
    T: Clone,
{
    fn with_copy<F>(&self, thunk: F)
    where
        F: FnOnce(T),
    {
        let copy: T = self.clone();
        thunk(copy);
    }
}

pub type SVec16<T> = SmallVec<T, 16>;
// pub const fn svec16<T, const M: usize>(input: [T; M]) -> SVec16<T>
// where
//     T: Copy,
// {
//     SVec16::from_array(input)
// }
#[macro_export]
macro_rules! svec16 {
    ($($elem:expr),* $(,)?) => {{
        SVec16::from_array([$($elem),*])
    }};
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SmallVec<T, const N: usize> {
    data: [Option<T>; N],
    len: usize,
}

impl<T: Copy, const N: usize> SmallVec<T, N> {
    pub const fn new() -> Self {
        Self {
            data: [None; N],
            len: 0,
        }
    }

    pub const fn from_array<const M: usize>(input: [T; M]) -> Self {
        assert!(M <= N);

        let mut data = [None; N];
        let mut i = 0;
        while i < M {
            data[i] = Some(input[i]);
            i += 1;
        }

        Self { data, len: M }
    }

    pub const fn len(&self) -> usize {
        self.len
    }
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub const fn get(&self, idx: usize) -> Option<&T> {
        if idx < self.len {
            self.data[idx].as_ref()
        } else {
            None
        }
    }

    pub const fn at(&self, idx: usize) -> &T {
        match self.get(idx) {
            Some(v) => v,
            None => panic!("index out of bounds"),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = T> + '_ {
        self.data[..self.len].iter().map(|x| x.unwrap())
    }
}
impl<T: Copy, const N: usize> Default for SmallVec<T, N> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T: Copy, const N: usize> FromIterator<T> for SmallVec<T, N> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut out = SmallVec::<T, N>::new();
        let mut i = 0;

        for v in iter {
            if i >= N {
                panic!("SmallVec capacity exceeded");
            }
            out.data[i] = Some(v);
            i += 1;
        }

        out.len = i;
        out
    }
}
