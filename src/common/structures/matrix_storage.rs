//! Trait bound for matrix-backed sketches.

pub trait MatrixStorage<T: Clone> {
    type HashValue;
    fn rows(&self) -> usize;
    fn cols(&self) -> usize;

    fn update_one_counter<F, V>(&mut self, row: usize, col: usize, op: F, value: V)
    where
        F: Fn(&mut T, V);

    fn increment_by_row(&mut self, row: usize, col: usize, value: T);

    fn fast_insert<F, V>(&mut self, op: F, value: V, hashed_val: Self::HashValue)
    where
        F: Fn(&mut T, &V, usize),
        V: Clone;

    fn fast_query_min<F, R>(&self, hashed_val: Self::HashValue, op: F) -> R
    where
        F: Fn(&T, usize, Self::HashValue) -> R,
        R: Ord;

    fn fast_query_median<F>(&self, hashed_val: Self::HashValue, op: F) -> f64
    where
        F: Fn(&T, usize, Self::HashValue) -> f64;

    fn query_one_counter(&self, row: usize, col: usize) -> T;
}
