use crate::matrix::DenseMatrix;

impl DenseMatrix {
    // Check whether self is invertible.
    pub fn check_invertible(self) -> bool {
        self.rowwise_elimination_gaussian(|_| {}).is_ok()
    }
}
