use crate::{matrix::DenseMatrix, r10::CodeParameters};

impl CodeParameters {
    pub fn a_systematic_intermediate_matrix(&self) -> DenseMatrix {
        let mut a = DenseMatrix::from_element(
            self.num_intermediate_symbols(),
            self.num_intermediate_symbols(),
            false,
        );

        self.a_systematic_intermediate(|i, j| {
            a[(i, j)] = true;
        });

        a
    }
}
