use std::{
    fmt::{Display, Formatter},
    ops::{Index, IndexMut},
};

use crate::matrix::{rc_permutation::RCPermutation, DenseMatrix};

#[derive(Clone, Debug)]
pub struct RCSwapMatrix {
    pub mat: DenseMatrix,
    pub row_permutation: RCPermutation,
    pub column_permutation: RCPermutation,
}

impl RCSwapMatrix {
    pub fn from_dmatrix(mat: DenseMatrix) -> RCSwapMatrix {
        let row_permutation = RCPermutation::new(mat.nrows());
        let column_permutation = RCPermutation::new(mat.ncols());

        RCSwapMatrix {
            mat,
            row_permutation,
            column_permutation,
        }
    }

    pub fn nrows(&self) -> usize {
        self.mat.nrows()
    }

    pub fn ncols(&self) -> usize {
        self.mat.ncols()
    }

    pub fn swap_rows(&mut self, a: usize, b: usize) {
        self.row_permutation.swap(a, b);
    }

    pub fn swap_columns(&mut self, a: usize, b: usize) {
        self.column_permutation.swap(a, b);
    }

    // row[a] -= row[b]
    pub fn row_sub_assign(&mut self, a: usize, b: usize) {
        let a_phys = self.row_permutation.index(a);
        let b_phys = self.row_permutation.index(b);

        for k in 0..self.mat.ncols() {
            self.mat[(a_phys, k)] ^= self.mat[(b_phys, k)];
        }
    }

    pub fn to_dmatrix(&self) -> DenseMatrix {
        DenseMatrix::from_fn(self.mat.nrows(), self.mat.ncols(), |i, j| self[(i, j)])
    }
}

impl Display for RCSwapMatrix {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.to_dmatrix())
    }
}

impl Index<(usize, usize)> for RCSwapMatrix {
    type Output = bool;

    fn index(&self, index: (usize, usize)) -> &Self::Output {
        &self.mat[(
            self.row_permutation.index(index.0),
            self.column_permutation.index(index.1),
        )]
    }
}

impl IndexMut<(usize, usize)> for RCSwapMatrix {
    fn index_mut(&mut self, index: (usize, usize)) -> &mut Self::Output {
        &mut self.mat[(
            self.row_permutation.index(index.0),
            self.column_permutation.index(index.1),
        )]
    }
}
