use std::{
    fmt::{Display, Formatter},
    ops::{Index, IndexMut},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DenseMatrix {
    data: Vec<bool>,
    nrows: usize,
    ncols: usize,
}

impl DenseMatrix {
    pub fn from_element(nrows: usize, ncols: usize, elem: bool) -> DenseMatrix {
        let data = vec![elem; nrows * ncols];

        DenseMatrix { data, nrows, ncols }
    }

    pub fn from_fn(
        nrows: usize,
        ncols: usize,
        mut f: impl FnMut(usize, usize) -> bool,
    ) -> DenseMatrix {
        let mut data = Vec::with_capacity(nrows * ncols);

        for i in 0..nrows {
            for j in 0..ncols {
                data.push(f(i, j));
            }
        }

        DenseMatrix { data, nrows, ncols }
    }

    pub fn nrows(&self) -> usize {
        self.nrows
    }

    pub fn ncols(&self) -> usize {
        self.ncols
    }
}

impl Display for DenseMatrix {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(f)?;

        for i in 0..self.nrows {
            write!(f, "  |")?;

            for j in 0..self.ncols {
                if self[(i, j)] {
                    write!(f, " 1")?;
                } else {
                    write!(f, " 0")?;
                }
            }

            writeln!(f, " |")?;
        }

        Ok(())
    }
}

impl Index<(usize, usize)> for DenseMatrix {
    type Output = bool;

    fn index(&self, index: (usize, usize)) -> &bool {
        &self.data[index.0 * self.ncols + index.1]
    }
}

impl IndexMut<(usize, usize)> for DenseMatrix {
    fn index_mut(&mut self, index: (usize, usize)) -> &mut bool {
        &mut self.data[index.0 * self.ncols + index.1]
    }
}
