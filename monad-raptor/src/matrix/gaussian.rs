// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use crate::matrix::{DenseMatrix, RCSwapMatrix, RowOperation};

impl DenseMatrix {
    // Compute a row-wise Gaussian elimination schedule for a.
    fn rowwise_elimination_schedule(
        self,
        mut row_operation: impl FnMut(RowOperation),
        elimination_strategy_fn: impl Fn(&RCSwapMatrix, usize) -> Option<(usize, usize)>,
    ) -> Result<(), String> {
        assert!(self.nrows() >= self.ncols());

        let mut a = RCSwapMatrix::from_dmatrix(self);

        for step in 0..a.ncols() {
            let (row, col) = elimination_strategy_fn(&a, step)
                .ok_or(())
                .map_err(|_| "elimination_strategy_fn failed".to_string())?;

            // Move the leading element of this row to (step, step).
            if row != step {
                a.swap_rows(row, step);
            }

            if col != step {
                a.swap_columns(col, step);
            }

            // Subtract this row from every other row where necessary.
            for i in 0..a.nrows() {
                if i != step && a[(i, step)] {
                    a.row_sub_assign(i, step);

                    row_operation(RowOperation::SubAssign {
                        i: a.row_permutation.index(i),
                        j: a.row_permutation.index(step),
                    });
                }
            }
        }

        for i in 0..a.nrows() {
            for j in 0..a.ncols() {
                debug_assert_eq!(a[(i, j)], i == j);
            }
        }

        Ok(())
    }

    pub fn rowwise_elimination_gaussian(
        self,
        row_operation: impl FnMut(RowOperation),
    ) -> Result<(), String> {
        self.rowwise_elimination_schedule(row_operation, |a, step| {
            // In step 'step', find the first row that has column 'step' set.
            (step..a.nrows())
                .find(|&row| a[(row, step)])
                .map(|row| (row, step))
        })
    }

    pub fn rowwise_elimination_gaussian_partial_pivot(
        self,
        row_operation: impl FnMut(RowOperation),
    ) -> Result<(), String> {
        self.rowwise_elimination_schedule(row_operation, |a, step| {
            // In step 'step', find the row that has column 'step' set that has minimal row weight.
            let mut best = None;
            let mut best_weight = 0;

            for row in step..a.nrows() {
                if a[(row, step)] {
                    let mut row_weight = 0;

                    for i in step..a.ncols() {
                        if a[(row, i)] {
                            row_weight += 1;
                        }
                    }

                    if best.is_none() || row_weight < best_weight {
                        best = Some(row);
                        best_weight = row_weight;
                    }
                }
            }

            best.map(|row| (row, step))
        })
    }

    pub fn rowwise_elimination_gaussian_full_pivot(
        self,
        row_operation: impl FnMut(RowOperation),
    ) -> Result<(), String> {
        self.rowwise_elimination_schedule(row_operation, |a, step| {
            // In each step, find the row with minimal non-zero weight.
            let mut best = None;

            for row in step..a.nrows() {
                let mut weight = 0;
                let mut lead_column = None;

                for col in 0..a.ncols() {
                    if a[(row, col)] {
                        weight += 1;

                        if lead_column.is_none() {
                            lead_column = Some(col);
                        }
                    }
                }

                if weight != 0 {
                    let lead_column = lead_column.unwrap();

                    if match best {
                        None => true,
                        Some((_best_row, best_weight, _best_lead_column)) => weight < best_weight,
                    } {
                        best = Some((row, weight, lead_column));
                    }
                }
            }

            best.map(|(row, _weight, lead_column)| (row, lead_column))
        })
    }
}
