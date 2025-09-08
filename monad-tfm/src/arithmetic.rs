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

use std::ops::{Add, Div, Mul, Neg, Sub};

#[derive(Debug, Clone, Copy)]
pub struct CheckedU64(u64);

impl CheckedU64 {
    pub const fn from_u64(value: u64) -> Self {
        CheckedU64(value)
    }

    pub fn to_checked_i64(self) -> CheckedI64 {
        CheckedI64(
            self.0
                .try_into()
                .unwrap_or_else(|_| panic!("value {:?} out of range for i64", self.0)),
        )
    }

    pub fn to_u64(self) -> u64 {
        self.0
    }

    pub fn isqrt(self) -> CheckedU64 {
        CheckedU64(self.0.isqrt())
    }
}

impl Add for CheckedU64 {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        CheckedU64(
            self.0
                .checked_add(rhs.0)
                .unwrap_or_else(|| panic!("{:?} + {:?} overflowed", self.0, rhs.0)),
        )
    }
}

impl Sub for CheckedU64 {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        CheckedU64(
            self.0
                .checked_sub(rhs.0)
                .unwrap_or_else(|| panic!("{:?} - {:?} overflowed", self.0, rhs.0)),
        )
    }
}

impl Mul for CheckedU64 {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        CheckedU64(
            self.0
                .checked_mul(rhs.0)
                .unwrap_or_else(|| panic!("{:?} * {:?} overflowed", self.0, rhs.0)),
        )
    }
}

impl Mul<u64> for CheckedU64 {
    type Output = Self;

    fn mul(self, rhs: u64) -> Self::Output {
        CheckedU64(
            self.0
                .checked_mul(rhs)
                .unwrap_or_else(|| panic!("{:?} * {:?} overflowed", self.0, rhs)),
        )
    }
}

impl Div<u64> for CheckedU64 {
    type Output = Self;

    fn div(self, rhs: u64) -> Self::Output {
        CheckedU64(
            self.0
                .checked_div(rhs)
                .unwrap_or_else(|| panic!("{:?} / {:?} divided by zero", self.0, rhs)),
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CheckedI64(i64);

impl CheckedI64 {
    pub const fn from_i64(value: i64) -> Self {
        CheckedI64(value)
    }

    pub fn to_i64(self) -> i64 {
        self.0
    }

    pub fn saturating_to_checked_u64(self) -> CheckedU64 {
        CheckedU64(self.0.max(0).try_into().expect("no overflow"))
    }
}

impl Neg for CheckedI64 {
    type Output = Self;

    fn neg(self) -> Self::Output {
        CheckedI64(
            self.0
                .checked_neg()
                .unwrap_or_else(|| panic!("-{:?} overflowed", self.0)),
        )
    }
}

impl Add for CheckedI64 {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        CheckedI64(
            self.0
                .checked_add(rhs.0)
                .unwrap_or_else(|| panic!("{:?} + {:?} overflowed", self.0, rhs.0)),
        )
    }
}

impl Sub for CheckedI64 {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        CheckedI64(
            self.0
                .checked_sub(rhs.0)
                .unwrap_or_else(|| panic!("{:?} - {:?} overflowed", self.0, rhs.0)),
        )
    }
}

impl Mul for CheckedI64 {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        CheckedI64(
            self.0
                .checked_mul(rhs.0)
                .unwrap_or_else(|| panic!("{:?} * {:?} overflowed", self.0, rhs.0)),
        )
    }
}

impl Div for CheckedI64 {
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        CheckedI64(
            self.0
                .checked_div(rhs.0)
                .unwrap_or_else(|| panic!("{:?} / {:?} divided by zero", self.0, rhs.0)),
        )
    }
}
