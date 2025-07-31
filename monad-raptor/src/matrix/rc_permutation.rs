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

#[derive(Clone, Debug, PartialEq)]
pub struct RCPermutation {
    virt_to_phys: Vec<u16>,
    phys_to_virt: Vec<u16>,
}

impl RCPermutation {
    pub fn new(len: usize) -> RCPermutation {
        let len: u16 = len.try_into().unwrap();
        let virt_to_phys: Vec<u16> = (0..len).collect();
        let phys_to_virt: Vec<u16> = (0..len).collect();

        RCPermutation {
            virt_to_phys,
            phys_to_virt,
        }
    }

    pub fn index(&self, a: usize) -> usize {
        self.virt_to_phys[a].into()
    }

    pub fn swap(&mut self, a: usize, b: usize) {
        self.virt_to_phys.swap(a, b);

        self.phys_to_virt.swap(
            usize::from(self.virt_to_phys[a]),
            usize::from(self.virt_to_phys[b]),
        );
    }
}

#[cfg(test)]
mod test {
    use rand::{thread_rng, RngCore};

    use super::RCPermutation;

    #[test]
    fn test_rc_permutation() {
        let num = 1000;

        let mut v: Vec<usize> = (0..num).collect();
        let mut perm = RCPermutation::new(num);

        let mut rng = thread_rng();

        for _ in 0..num * num {
            let a: usize = usize::try_from(rng.next_u32()).unwrap() % num;
            let b: usize = usize::try_from(rng.next_u32()).unwrap() % num;

            v.swap(a, b);
            perm.swap(a, b);
        }

        for (i, item) in v.iter().enumerate() {
            assert!(*item == usize::from(perm.virt_to_phys[i]));
        }

        let mut s = perm.virt_to_phys.clone();

        s.sort();

        for i in 0..u16::try_from(s.len()).unwrap() {
            assert!(s[usize::from(i)] == i);
            assert!(perm.phys_to_virt[usize::from(perm.virt_to_phys[usize::from(i)])] == i);
        }
    }
}
