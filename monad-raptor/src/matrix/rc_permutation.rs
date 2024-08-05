use std::mem::swap;

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

    pub fn invert(&mut self) {
        swap(&mut self.virt_to_phys, &mut self.phys_to_virt);
    }

    pub fn apply(mut self, mut swap_fn: impl FnMut(usize, usize)) {
        let len: u16 = self.virt_to_phys.len().try_into().unwrap();

        for i in 0..len {
            let i_val = self.phys_to_virt[usize::from(i)];

            if i != i_val {
                let j = self.virt_to_phys[usize::from(i)];

                self.phys_to_virt.swap(usize::from(i), usize::from(j));

                self.virt_to_phys[usize::from(i)] = i;
                self.virt_to_phys[usize::from(i_val)] = j;

                swap_fn(usize::from(i), usize::from(j));
            }
        }
    }

    pub fn undo(mut self, swap_fn: impl FnMut(usize, usize)) {
        self.invert();
        self.apply(swap_fn);
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

    #[test]
    fn test_apply_undo() {
        let num = 32;

        let mut perm = RCPermutation::new(num);

        let mut rng = thread_rng();

        for _ in 0..num * num {
            let a: usize = usize::try_from(rng.next_u32()).unwrap() % num;
            let b: usize = usize::try_from(rng.next_u32()).unwrap() % num;

            perm.swap(a, b);
        }

        let mut perm2 = RCPermutation::new(num);

        perm.clone().apply(|i, j| perm2.swap(i, j));

        assert_eq!(perm, perm2);

        perm.clone().undo(|i, j| perm2.swap(i, j));

        assert_eq!(perm2, RCPermutation::new(num));
    }
}
