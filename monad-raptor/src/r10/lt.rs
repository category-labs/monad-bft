use std::cmp::min;

use crate::r10::{deg, rand, CodeParameters, MAX_DEGREE};

pub const MAX_TRIPLES: usize = 65521;

impl CodeParameters {
    // Compute the Triple Generator function Trip() defined in RFC 5053 section 5.4.4.4.
    fn trip(&self, encoding_symbol_id: u16) -> (u8, u16, u16) {
        assert!(usize::from(encoding_symbol_id) < MAX_TRIPLES);

        let y: u16 = {
            // Q = 65521, the largest prime smaller than 2^^16
            let q = 65521;

            // A = (53591 + J(K)*997) % Q
            let a = (53591 + self.systematic_index() * 997) % q;

            // B = 10267*(J(K)+1) % Q
            let b = (10267 * (self.systematic_index() + 1)) % q;

            // Y = (B + X*A) % Q
            (b + usize::from(encoding_symbol_id) * a) % q
        }
        .try_into()
        .unwrap();

        let d: u8 = {
            // v = Rand[Y, 0, 2^^20]
            let v = rand(y, 0, 1 << 20);

            // d = Deg[v]
            deg(v)
        };

        {
            let num_intermediate_symbols_prime: u32 =
                self.num_intermediate_symbols_prime().try_into().unwrap();

            // a = 1 + Rand[Y, 1, L’-1].
            let a = 1 + rand(y, 1, num_intermediate_symbols_prime - 1);

            // b = Rand[Y, 2, L’].
            let b = rand(y, 2, num_intermediate_symbols_prime);

            // a will be in [1..8418].
            // b will be in [0..8418].
            (d, a.try_into().unwrap(), b.try_into().unwrap())
        }
    }

    // Compute the LT sequence according to RFC 5053 section 5.4.4.3.
    pub fn lt_sequence_op(&self, encoding_symbol_id: usize, mut set_element: impl FnMut(usize)) {
        let encoding_symbol_id: u16 = encoding_symbol_id.try_into().unwrap();

        let (d, a, b) = self.trip(encoding_symbol_id);

        let d: usize = d.into();
        let a: usize = a.into();
        let mut b: usize = b.into();

        let num_symbols = min(d, self.num_intermediate_symbols());

        let mut symbols: [u16; MAX_DEGREE] = [0; MAX_DEGREE];

        for symbol in symbols.iter_mut().take(num_symbols) {
            while b >= self.num_intermediate_symbols() {
                b = (b + a) % self.num_intermediate_symbols_prime();
            }

            *symbol = b.try_into().unwrap();

            b = (b + a) % self.num_intermediate_symbols_prime();
        }

        symbols[0..num_symbols].sort();

        for symbol in &symbols[0..num_symbols] {
            set_element((*symbol).into());
        }
    }

    // Implant the elements for G_LT according to RFC 5053 section 5.4.4.3.
    pub fn g_lt(&self, mut set_element: impl FnMut(usize, usize), nrows: usize) {
        for i in 0..nrows {
            self.lt_sequence_op(i, |el| set_element(i, el));
        }
    }
}
