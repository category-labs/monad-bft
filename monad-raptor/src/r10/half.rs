use crate::r10::CodeParameters;

impl CodeParameters {
    // Implant the elements for G_Half according to RFC 5053 section 5.4.2.3.
    pub fn g_half(&self, mut set_element: impl FnMut(usize, usize)) {
        let h_prime: u32 = ((self.num_half_symbols() + 1) >> 1).try_into().unwrap();

        // Generate the sequence m[H'] according to RFC 5053 section 5.4.2.3.
        let mut i: u64 = 0;
        let mut m_next = || loop {
            let g_i = i ^ (i >> 1);
            i += 1;

            if g_i.count_ones() == h_prime {
                return g_i;
            }
        };

        for j in 0..self.num_source_symbols() + self.num_ldpc_symbols() {
            // Compute m[j, H'].
            let m = m_next();

            for h in 0..self.num_half_symbols() {
                if (m & (1 << h)) != 0 {
                    set_element(h, j);
                }
            }
        }
    }
}
