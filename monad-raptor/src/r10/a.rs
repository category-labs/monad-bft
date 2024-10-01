use crate::r10::CodeParameters;

impl CodeParameters {
    // Implant the common part of the matrix A according to RFC 5053 section 5.4.2.4.2:
    //
    //               K               S       H
    //   +-----------------------+-------+-------+
    //   |                       |       |       |
    // S |        G_LDPC         |  I_S  | 0_SxH |
    //   |                       |       |       |
    //   +-----------------------+-------+-------+
    //   |                               |       |
    // H |        G_Half                 |  I_H  |
    //   |                               |       |
    //   +-------------------------------+-------+
    #[allow(clippy::redundant_closure)]
    pub fn a_common(&self, mut set_element: impl FnMut(usize, usize)) {
        // G_LDPC
        self.g_ldpc(|i, j| set_element(i, j));

        // I_S
        for i in 0..self.num_ldpc_symbols() {
            set_element(i, self.num_source_symbols() + i);
        }

        // G_Half
        self.g_half(|i, j| set_element(self.num_ldpc_symbols() + i, j));

        // I_H
        for i in 0..self.num_half_symbols() {
            set_element(
                self.num_ldpc_symbols() + i,
                self.num_source_symbols() + self.num_ldpc_symbols() + i,
            );
        }
    }

    // Generate an A matrix with a bottom G_LT part consisting of 'num_encoded_symbols' rows.
    fn a_with_g_lt(&self, num_encoded_symbols: usize, mut set_element: impl FnMut(usize, usize)) {
        self.a_common(&mut set_element);

        // G_LT
        self.g_lt(
            |i, j| set_element(self.num_ldpc_symbols() + self.num_half_symbols() + i, j),
            num_encoded_symbols,
        );
    }

    // Generate the A matrix which when inverted produces the intermediate symbols from
    // the constraint symbols and the source symbols for systematic R10 codes.
    pub fn a_systematic_intermediate(&self, set_element: impl FnMut(usize, usize)) {
        self.a_with_g_lt(self.num_source_symbols(), set_element);
    }
}
