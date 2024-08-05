use crate::r10::nonsystematic::decoder::Decoder;

#[derive(Debug)]
pub enum BufferId {
    // One of the temporary buffers allocated by the caller for pre-code symbol recovery.
    TempBuffer { index: usize },

    // A buffer in which an encoded symbol was originally received from the encoder.
    ReceiveBuffer { index: usize },
}

impl Decoder {
    pub fn buffer_index_to_buffer_id(&self, index: u16) -> BufferId {
        let index = usize::from(index);

        let num_redundant_intermediate_symbols = self.num_redundant_intermediate_symbols();

        if index < num_redundant_intermediate_symbols {
            BufferId::TempBuffer { index }
        } else {
            BufferId::ReceiveBuffer {
                index: index - num_redundant_intermediate_symbols,
            }
        }
    }
}
