pub mod core;
pub mod error;

pub use core::{
    clause::Clause,
    header::EvmBlockHeader,
    page::{QueryOrder, QueryPage, QueryPageMeta},
    refs::BlockRef,
};

pub use error::{Error, Result};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposes_core_vocabulary_from_the_crate_root() {
        let header = EvmBlockHeader::minimal(7, [1; 32], [2; 32]);
        let page = QueryPage::<()> {
            items: Vec::new(),
            meta: QueryPageMeta {
                resolved_from_block: BlockRef::zero(7),
                resolved_to_block: BlockRef::zero(7),
                cursor_block: BlockRef::zero(7),
                has_more: false,
                next_resume_id: None,
            },
        };

        let clause = Clause::One([9u8; 32]);

        assert_eq!(header.number, 7);
        assert_eq!(page.meta.cursor_block.number, 7);
        assert_eq!(clause.or_terms(), 1);
    }
}
