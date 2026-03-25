#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Clause<T> {
    Any,
    One(T),
    Or(Vec<T>),
}

impl<T> Clause<T> {
    pub fn or_terms(&self) -> usize {
        match self {
            Self::Any => 0,
            Self::One(_) => 1,
            Self::Or(values) => values.len(),
        }
    }

    pub fn indexed_values(&self) -> Vec<Vec<u8>>
    where
        T: Copy + Into<Vec<u8>>,
    {
        match self {
            Self::Any => Vec::new(),
            Self::One(value) => vec![(*value).into()],
            Self::Or(values) => values.iter().copied().map(Into::into).collect(),
        }
    }
}

pub fn clause_matches<T: Eq>(actual: &T, clause: &Option<Clause<T>>) -> bool {
    match clause {
        None | Some(Clause::Any) => true,
        Some(Clause::One(value)) => value == actual,
        Some(Clause::Or(values)) => values.iter().any(|value| value == actual),
    }
}

pub fn optional_clause_matches<T: Eq>(actual: Option<T>, clause: &Option<Clause<T>>) -> bool {
    match clause {
        None | Some(Clause::Any) => true,
        Some(Clause::One(value)) => actual.as_ref() == Some(value),
        Some(Clause::Or(values)) => actual
            .as_ref()
            .map(|actual| values.iter().any(|value| value == actual))
            .unwrap_or(false),
    }
}

pub fn has_indexed_value<T>(clause: &Option<Clause<T>>) -> bool {
    matches!(clause, Some(Clause::One(_) | Clause::Or(_)))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRef {
    pub number: u64,
    pub hash: [u8; 32],
    pub parent_hash: [u8; 32],
}

impl BlockRef {
    pub const fn zero(number: u64) -> Self {
        Self {
            number,
            hash: [0; 32],
            parent_hash: [0; 32],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOrder {
    Ascending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPageMeta {
    pub resolved_from_block: BlockRef,
    pub resolved_to_block: BlockRef,
    pub cursor_block: BlockRef,
    pub has_more: bool,
    pub next_resume_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPage<T> {
    pub items: Vec<T>,
    pub meta: QueryPageMeta,
}
