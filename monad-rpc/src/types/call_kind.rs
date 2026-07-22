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

use monad_query_primitives::CallKind as EngineCallKind;
use serde::Serialize;

#[derive(
    Serialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    schemars::JsonSchema,
    strum::AsRefStr,
    strum::VariantArray,
)]
#[serde(rename_all = "UPPERCASE")]
#[strum(serialize_all = "UPPERCASE")]
pub enum CallKind {
    Call,
    DelegateCall,
    CallCode,
    Create,
    Create2,
    SelfDestruct,
    StaticCall,
}

impl From<EngineCallKind> for CallKind {
    fn from(kind: EngineCallKind) -> Self {
        match kind {
            EngineCallKind::Call => Self::Call,
            EngineCallKind::StaticCall => Self::StaticCall,
            EngineCallKind::DelegateCall => Self::DelegateCall,
            EngineCallKind::CallCode => Self::CallCode,
            EngineCallKind::Create => Self::Create,
            EngineCallKind::Create2 => Self::Create2,
            EngineCallKind::SelfDestruct => Self::SelfDestruct,
        }
    }
}
