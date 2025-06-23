use std::{ops::Deref, sync::Arc};

use serde::Serialize;
use serde_json::value::RawValue;

pub type SharedJsonSerialized<T> = Arc<JsonSerialized<T>>;

pub struct JsonSerialized<T>
where
    T: Serialize,
{
    value: T,
    serialized: Box<RawValue>,
}

impl<T> JsonSerialized<T>
where
    T: Serialize,
{
    pub fn new(value: T) -> Self {
        let serialized = serde_json::value::to_raw_value(&value).unwrap();

        Self { value, serialized }
    }

    pub fn new_shared(value: T) -> SharedJsonSerialized<T> {
        Arc::new(Self::new(value))
    }
}

impl<T> AsRef<RawValue> for JsonSerialized<T>
where
    T: Serialize,
{
    fn as_ref(&self) -> &RawValue {
        &self.serialized
    }
}

impl<T> Deref for JsonSerialized<T>
where
    T: Serialize,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> std::fmt::Debug for JsonSerialized<T>
where
    T: std::fmt::Debug + Serialize,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonSerializedInner")
            .field("value", &self.value)
            .finish()
    }
}

impl<T> Serialize for JsonSerialized<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.serialized.serialize(serializer)
    }
}
