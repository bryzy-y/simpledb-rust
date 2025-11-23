#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageID(u32);

impl PageID {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    pub const fn as_u32(self) -> u32 {
        self.0
    }

    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    pub const fn as_u64(self) -> u64 {
        self.0 as u64
    }

    pub const fn from_le_bytes(bytes: [u8; 4]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }

    pub const fn to_le_bytes(self) -> [u8; 4] {
        self.0.to_le_bytes()
    }
}

impl From<u32> for PageID {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<usize> for PageID {
    fn from(id: usize) -> Self {
        Self(id as u32)
    }
}

impl std::fmt::Display for PageID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TypeID {
    Int,
    BigInt,
}

impl TypeID {
    pub fn size(&self) -> usize {
        match self {
            TypeID::Int => 4,
            TypeID::BigInt => 8,
        }
    }
}

#[derive(Debug)]
pub enum Value {
    Int(i32),
    BigInt(i64),
}

impl Value {
    pub fn serialize(&self, buf: &mut [u8]) {
        match self {
            Value::Int(v) => buf[..4].copy_from_slice(&v.to_le_bytes()),
            Value::BigInt(v) => buf[..8].copy_from_slice(&v.to_le_bytes()),
        }
    }

    pub fn deserialize(type_id: TypeID, buf: &[u8]) -> Self {
        match type_id {
            TypeID::Int => Value::Int(i32::from_le_bytes(buf[..4].try_into().unwrap())),
            TypeID::BigInt => Value::BigInt(i64::from_le_bytes(buf[..8].try_into().unwrap())),
        }
    }

    pub fn compare(&self, other: &Value) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.cmp(b),
            _ => panic!("Cannot compare different Value types"),
        }
    }
}
