#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct PageId(u32);

impl PageId {
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

impl From<u32> for PageId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<usize> for PageId {
    fn from(id: usize) -> Self {
        Self(id as u32)
    }
}

impl std::fmt::Display for PageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TypeId {
    Int,
    BigInt,
}

impl TypeId {
    pub fn size(&self) -> usize {
        match self {
            TypeId::Int => 4,
            TypeId::BigInt => 8,
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

    pub fn deserialize(type_id: TypeId, buf: &[u8]) -> Self {
        match type_id {
            TypeId::Int => Value::Int(i32::from_le_bytes(buf[..4].try_into().unwrap())),
            TypeId::BigInt => Value::BigInt(i64::from_le_bytes(buf[..8].try_into().unwrap())),
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

pub enum Key {
    Int(i32),
    BigInt(i64),
}

impl Key {
    pub fn type_id(&self) -> TypeId {
        match self {
            Key::Int(_) => TypeId::Int,
            Key::BigInt(_) => TypeId::BigInt,
        }
    }

    pub fn size(&self) -> usize {
        self.type_id().size()
    }

    pub fn from_bytes(type_id: TypeId, bytes: &[u8]) -> Self {
        match type_id {
            TypeId::Int => {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes[..4]);
                Key::Int(i32::from_be_bytes(arr) ^ (1 << 31))
            }
            TypeId::BigInt => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                Key::BigInt(i64::from_be_bytes(arr) ^ (1 << 63))
            }
        }
    }

    // Order-Preserving Encoding
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Key::Int(v) => {
                // XOR with the sign bit mask to flip it
                let flipped = *v ^ (1 << 31);
                // Convert to big-endian bytes for lexicographical ordering
                flipped.to_be_bytes().to_vec()
            }
            Key::BigInt(v) => {
                let flipped = *v ^ (1 << 63);
                flipped.to_be_bytes().to_vec()
            }
        }
    }
}
