#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    /// Hash of the previous block.
    prev_hash: Option<u64>,

    /// Content of the block.
    data: Vec<u8>,

    /// Digital signature of the block.
    /// 
    /// ```text
    /// sign(prev_hash + data)
    /// ```
    sign: Vec<u8>
}

impl Block {
    #[inline]
    pub fn prev_hash(&self) -> Option<u64> {
        self.prev_hash
    }

    #[inline]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[inline]
    pub fn sign(&self) -> &[u8] {
        &self.sign
    }

    /// Get hash of the current block.
    /// 
    /// ```text
    /// hash(prev_hash + data)
    /// ```
    pub fn hash(&self) -> u64 {
        let mut buf = Vec::with_capacity(self.data.len() + 9);

        // Append previous block's hash
        match self.prev_hash {
            Some(hash) => {
                buf.push(1);
                buf.extend_from_slice(&hash.to_be_bytes());
            }

            None => {
                buf.push(0);
            }
        }

        // Append block's data
        buf.extend_from_slice(&self.data);

        // TODO: perhaps hashing sign instead of data is good enough
        seahash::hash(&buf)
    }
}
