/// Checksum utilities shared across snapshot, WAL, and LSM manifest.
///
/// Two algorithms:
/// - `SimpleChecksum`: legacy (v6 snapshot, WAL v1/v2, LSM manifest v1-v3). Weak but fast.
/// - `Crc32c`: CRC32C (Castagnoli). Standard, stronger collision resistance.
///
/// New writes always use CRC32C; readers fall back to SimpleChecksum for old data.

/// Dual-mode checksum: selects algorithm at construction time.
pub enum Checksum32 {
    Simple(SimpleChecksum),
    Crc32c(Crc32c),
}

impl Checksum32 {
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Checksum32::Simple(s) => s.update(data),
            Checksum32::Crc32c(c) => c.update(data),
        }
    }

    pub fn finalize(self) -> u32 {
        match self {
            Checksum32::Simple(s) => s.finalize(),
            Checksum32::Crc32c(c) => c.finalize(),
        }
    }
}

/// Legacy checksum: wrapping-add + rotate. Not cryptographic; sufficient for truncation detection.
pub struct SimpleChecksum {
    hash: u32,
    pending: [u8; 4],
    pending_len: usize,
}

impl SimpleChecksum {
    pub fn new() -> Self {
        Self {
            hash: 0,
            pending: [0u8; 4],
            pending_len: 0,
        }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        if self.pending_len > 0 {
            let need = 4 - self.pending_len;
            let take = need.min(data.len());
            self.pending[self.pending_len..self.pending_len + take].copy_from_slice(&data[..take]);
            self.pending_len += take;
            data = &data[take..];

            if self.pending_len == 4 {
                self.process_chunk(self.pending);
                self.pending_len = 0;
                self.pending = [0u8; 4];
            }
        }

        while data.len() >= 4 {
            let chunk: [u8; 4] = data[..4].try_into().expect("slice len checked");
            self.process_chunk(chunk);
            data = &data[4..];
        }

        if !data.is_empty() {
            self.pending[..data.len()].copy_from_slice(data);
            self.pending_len = data.len();
        }
    }

    pub fn finalize(mut self) -> u32 {
        if self.pending_len > 0 {
            let mut buf = [0u8; 4];
            buf[..self.pending_len].copy_from_slice(&self.pending[..self.pending_len]);
            self.process_chunk(buf);
        }
        self.hash
    }

    fn process_chunk(&mut self, chunk: [u8; 4]) {
        self.hash = self.hash.wrapping_add(u32::from_le_bytes(chunk));
        self.hash = self.hash.rotate_left(7);
    }
}

/// CRC32C (Castagnoli) checksum: u32 output, streaming update.
///
/// - Initial value 0xFFFF_FFFF, finalize inverts (standard CRC32C convention).
/// - Uses reflected polynomial 0x82F63B78.
pub struct Crc32c {
    state: u32,
}

impl Crc32c {
    pub fn new() -> Self {
        Self { state: 0xFFFF_FFFF }
    }

    pub fn update(&mut self, data: &[u8]) {
        for &b in data {
            let idx = (self.state as u8) ^ b;
            self.state = (self.state >> 8) ^ CRC32C_TABLE[idx as usize];
        }
    }

    pub fn finalize(self) -> u32 {
        !self.state
    }
}

/// CRC32C lookup table (reflected poly = 0x82F63B78).
pub const CRC32C_TABLE: [u32; 256] = {
    const POLY: u32 = 0x82F6_3B78;
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (POLY & mask);
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Convenience: compute SimpleChecksum over a byte slice in one shot.
pub fn simple_checksum(data: &[u8]) -> u32 {
    let mut c = SimpleChecksum::new();
    c.update(data);
    c.finalize()
}

/// Convenience: compute CRC32C over a byte slice in one shot.
pub fn crc32c_checksum(data: &[u8]) -> u32 {
    let mut c = Crc32c::new();
    c.update(data);
    c.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_checksum_deterministic() {
        let data = b"hello world";
        let a = simple_checksum(data);
        let b = simple_checksum(data);
        assert_eq!(a, b);
        assert_ne!(a, 0);
    }

    #[test]
    fn simple_checksum_streaming_matches_oneshot() {
        let data = b"hello world, this is a longer test string for streaming";
        let oneshot = simple_checksum(data);

        let mut s = SimpleChecksum::new();
        s.update(&data[..5]);
        s.update(&data[5..13]);
        s.update(&data[13..]);
        let streamed = s.finalize();
        assert_eq!(oneshot, streamed);
    }

    #[test]
    fn crc32c_deterministic() {
        let data = b"hello world";
        let a = crc32c_checksum(data);
        let b = crc32c_checksum(data);
        assert_eq!(a, b);
        assert_ne!(a, 0);
    }

    #[test]
    fn crc32c_streaming_matches_oneshot() {
        let data = b"hello world, this is a longer test string for streaming";
        let oneshot = crc32c_checksum(data);

        let mut c = Crc32c::new();
        c.update(&data[..5]);
        c.update(&data[5..13]);
        c.update(&data[13..]);
        let streamed = c.finalize();
        assert_eq!(oneshot, streamed);
    }

    #[test]
    fn simple_and_crc32c_differ() {
        let data = b"test data for comparison";
        let s = simple_checksum(data);
        let c = crc32c_checksum(data);
        assert_ne!(s, c);
    }

    #[test]
    fn checksum32_enum_dispatches_correctly() {
        let data = b"enum dispatch test";

        let mut cs = Checksum32::Simple(SimpleChecksum::new());
        cs.update(data);
        let vs = cs.finalize();
        assert_eq!(vs, simple_checksum(data));

        let mut cc = Checksum32::Crc32c(Crc32c::new());
        cc.update(data);
        let vc = cc.finalize();
        assert_eq!(vc, crc32c_checksum(data));
    }
}
