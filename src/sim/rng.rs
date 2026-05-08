#[derive(Debug, Clone)]
pub struct Rng64 {
    state: u64,
}

impl Rng64 {
    pub fn new(seed: u64) -> Self {
        let state = if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        };
        Self { state }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    pub fn next_f64(&mut self) -> f64 {
        let raw = self.next_u64() >> 11;
        (raw as f64) * (1.0 / ((1u64 << 53) as f64))
    }

    pub fn usize_range(&mut self, start: usize, end: usize) -> usize {
        if end <= start {
            return start;
        }
        let span = end - start;
        start + (self.next_u64() as usize % span)
    }

    pub fn u64_range(&mut self, start: u64, end: u64) -> u64 {
        if end <= start {
            return start;
        }
        let span = end - start;
        start + (self.next_u64() % span)
    }

    pub fn bool(&mut self, probability: f64) -> bool {
        self.next_f64() < probability.clamp(0.0, 1.0)
    }
}
