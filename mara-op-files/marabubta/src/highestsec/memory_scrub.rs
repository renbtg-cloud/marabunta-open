// Marabunta - Licensed under the MIT License.
/// Memory scrubbing for security_domain-in-depth after sandbox execution.
///
/// Uses volatile writes to prevent compiler from optimizing away zeroing.
/// Combined with a compiler fence to prevent reordering.

/// Memory scrubber for sensitive data.
pub struct MemoryScrubber;

impl MemoryScrubber {
    /// Scrub a byte slice with volatile writes.
    pub fn scrub(data: &mut [u8]) {
        for byte in data.iter_mut() {
            unsafe {
                std::ptr::write_volatile(byte, 0);
            }
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
    }

    /// Scrub a Vec and drop it.
    pub fn scrub_vec(mut data: Vec<u8>) {
        Self::scrub(&mut data);
        drop(data);
    }

    /// Full post-execution scrub cycle. Returns total bytes scrubbed.
    pub fn post_execution_scrub(
        input: Vec<u8>,
        params: Vec<u8>,
        output: Vec<u8>,
        sandbox_state: Vec<u8>,
    ) -> u64 {
        let total = (input.len() + params.len() + output.len() + sandbox_state.len()) as u64;
        Self::scrub_vec(input);
        Self::scrub_vec(params);
        Self::scrub_vec(output);
        Self::scrub_vec(sandbox_state);
        total
    }

    /// Scrub with a specific pattern before zeroing (multi-pass).
    /// Pass 1: 0xFF, Pass 2: 0x00, Pass 3: random, Pass 4: 0x00
    pub fn scrub_sec_wipe(data: &mut [u8]) {
        // Pass 1: all 1s
        for byte in data.iter_mut() {
            unsafe { std::ptr::write_volatile(byte, 0xFF); }
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

        // Pass 2: all 0s
        for byte in data.iter_mut() {
            unsafe { std::ptr::write_volatile(byte, 0x00); }
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

        // Pass 3: random
        use rand::RngCore;
        let mut rng = rand::thread_rng();
        let mut random_buf = vec![0u8; data.len()];
        rng.fill_bytes(&mut random_buf);
        for (byte, &r) in data.iter_mut().zip(random_buf.iter()) {
            unsafe { std::ptr::write_volatile(byte, r); }
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);

        // Pass 4: final zero
        for byte in data.iter_mut() {
            unsafe { std::ptr::write_volatile(byte, 0x00); }
        }
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
    }
}

/// Output padding to prevent size-based information leakage.
pub struct OutputPadder;

impl OutputPadder {
    /// Pad output to nearest power-of-2 boundary.
    /// Format: [4-byte LE actual length][actual data][random padding]
    /// Total size is next power of 2 >= actual_len + 4.
    pub fn pad(output: &[u8], max_bytes: u64) -> Vec<u8> {
        let actual_len = output.len();
        let min_size = actual_len + 4;
        let padded_len = min_size.next_power_of_two().min(max_bytes as usize);

        let mut padded = Vec::with_capacity(padded_len);
        padded.extend_from_slice(&(actual_len as u32).to_le_bytes());
        padded.extend_from_slice(output);

        let padding_needed = padded_len.saturating_sub(padded.len());
        if padding_needed > 0 {
            let mut padding = vec![0u8; padding_needed];
            use rand::RngCore;
            rand::thread_rng().fill_bytes(&mut padding);
            padded.extend_from_slice(&padding);
        }

        padded
    }

    /// Unpad: extract actual data from padded output.
    pub fn unpad(padded: &[u8]) -> Option<Vec<u8>> {
        if padded.len() < 4 {
            return None;
        }
        let actual_len = u32::from_le_bytes([padded[0], padded[1], padded[2], padded[3]]) as usize;
        if 4 + actual_len > padded.len() {
            return None;
        }
        Some(padded[4..4 + actual_len].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scrub_zeros_data() {
        let mut data = vec![0xAB; 256];
        MemoryScrubber::scrub(&mut data);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_scrub_empty() {
        let mut data = vec![];
        MemoryScrubber::scrub(&mut data);
        // no panic
    }

    #[test]
    fn test_scrub_vec() {
        let data = vec![0xCD; 128];
        MemoryScrubber::scrub_vec(data);
        // Vec is dropped after scrub
    }

    #[test]
    fn test_post_execution_scrub_returns_count() {
        let input = vec![1; 100];
        let params = vec![2; 50];
        let output = vec![3; 200];
        let state = vec![4; 150];
        let total = MemoryScrubber::post_execution_scrub(input, params, output, state);
        assert_eq!(total, 500);
    }

    #[test]
    fn test_scrub_sec_wipe_zeros_final() {
        let mut data = vec![0xAB; 64];
        MemoryScrubber::scrub_sec_wipe(&mut data);
        assert!(data.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_pad_output_power_of_two() {
        let output = vec![0xAA; 100];
        let padded = OutputPadder::pad(&output, 1024);
        // 100 + 4 = 104, next power of 2 = 128
        assert_eq!(padded.len(), 128);
    }

    #[test]
    fn test_pad_output_length_prefix() {
        let output = vec![0xBB; 50];
        let padded = OutputPadder::pad(&output, 1024);
        let len = u32::from_le_bytes([padded[0], padded[1], padded[2], padded[3]]);
        assert_eq!(len, 50);
    }

    #[test]
    fn test_pad_output_max_bytes() {
        let output = vec![0xCC; 200];
        let padded = OutputPadder::pad(&output, 256);
        assert_eq!(padded.len(), 256);
    }

    #[test]
    fn test_pad_output_empty() {
        let output = vec![];
        let padded = OutputPadder::pad(&output, 1024);
        // 0 + 4 = 4, next power of 2 = 4
        assert_eq!(padded.len(), 4);
        let len = u32::from_le_bytes([padded[0], padded[1], padded[2], padded[3]]);
        assert_eq!(len, 0);
    }

    #[test]
    fn test_pad_unpad_roundtrip() {
        let original = b"hello world, this is secret data!";
        let padded = OutputPadder::pad(original, 1024);
        let recovered = OutputPadder::unpad(&padded).unwrap();
        assert_eq!(recovered, original);
    }

    #[test]
    fn test_unpad_invalid() {
        assert!(OutputPadder::unpad(&[]).is_none());
        assert!(OutputPadder::unpad(&[0, 0, 0]).is_none());
        // Length says 100 but only 10 bytes total
        assert!(OutputPadder::unpad(&[100, 0, 0, 0, 1, 2, 3, 4, 5, 6]).is_none());
    }

    #[test]
    fn test_pad_output_exact_power_of_two() {
        // 12 bytes + 4 = 16, which IS a power of 2
        let output = vec![0xDD; 12];
        let padded = OutputPadder::pad(&output, 1024);
        assert_eq!(padded.len(), 16);
    }
}
