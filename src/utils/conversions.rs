
pub fn hex_to_bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks(2)
        .map(|chunk| {
            let hex_str = std::str::from_utf8(chunk).unwrap();
            u8::from_str_radix(hex_str, 16).unwrap()
        })
        .collect()
}
