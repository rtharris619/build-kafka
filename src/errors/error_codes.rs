pub enum ErrorCode {
    UnsupportedVersion = 35,
}

impl ErrorCode {
    pub fn as_i16(self) -> i16 {
        self as i16
    }
}