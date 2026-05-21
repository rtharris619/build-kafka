pub struct NetworkConfig {
    pub host: &'static str,
    pub port: u16,
}

impl NetworkConfig {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

pub const NETWORK_ADDRESS: NetworkConfig = NetworkConfig {
    host: "127.0.0.1",
    port: 9092,
};
