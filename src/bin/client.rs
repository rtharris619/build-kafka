use std::io::{Read, Write};
use std::net::TcpStream;
use build_kafka::config::network::NETWORK_ADDRESS;
use build_kafka::utils::conversions;

fn main() {
    let address = NETWORK_ADDRESS.address();
    let mut stream = TcpStream::connect(&address)
        .expect("failed to connect!");

    let hex = "0000001a0012000467890abc00096b61666b612d636c69000a6b61666b612d636c6904302e3100";
    let bytes = conversions::hex_to_bytes(hex);

    stream
        .write_all(&bytes)
        .expect("failed to write.");

    let mut buffer = [0; 1024];
    
    let size = stream
        .read(&mut buffer)
        .expect("failed to read.");

    println!("server replied: {:02X?}", &buffer[..size]);
    println!("size: {}", size);
}
