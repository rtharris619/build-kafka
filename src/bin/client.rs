use std::io::{Read, Write};
use std::net::TcpStream;
use build_kafka::config::network::NETWORK_ADDRESS;
// use build_kafka::utils::conversions;

fn build_request(correlation_id: i32) -> Vec<u8> {
    let mut request = Vec::new();

    let mut header = Vec::new();
    let api_key: i16 = 18;
    let api_version: i16 = 4;
    
    let client_id = String::from("kafka-cli");
    let client_id_length = client_id.len() as i16;
    let client_id_length_compacted = (client_id_length + 1) as i8;
    
    let client_software_version = String::from("0.1");
    let client_software_version_length = client_software_version.len() as i8;

    let tag_buffer: i8 = 0;

    header.extend_from_slice(&api_key.to_be_bytes());
    header.extend_from_slice(&api_version.to_be_bytes());
    header.extend_from_slice(&correlation_id.to_be_bytes());
    header.extend_from_slice(&client_id_length.to_be_bytes());
    header.extend_from_slice(&client_id.as_bytes());
    header.extend_from_slice(&tag_buffer.to_be_bytes());

    let mut body = Vec::new();
    body.extend_from_slice(&client_id_length_compacted.to_be_bytes());
    body.extend_from_slice(&client_id.as_bytes());
    body.extend_from_slice(&client_software_version_length.to_be_bytes());
    body.extend_from_slice(&client_software_version.as_bytes());
    body.extend_from_slice(&tag_buffer.to_be_bytes());

    let message_size = header.len() as i32 + body.len() as i32;
    request.extend_from_slice(&message_size.to_be_bytes());
    request.extend_from_slice(&header);
    request.extend_from_slice(&body);

    request
}

fn main() {
    let address = NETWORK_ADDRESS.address();
    let mut stream = TcpStream::connect(&address)
        .expect("failed to connect!");

    // let hex = "0000001a0012000467890abc00096b61666b612d636c69000a6b61666b612d636c6904302e3100";
    // let bytes = conversions::hex_to_bytes(hex);
    let bytes = build_request(7);

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
