use std::io::{Read, Write};
use std::net::TcpStream;
use build_kafka::config::network::NETWORK_ADDRESS;

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

fn send_request(stream: &mut TcpStream, request: &[u8]) -> std::io::Result<Vec<u8>> {
    stream.write_all(request)?;

    let mut size_buffer = [0u8; 4];
    let _ = stream.read_exact(&mut size_buffer);

    let response_size = i32::from_be_bytes(size_buffer) as usize;

    let mut response_buffer = vec![0u8; response_size];
    let _ = stream.read_exact(&mut response_buffer)?;

    Ok(response_buffer)
}

fn main() {
    let address = NETWORK_ADDRESS.address();
    let mut stream = TcpStream::connect(&address)
        .expect("failed to connect!");

    let request_1 = build_request(7);
    let response_1 = send_request(&mut stream, &request_1).expect("failed to send");

    println!("server replied: {:02X?}", &response_1);

    let request_2 = build_request(8);
    let response_2 = send_request(&mut stream, &request_2).expect("failed to send");

    println!("server replied: {:02X?}", &response_2);
}
