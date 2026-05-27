#![allow(unused_imports)]
use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};
use build_kafka::config::network::NETWORK_ADDRESS;
use build_kafka::errors::error_codes;
use build_kafka::api::api_versions;

fn send_response(stream: &mut TcpStream, response: Vec<u8>) {

    match stream.write_all(&response) {
        Ok(_) => {
            println!("response sent");
        }
        Err(e) => {
            println!("write error: {}", e);
        }
    }
}

fn build_response(buffer: [u8; 1024]) -> Vec<u8> {

    let mut response: Vec<u8> = Vec::new();

    let _request_api_key = i16::from_be_bytes(buffer[4..6].try_into().unwrap());
    let request_api_version = i16::from_be_bytes(buffer[6..8].try_into().unwrap());
    let correlation_id = i32::from_be_bytes(buffer[8..12].try_into().unwrap());

    let mut error_code = 0;

    if !api_versions::VALID_API_VERSIONS.contains(&request_api_version) {
        error_code = error_codes::ErrorCode::UnsupportedVersion.as_i16();
    }

    let mut body = Vec::new();
    body.extend_from_slice(&error_code.to_be_bytes());

    let tag_buffer: i8 = 0;

    let api_versions_length_compacted: i8 = (api_versions::API_VERSIONS.len() + 1) as i8;
    body.extend_from_slice(&api_versions_length_compacted.to_be_bytes());

    for api_version in api_versions::API_VERSIONS {
        body.extend_from_slice(&api_version.api_key.to_be_bytes());
        body.extend_from_slice(&api_version.min_api_version.to_be_bytes());
        body.extend_from_slice(&api_version.max_api_version.to_be_bytes());
        body.extend_from_slice(&tag_buffer.to_be_bytes());
    }

    let throttle_time_ms: i32 = 0;

    body.extend_from_slice(&throttle_time_ms.to_be_bytes());

    body.extend_from_slice(&tag_buffer.to_be_bytes());

    let correlation_id_in_bytes = correlation_id.to_be_bytes();

    let mut header = Vec::new();
    header.extend_from_slice(&correlation_id_in_bytes);

    let message_size: i32 = header.len() as i32 + body.len() as i32;

    response.extend_from_slice(&message_size.to_be_bytes());
    response.extend_from_slice(&header);
    response.extend_from_slice(&body);

    response
}

fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0u8; 1024];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                println!("client disconnected");
                break;
            }

            // received data
            Ok(size) => {
                println!("stream: {:02X?}", &buffer[..size]);
                println!("size: {}", size);

                let response = build_response(buffer);

                send_response(&mut stream, response);
            }

            Err(err) => {
                eprintln!("connection error: {}", err);
                break;
            }
        }
    }
}

fn main() {
    let address = NETWORK_ADDRESS.address();
    let listener = TcpListener::bind(&address)
        .expect("failed to bind");

    println!("server listening on {}", address);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("client connected");
                handle_connection(stream)
            },
            Err(err) => eprintln!("connection failed: {}", err),
        }
    }
}
