#![allow(unused_imports)]
use std::{io::{Read, Write}, net::{TcpListener, TcpStream}};
use build_kafka::config::network::NETWORK_ADDRESS;
use build_kafka::errors::error_codes;

const VALID_API_VERSIONS: &[i16] = &[0, 1, 2, 3, 4];

fn send_response(stream: &mut TcpStream, header: Vec<u8>, body: Vec<u8>) {

     let message_size: i32 = header.len() as i32 + body.len() as i32;

    let mut buffer = Vec::<u8>::new();
    buffer.extend_from_slice(&message_size.to_be_bytes());
    buffer.extend_from_slice(&header);
    buffer.extend_from_slice(&body);

    match stream.write_all(&buffer) {
        Ok(_) => {
            println!("response sent");
        }
        Err(e) => {
            println!("write error: {}", e);
        }
    }
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
                
                let request_api_key = i16::from_be_bytes(buffer[4..6].try_into().unwrap());
                let request_api_version = i16::from_be_bytes(buffer[6..8].try_into().unwrap());
                let correlation_id = i32::from_be_bytes(buffer[8..12].try_into().unwrap());

                let mut error_code = 0;

                if !VALID_API_VERSIONS.contains(&request_api_version) {
                    error_code = error_codes::ErrorCode::UnsupportedVersion.as_i16();
                }

                let mut body = Vec::new();
                body.extend_from_slice(&error_code.to_be_bytes());

                let api_keys: &[i16] = &[request_api_key, VALID_API_VERSIONS[0], VALID_API_VERSIONS[VALID_API_VERSIONS.len() - 1]];

                let number_api_keys:i8 = 1;
                body.extend_from_slice(&number_api_keys.to_be_bytes());

                for key in api_keys {
                    body.extend_from_slice(&key.to_be_bytes());
                }

                let tag_buffer: i8 = 0;
                body.extend_from_slice(&tag_buffer.to_be_bytes());

                let throttle_time_ms: i32 = 0;
                body.extend_from_slice(&throttle_time_ms.to_be_bytes());

                body.extend_from_slice(&tag_buffer.to_be_bytes());

                let correlation_id_in_bytes = correlation_id.to_be_bytes();

                let mut header = Vec::new();
                header.extend_from_slice(&correlation_id_in_bytes);

                send_response(&mut stream, header, body);
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
