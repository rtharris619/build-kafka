# build-kafka

A Kafka broker implementation built from scratch in Rust. This project implements the Kafka wire protocol over raw TCP, handling client connections and API version negotiation.

## Overview

The server listens on `127.0.0.1:9092` (Kafka's default port) and responds to incoming requests following the Kafka protocol format. The client sends a raw hex-encoded request and prints the server's response.

## Project Structure

```
src/
├── bin/
│   ├── server.rs       # TCP server handling Kafka protocol requests
│   └── client.rs       # Test client that sends a sample request
├── config/
│   └── network.rs      # Network configuration (host/port)
├── errors/
│   └── error_codes.rs  # Kafka error codes
└── utils/
    └── conversions.rs  # Hex/byte utilities
```

## Requirements

- Rust 1.95+

## Running

Start the server:

```bash
cargo run --bin server
```

In a separate terminal, run the test client:

```bash
cargo run --bin client
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `anyhow` | Error handling |
| `thiserror` | Typed error definitions |
| `bytes` | Buffer management |
