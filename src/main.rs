#![allow(unused_imports)]
use core::str;
use std::{
    collections::{btree_map::Values, HashMap, HashSet},
    error::Error,
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    ops::Deref,
    time::Duration,
    usize,
};

enum RESP {
    BulkString(String),
    Array(Vec<RESP>),
}

struct Connection {
    connection: TcpStream,
    buffer: Vec<u8>,
}

struct TcpServer {
    listener: TcpListener,
    connections: HashMap<usize, Connection>,
    next_connection_id: usize,
}

impl RESP {
    fn parse_redis_protocol(input: &str) -> Result<RESP, String> {
        if input.starts_with("*") {
            RESP::parse_array(input)
        } else if input.starts_with('$') {
            RESP::parse_bulk_strings(input)
        } else {
            Err("Invalid Redis protocol".to_string())
        }
    }

    fn parse_bulk_strings(input: &str) -> Result<RESP, String> {
        let mut lines = input.lines();
        let _length: usize = lines
            .next()
            .and_then(|l| l.strip_prefix('$'))
            .and_then(|l| l.parse().ok())
            .ok_or("Invalid bulk string length")?;

        let value = lines.next().ok_or("Missing bulk string value")?;
        Ok(RESP::BulkString(value.to_string()))
    }

    fn parse_array(input: &str) -> Result<RESP, String> {
        let mut lines = input.lines();

        let count: usize = lines
            .next()
            .and_then(|l| l.strip_prefix('*'))
            .and_then(|l| l.parse().ok())
            .ok_or("Invalid array length")?;

        let mut values: Vec<RESP> = Vec::new();
        for _ in 0..count {
            let element = lines.by_ref().take(2).collect::<Vec<_>>().join("\r\n");
            values.push(RESP::parse_bulk_strings(&element)?);
        }

        Ok(RESP::Array(values))
    }

    fn get_response_value<'a, I>(values: I) -> String
    where
        I: IntoIterator<Item = &'a RESP>,
    {
        values
            .into_iter()
            .skip(1)
            .flat_map(|value| match value {
                RESP::BulkString(s) => vec![s.as_str()],
                RESP::Array(arr) => arr
                    .iter()
                    .filter_map(|v| {
                        if let RESP::BulkString(s) = v {
                            Some(s.as_str())
                        } else {
                            None
                        }
                    })
                    .collect(),
            })
            .collect::<Vec<&str>>()
            .join(" ")
    }
}

impl TcpServer {
    fn new(addr: &str) -> std::io::Result<TcpServer> {
        let listener = TcpListener::bind(addr).unwrap();
        listener.set_nonblocking(true).unwrap();
        Ok(TcpServer {
            listener,
            connections: HashMap::new(),
            next_connection_id: 1,
        })
    }

    fn run(&mut self) -> std::io::Result<()> {
        loop {
            self.accept_new_connections()?;

            self.handle_connections()?;

            self.parse_resp_connection_buffer()?;

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn accept_new_connections(&mut self) -> std::io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true).unwrap();
                    let id = self.next_connection_id;
                    self.connections.insert(id, {
                        Connection {
                            connection: stream,
                            buffer: Vec::new(),
                        }
                    });
                    self.next_connection_id += 1;
                    println!("New connection {}", id);
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn handle_connections(&mut self) -> std::io::Result<()> {
        let mut to_remove = Vec::new();

        for (&id, connection) in self.connections.iter_mut() {
            let mut buffer = [0; 1024];
            match connection.connection.read(&mut buffer) {
                Ok(0) => {
                    to_remove.push(id);
                    println!("Connection closed: {}", id);
                }
                Ok(_n) => connection.buffer.extend_from_slice(&buffer[.._n]),
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
        }
        for id in to_remove {
            self.connections.remove(&id);
        }
        Ok(())
    }

    fn parse_resp_connection_buffer(&mut self) -> std::io::Result<()> {
        for (_, connection) in self.connections.iter_mut() {
            match connection.buffer.len() {
                0 => {}
                _ => {
                    let incoming_command = std::str::from_utf8(&connection.buffer).unwrap();

                    match RESP::parse_redis_protocol(incoming_command) {
                        Ok(RESP::Array(values)) => {
                            println!("Passed array with {} elements", values.len());
                            for (_, value) in values.iter().enumerate() {
                                if let RESP::BulkString(s) = value {
                                    if s.starts_with("ECHO") {
                                        let response = RESP::get_response_value(&values);
                                        if let Err(e) = connection.connection.write_all(
                                            format!("${}\r\n{}\r\n", response.len(), response)
                                                .as_bytes(),
                                        ) {
                                            eprintln!("Error writing to stream: {}", e);
                                        }

                                        connection.buffer.clear();
                                        break;
                                    } else {
                                        if let Err(e) =
                                            connection.connection.write_all(b"+PONG\r\n")
                                        {
                                            eprintln!("Error writing to stream: {}", e);
                                        }
                                        connection.buffer.clear();
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(_) => println!("Unexpected Result"),
                        Err(e) => println!("Error: {}", e),
                    }
                }
            }
        }

        Ok(())
    }
}

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let mut server = TcpServer::new("127.0.0.1:6379").unwrap();
    let _ = server.run();
}
