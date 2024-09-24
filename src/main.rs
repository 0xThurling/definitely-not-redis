#![allow(unused_imports)]
use std::{
    collections::{HashMap, HashSet},
    io::{ErrorKind, Read, Write},
    net::{TcpListener, TcpStream},
    time::Duration,
    usize,
};

struct TcpServer {
    listener: TcpListener,
    connections: HashMap<usize, TcpStream>,
    next_connection_id: usize,
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

            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn accept_new_connections(&mut self) -> std::io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true).unwrap();
                    let id = self.next_connection_id;
                    self.connections.insert(id, stream);
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

        for (&id, stream) in self.connections.iter_mut() {
            let mut buffer = [0; 1024];
            match stream.read(&mut buffer) {
                Ok(0) => {
                    to_remove.push(id);
                    println!("Connection closed: {}", id);
                }
                Ok(n) => {
                    if let Err(e) = stream.write_all(b"+PONG\r\n") {
                        eprintln!("Error writing to stream {}: {}", id, e);
                        to_remove.push(id);
                    }
                }
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {}
                Err(e) => return Err(e),
            }
        }
        for id in to_remove {
            self.connections.remove(&id);
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
