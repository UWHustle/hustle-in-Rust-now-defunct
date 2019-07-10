use hustle_execution::ExecutionEngine;
use hustle_transaction::TransactionManager;
use crate::connection::ServerConnection;
use std::net::{TcpListener, ToSocketAddrs};
use std::io::Error;
use crossbeam_utils::thread;
use std::sync::{mpsc, Arc, RwLock, Mutex};
use std::collections::HashMap;
use hustle_common::Message;
use std::sync::mpsc::Sender;
use hustle_parser::Parser;
use hustle_resolver::Resolver;

pub struct Server {
    tcp_listener: TcpListener,
    connection_ctr: u64
}

impl Server {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<Server, Error> {
        TcpListener::bind(addr)
            .map(|tcp_listener| {
                Server {
                    tcp_listener,
                    connection_ctr: 0
                }
            })
    }

    pub fn listen(&mut self) {
        let connections: Arc<RwLock<HashMap<u64, Mutex<Sender<Vec<u8>>>>>> = Arc::new(
            RwLock::new(HashMap::new())
        );

        let mut parser = Parser::new();
        let mut resolver = Resolver::new();
        let mut transaction_manager = TransactionManager::new();
        let mut execution_engine = ExecutionEngine::new();

        let (parser_tx, parser_rx) = mpsc::channel();
        let (resolver_tx, resolver_rx) = mpsc::channel();
        let (transaction_tx, transaction_rx) = mpsc::channel();
        let (execution_tx, execution_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();

        thread::scope(|s| {
            // Spawn parser thread.
            let completed_tx_clone = completed_tx.clone();
            s.builder().name("parser".to_string()).spawn(move |_| {
                parser.listen(parser_rx, resolver_tx, completed_tx_clone);
            }).unwrap();

            // Spawn resolver thread.
            let transaction_tx_clone = transaction_tx.clone();
            let completed_tx_clone = completed_tx.clone();
            s.builder().name("resolver".to_string()).spawn(move |_| {
                resolver.listen(resolver_rx, transaction_tx_clone, completed_tx_clone);
            }).unwrap();

            // Spawn transaction manager thread.
            let completed_tx_clone = completed_tx.clone();
            s.builder().name("transaction".to_string()).spawn(move |_| {
                transaction_manager.listen(transaction_rx, execution_tx, completed_tx_clone);
            }).unwrap();

            // Spawn execution engine thread.
            let transaction_tx_clone = transaction_tx.clone();
            let completed_tx_clone = completed_tx.clone();
            s.builder().name("execution".to_string()).spawn(move |_| {
                execution_engine.listen(execution_rx, transaction_tx_clone, completed_tx_clone);
            }).unwrap();

            // Spawn completed statement thread.
            let connections_clone = connections.clone();
            s.builder().name("completed".to_string()).spawn(move |_| {
                loop {
                    let buf = completed_rx.recv().unwrap();
                    let request = Message::deserialize(&buf).unwrap();

                    let send_to_client = |connection_id: &u64| {
                        // Pass on the message to the correct connection.
                        connections_clone
                            .read().unwrap()[connection_id]
                            .lock().unwrap()
                            .send(request.serialize().unwrap())
                            .unwrap();
                    };

                    match &request {
                        Message::Schema { schema: _, connection_id } =>
                            send_to_client(connection_id),
                        Message::ReturnRow { row: _, connection_id } =>
                            send_to_client(connection_id),
                        Message::Success { connection_id } =>
                            send_to_client(connection_id),
                        Message::Failure { reason: _, connection_id } =>
                            send_to_client(connection_id),
                        _ => panic!("Invalid message type sent to completed statement handler")
                    }
                }
            }).unwrap();

            // Listen for new connections.
            for stream_result in self.tcp_listener.incoming() {
                match stream_result {
                    Ok(stream) => {
                        // Generate a new connection id.
                        let connection_id = self.connection_ctr;
                        self.connection_ctr += 1;

                        // Generate a new connection channel.
                        let (connection_tx, connection_rx) = mpsc::channel();
                        connections.write().unwrap()
                            .insert(connection_id, Mutex::new(connection_tx));
                        // TODO: Drop the connection when the TCP stream closes.

                        // Spawn a new connection thread.
                        let parser_tx = parser_tx.clone();
                        let transaction_tx = transaction_tx.clone();
                        let completed_tx = completed_tx.clone();
                        s.builder().name(format!("connection_{}", connection_id)).spawn(move |_| {
                            ServerConnection::new(
                                connection_id,
                                stream
                            ).listen(connection_rx, parser_tx, transaction_tx, completed_tx);
                        }).unwrap();
                    },
                    Err(e) => panic!("{}", e)
                }
            }
        }).unwrap();
    }
}
