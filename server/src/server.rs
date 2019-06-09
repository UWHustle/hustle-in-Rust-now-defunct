use execution::ExecutionEngine;
use concurrency::TransactionManager;
use crate::connection::ServerConnection;
use std::net::{TcpListener, ToSocketAddrs};
use std::io::Error;
use crossbeam_utils::thread;
use std::sync::{mpsc, Arc, RwLock, Mutex};
use optimizer::Optimizer;
use std::collections::HashMap;
use message::Message;
use std::sync::mpsc::Sender;

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

        let mut optimizer = Optimizer::new();
        let mut transaction_manager = TransactionManager::new();
        let mut execution_engine = ExecutionEngine::new();

        let (optimizer_tx, optimizer_rx) = mpsc::channel();
        let (transaction_tx, transaction_rx) = mpsc::channel();
        let (execution_tx, execution_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();

        thread::scope(|s| {
            // Spawn optimizer thread.
            let optimizer_error_tx = completed_tx.clone();
            s.spawn(move |_| {
                optimizer.listen(optimizer_rx, transaction_tx, optimizer_error_tx);
            });

            // Spawn transaction manager thread.
            s.spawn(move |_| {
                transaction_manager.listen(transaction_rx, execution_tx);
            });

            // Spawn execution engine thread.
            s.spawn(move |_| {
                execution_engine.listen(execution_rx, completed_tx);
            });

            // Spawn completed statement thread.
            let connections_clone = connections.clone();
            s.spawn(move |_| {
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
                        Message::Error { reason: _, connection_id } =>
                            send_to_client(connection_id),
                        _ => panic!("Invalid message type sent to completed statement handler")
                    }
                }
            });

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

                        // Spawn a new connection thread.
                        let optimizer_tx = optimizer_tx.clone();
                        s.spawn(move |_| {
                            ServerConnection::new(
                                connection_id,
                                stream
                            ).listen(connection_rx, optimizer_tx);
                        });
                    },
                    Err(e) => panic!("{}", e)
                }
            }
        }).unwrap();
    }
}

