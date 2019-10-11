use std::collections::HashMap;
use std::io::Error;
use std::net::{TcpListener, ToSocketAddrs};
use std::sync::{Arc, mpsc, Mutex, RwLock};
use std::sync::mpsc::Sender;

use crossbeam_utils::thread;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use hustle_catalog::Catalog;
use hustle_common::message::{InternalMessage, Message};
use hustle_execution::ExecutionEngine;
use hustle_resolver::Resolver;
use hustle_transaction::TransactionManager;
use hustle_common::plan::Statement;
use threadpool::ThreadPool;

/// Hustle`s server. Clients connect to the server via TCP.
pub struct Server {
    tcp_listener: TcpListener,
    connection_ctr: u64
}

impl Server {
    /// Binds the `Server` to the specified `addr` and returns a `Server` if the binding is
    /// successful.
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<Server, Error> {
        TcpListener::bind(addr)
            .map(|tcp_listener| {
                Server {
                    tcp_listener,
                    connection_ctr: 0
                }
            })
    }

    /// Listen for new TCP connections and fulfill the requests of clients.
    pub fn listen(&mut self) {
        // A set of message transmitters to each client connection, indexed on connection ID.
        let connections: Arc<RwLock<HashMap<u64, Mutex<Sender<InternalMessage>>>>> = Arc::new(
            RwLock::new(HashMap::new())
        );

        // Construct the major components of Hustle.
        let catalog = Arc::new(Catalog::try_from_file().unwrap_or(Catalog::new()));
        let mut resolver = Resolver::new(catalog.clone());
        let mut transaction_manager = TransactionManager::new();
        let execution_engine = Arc::new(ExecutionEngine::new(catalog));

        // Construct channels to pass messages between the major components.
        let (parser_tx, parser_rx) = mpsc::channel::<InternalMessage>();
        let (transaction_tx, transaction_rx) = mpsc::channel();
        let (execution_tx, execution_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();

        let transaction_tx_clone = transaction_tx.clone();
        thread::scope(|s| {
            // Spawn parser/resolver thread.
            let completed_tx_clone = completed_tx.clone();
            s.builder().name("parser".to_string()).spawn(move |_| {
                let dialect = GenericDialect {};
                for message in parser_rx {
                    if let Message::ParseSql { sql } = message.inner {
                        match Parser::parse_sql(&dialect, sql) {
                            Ok(stmts) => {
                                match resolver.resolve(&stmts) {
                                    Ok(plan) => transaction_tx_clone.send(InternalMessage::new(
                                        message.connection_id,
                                        Message::TransactPlan { plan}
                                    )).unwrap(),
                                    Err(reason) => completed_tx_clone.send(InternalMessage::new(
                                        message.connection_id,
                                        Message::Failure { reason }
                                    )).unwrap(),
                                }
                            },
                            Err(e) => completed_tx_clone.send(InternalMessage::new(
                                message.connection_id,
                                Message::Failure { reason: e.to_string() },
                            )).unwrap(),
                        }
                    }
                }
            }).unwrap();

            // Spawn transaction manager thread.
            let completed_tx_clone = completed_tx.clone();
            s.builder().name("transaction".to_string()).spawn(move |_| {

                let execute_statements = |statements: Vec<Statement>| {
                    for statement in statements {
                        execution_tx.send(InternalMessage::new(
                            statement.connection_id,
                            Message::ExecuteStatement { statement }
                        )).unwrap();
                    }
                };

                let handle_result = |statements: Result<Vec<Statement>, String>, connection_id| {
                    match statements {
                        Ok(statements) => execute_statements(statements),
                        Err(reason) => completed_tx_clone.send(InternalMessage::new(
                            connection_id,
                            Message::Failure { reason },
                        )).unwrap()
                    }
                };

                for message in transaction_rx {
                    match message.inner {
                        Message::TransactPlan { plan } => {
                            let statements = transaction_manager.transact_plan(
                                plan,
                                message.connection_id,
                            );
                            handle_result(statements, message.connection_id);
                        },
                        Message::CompleteStatement { statement } => {
                            let statements = transaction_manager.complete_statement(statement);
                            handle_result(statements, message.connection_id);
                        },
                        Message::CloseConnection => {
                            let statements = transaction_manager.close_connection(message.connection_id);
                            handle_result(Ok(statements), message.connection_id);
                        },
                        _ => (),
                    }
                }
            }).unwrap();

            // Spawn execution engine thread.
            let transaction_tx_clone = transaction_tx.clone();
            let completed_tx_clone = completed_tx.clone();
            s.builder().name("execution".to_string()).spawn(move |_| {
                let pool = ThreadPool::new(2);
                for message in execution_rx {
                    match message.inner {
                        Message::ExecuteStatement { statement } => {
                            let execution_engine = execution_engine.clone();
                            let transaction_tx_clone = transaction_tx_clone.clone();
                            let completed_tx_clone = completed_tx_clone.clone();
                            let connection_id = message.connection_id;
                            pool.execute(move || {
                                match execution_engine.execute_plan(statement.plan.clone()) {
                                    Ok(table) => {
                                        // The execution may have produced a result table. If so, we send the
                                        // rows back to the user.
                                        if let Some(table) = table {
                                            // Send a message with the result schema.
                                            completed_tx_clone.send(InternalMessage::new(
                                                connection_id,
                                                Message::Schema { schema: table.columns.clone() }
                                            )).unwrap();

                                            execution_engine.get_rows(table, |row|
                                                completed_tx_clone.send(InternalMessage::new(
                                                    connection_id,
                                                    Message::ReturnRow { row }
                                                )).unwrap()
                                            );
                                        }

                                        // Send a success message to indicate completion.
                                        if !statement.silent {
                                            completed_tx_clone.send(InternalMessage::new(
                                                connection_id,
                                                Message::Success,
                                            )).unwrap();
                                        }
                                    },
                                    Err(reason) => completed_tx_clone.send(InternalMessage::new(
                                        connection_id,
                                        Message::Failure { reason }
                                    )).unwrap(),
                                };

                                // Notify the transaction manager that the plan execution has completed.
                                transaction_tx_clone.send(InternalMessage::new(
                                    connection_id,
                                    Message::CompleteStatement { statement }
                                )).unwrap();
                            });
                        },
                        _ => (),
                    }
                }
            }).unwrap();

            // Spawn completed statement thread.
            let connections_clone = connections.clone();
            s.builder().name("completed".to_string()).spawn(move |_| {
                for message in completed_rx {
                    let _ = connections_clone
                        .read().unwrap()[&message.connection_id]
                        .lock().unwrap()
                        .send(message);
                }
            }).unwrap();

            // Listen for new connections.
            for stream_result in self.tcp_listener.incoming() {
                match stream_result {
                    Ok(mut tcp_stream) => {
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
                        s.builder().name(format!("connection_{}", connection_id)).spawn(move |_| {
                            loop {
                                match Message::receive(&mut tcp_stream) {
                                    Ok(request) => {
                                        match request {
                                            Message::ExecuteSql { sql } => parser_tx.send(
                                                InternalMessage::new(
                                                    connection_id,
                                                    Message::ParseSql { sql }
                                                )
                                            ).unwrap(),
                                            _ => (),
                                        };

                                        // Wait for the response.
                                        for result in &connection_rx {
                                            result.inner.send(&mut tcp_stream).unwrap();
                                            match result.inner {
                                                Message::Success
                                                    | Message::Failure { reason: _ } => break,
                                                _ => (),
                                            }
                                        }
                                    },
                                    Err(_) => {
                                        transaction_tx.send(InternalMessage::new(
                                            connection_id,
                                            Message::CloseConnection,
                                        )).unwrap();
                                        break;
                                    },
                                }
                            }
                        }).unwrap();
                    },
                    Err(e) => panic!("{}", e)
                }
            }
        }).unwrap();
    }
}
