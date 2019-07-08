use std::collections::{VecDeque, HashMap};
use crate::statement::Statement;

pub struct Transaction {
    pub id: u64,
    pub committed: bool,
    statements: VecDeque<Statement>,
}

impl Transaction {
    pub fn new(id: u64) -> Self {
        Transaction {
            id,
            committed: false,
            statements: VecDeque::new(),
        }
    }

    pub fn push_back(&mut self, statement: Statement) {
        self.statements.push_back(statement);
    }

    pub fn pop_front(&mut self) -> Option<Statement> {
        self.statements.pop_front()
    }

    pub fn commit(&mut self) {
        self.committed = true;
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }
}

pub struct TransactionQueue {
    deque: VecDeque<u64>,
    map: HashMap<u64, Transaction>,
}


impl TransactionQueue {
    pub fn new() -> Self {
        TransactionQueue {
            deque: VecDeque::new(),
            map: HashMap::new(),
        }
    }

    pub fn push_back(&mut self, transaction: Transaction) {
        self.deque.push_back(transaction.id);
        self.map.insert(transaction.id, transaction);
    }

    pub fn pop_front(&mut self) -> Option<Transaction> {
        self.deque.pop_front()
            .and_then(|ref transaction_id| self.map.remove(transaction_id))
    }

    pub fn get_mut(&mut self, transaction_id: &u64) -> Option<&mut Transaction> {
        self.map.get_mut(transaction_id)
    }

    pub fn front(&self) -> Option<&Transaction> {
        self.deque.front()
            .and_then(|transaction_id| self.map.get(transaction_id))
    }

    pub fn front_mut(&mut self) -> Option<&mut Transaction> {
        self.deque.front()
            .map(|transaction_id| transaction_id.clone())
            .and_then(move |transaction_id| self.map.get_mut(&transaction_id))
    }

    pub fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }

    pub fn len(&self) -> usize {
        self.deque.len()
    }
}
