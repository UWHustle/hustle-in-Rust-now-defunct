#[cfg(test)]
mod tests {
    use concurrency::TransactionManager;
    use std::sync::Arc;
    use execution::ExecutionEngine;

    #[test]
    fn test_transaction_manager() {
        let ee = Arc::new(ExecutionEngine::new());
        let tm = TransactionManager::new(ee);

        let _conn = tm.connect();
    }
}