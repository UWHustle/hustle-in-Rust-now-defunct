use optimizer::optimize;
use crate::result::HustleResult;
use crate::connection::HustleConnection;

pub struct HustleStatement<'a> {
    statement: Vec<String>,
    params: Vec<String>,
    connection: &'a HustleConnection<'a>
}

impl<'a> HustleStatement<'a> {
    pub fn new(sql: &str, connection: &'a HustleConnection<'a>) -> Self {
        let statement: Vec<String> = sql.split("?").map(|s| s.to_string()).collect();
        let params = (0..statement.len() - 1).map(|_| String::new()).collect();
        HustleStatement {
            statement,
            params,
            connection
        }
    }

    pub fn bind(&mut self, index: usize, param: String) -> Result<(), String> {
        if index > self.params.len() {
            Err("Parameter index out of range".to_string())
        } else {
            self.params[index] = param;
            Ok(())
        }
    }

    pub fn execute(&mut self) -> Result<Option<HustleResult>, String> {
        if self.params.iter().any(|p| p.is_empty()) {
            Err("Statement has unbound parameters".to_string())
        } else {
            let mut sql: String = self.statement.first()
                .map_or(Err("Sql is empty".to_string()), |r| Ok(r))?
                .clone();

            for param in &self.params {
                sql.push_str(param.as_str())
            }

            let plan = optimize(&sql)?;

            let result = self.connection
                .execution_engine().execute_plan(&plan)
                .map(|relation| HustleResult::new(relation, self.connection));

            Ok(result)
        }
    }
}
