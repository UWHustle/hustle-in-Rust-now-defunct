pub struct Create_Table{
    relation: Relation
}

impl Create_Table {
    pun fn new(relation: Relation) -> Self {
        Create_Table {
            relation,
        }
    }
}

impl Operator for ImportCsv {
    fn get_target_relation(&self) -> Relation {
        self.relation.clone()
    }

    fn execute(&self) -> Relation {
        let mut data = StorageManager::create_relation(
            &self.relation,
            0,
        );

        StorageManager::flush(&data);

        self.get_target_relation()
    }
}