use hustle_storage::block::BlockReference;
use hustle_storage::StorageManager;
use std::sync::mpsc::Sender;
use crate::router::BlockPoolDestinationRouter;

pub fn send_rows<'a>(
    rows: impl Iterator<Item = impl Iterator<Item = &'a [u8]>>,
    output_block: &mut BlockReference,
    block_tx: &Sender<u64>,
    router: &BlockPoolDestinationRouter,
    storage_manager: &StorageManager,
) {
    let mut rows = rows.peekable();
    while rows.peek().is_some() {
        output_block.insert_rows(&mut rows);

        if output_block.is_full() {
            block_tx.send(output_block.id).unwrap();
            *output_block = router.get_block(storage_manager);
        }
    }
}