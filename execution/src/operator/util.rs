use hustle_storage::StorageManager;
use std::sync::mpsc::Sender;
use crate::router::BlockPoolDestinationRouter;

pub fn send_rows<'a>(
    rows: &mut impl Iterator<Item = impl Iterator<Item = &'a [u8]>>,
    block_tx: &Sender<u64>,
    router: &BlockPoolDestinationRouter,
    storage_manager: &StorageManager,
) {
    let mut rows = rows.peekable();
    let mut output_block = router.get_block(storage_manager);
    loop {
        output_block.insert_rows(&mut rows);

        if rows.peek().is_some() {
            block_tx.send(output_block.id).unwrap();
            output_block = router.get_block(storage_manager);
        } else {
            router.return_block(output_block);
            break;
        }
    }
}
