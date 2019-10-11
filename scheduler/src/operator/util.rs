use hustle_storage::StorageManager;
use crate::router::BlockPoolDestinationRouter;
use std::sync::mpsc::Sender;

pub fn send_rows<'a>(
    storage_manager: &StorageManager,
    rows: &mut impl Iterator<Item = impl Iterator<Item = &'a [u8]>>,
    block_tx: &Sender<u64>,
    router: &BlockPoolDestinationRouter,
) {
    let mut output_block = router.get_block(storage_manager);
    loop {
        output_block.insert_rows(rows);

        if output_block.is_full() {
            block_tx.send(output_block.id).unwrap();
            println!("util: send block id {}", output_block.id);
            output_block = router.get_block(storage_manager);
        } else {
            router.return_block(output_block);
            break;
        }
    }
}

