
    const ROW_SIZE: usize = 16;
    const FIELD_SIZE: usize = 8;

    const ROWS_PER_CHUNK: usize = 1024 * 128;
    const CHUNK_COUNT: usize = 16;

    const ROW_COUNT: usize = ROWS_PER_CHUNK * CHUNK_COUNT;
    const CHUNK_SIZE: usize = ROWS_PER_CHUNK * ROW_SIZE;
    //1024*1024;
    const SIZE: usize = CHUNK_SIZE * CHUNK_COUNT;
