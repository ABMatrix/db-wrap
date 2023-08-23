use rocksdb::Options;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RocksdbOptions {
    // TODO add more options for better usage
    pub create_if_missing: bool,
    pub atomic_flush: bool,
    // default 5
    pub log_file_num: usize,
    // default 100M
    pub log_file_size: usize,
}

impl Default for RocksdbOptions {
    fn default() -> Self {
        RocksdbOptions {
            create_if_missing: true,
            atomic_flush: true,
            log_file_num: 5,
            log_file_size: 100 * 1000 * 1000
        }
    }
}

impl From<RocksdbOptions> for Options {
    fn from(roc_opt: RocksdbOptions) -> Self {
        let mut opt = Options::default();
        opt.create_if_missing(roc_opt.create_if_missing);
        opt.set_atomic_flush(roc_opt.atomic_flush);
        opt.set_keep_log_file_num(roc_opt.log_file_num);
        opt.set_max_log_file_size(roc_opt.log_file_size);
        opt
    }
}
