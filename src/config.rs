use rocksdb::Options;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RocksdbOptions {
    // TODO add more options for better usage
    pub create_if_missing: bool,
    pub atomic_flush: bool,
}

impl Default for RocksdbOptions {
    fn default() -> Self {
        RocksdbOptions {
            create_if_missing: true,
            atomic_flush: true,
        }
    }
}

impl From<RocksdbOptions> for Options {
    fn from(roc_opt: RocksdbOptions) -> Self {
        let mut opt = Options::default();
        opt.create_if_missing(roc_opt.create_if_missing);
        opt.set_atomic_flush(roc_opt.atomic_flush);
        opt
    }
}
