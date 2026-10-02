use std::fs;

fn main() {
    let mut code = fs::read_to_string("src/swarm/blobstore.rs").unwrap();
    
    // 1. Add fields to BlobMeta
    code = code.replace("pub residency_region: Option<super::marketplace::GeoRegion>,\n}", "pub residency_region: Option<super::marketplace::GeoRegion>,\n    pub is_sharded: bool,\n    pub shard_params: Option<(usize, usize)>,\n}");

    // 2. Add them to instantiation 1
    code = code.replace("residency_region: None,\n        };", "residency_region: None,\n            is_sharded: false,\n            shard_params: None,\n        };");

    fs::write("src/swarm/blobstore.rs", code).unwrap();
}
