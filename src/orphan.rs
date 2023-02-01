use std::collections::HashMap;

use crate::db::Id;

#[derive(Debug)]
pub struct Orphan {
    pub shard: String,
    pub id: Id,
}

#[derive(Debug)]
pub struct OrphanSummary {
    total_count: usize,
    shard_map: HashMap<String, Vec<Id>>,
}

impl OrphanSummary {
    pub fn new(shards: Vec<&String>) -> Self {
        let total_count = 0;
        let mut shard_map = HashMap::new();
        for shard in shards.iter() {
            shard_map.insert(shard.clone().to_owned(), Vec::<Id>::new());
        }
        OrphanSummary {
            total_count,
            shard_map,
        }
    }

    pub fn add(&mut self, orphan: Orphan) {
        self.shard_map
            .get_mut(&orphan.shard)
            .expect("cannot find shard in orphan shard_map")
            .push(orphan.id);
        self.total_count += 1;
    }

    pub fn get_cluster_total(&self) -> usize {
        self.total_count
    }

    pub fn get_shard_totals(&self) -> HashMap<String, usize> {
        let shard_totals: HashMap<String, usize> = HashMap::from_iter(
            self.shard_map
                .iter()
                .map(|(key, vec_val)| (key.clone().to_owned(), vec_val.len())),
        );
        shard_totals
    }

    pub fn get_shard_map(&self) -> &HashMap<String, Vec<Id>> {
        &self.shard_map
    }
}
