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

    pub fn cluster_total(&self) -> usize {
        self.total_count
    }

    pub fn shard_totals(&self) -> HashMap<String, usize> {
        let shard_totals: HashMap<String, usize> = HashMap::from_iter(
            self.shard_map
                .iter()
                .filter(|(_, ids)| ids.len() > 0)
                .map(|(key, ids)| (key.clone().to_owned(), ids.len())),
        );
        shard_totals
    }

    pub fn shard_map(&self) -> HashMap<String, Vec<Id>> {
        let filtered = HashMap::from_iter(
            self.shard_map
                .iter()
                .filter(|(_, ids)| ids.len() > 0)
                .map(|(name, ids)| (name.to_owned(), ids.to_owned())),
        );
        filtered
    }

    pub fn num_shards(&self) -> usize {
        self.shard_map.values().filter(|ids| ids.len() > 0).count()
    }
}
