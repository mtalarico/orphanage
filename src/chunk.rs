use mongodb::bson;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Chunk {
    pub shard: String,
    pub min: bson::Document,
    pub max: bson::Document,
}

impl Chunk {
    pub fn filter_for_range(&self) -> bson::Document {
        let mut filter = bson::Document::new();
        let mut non_equal_fields = Vec::new();
        for ((k1, v1), (k2, v2)) in self.min.iter().zip(self.max.iter()) {
            if k1 != k2 {
                panic!("shard key ranges are not the same???");
            }
            if v1 == v2 {
                filter.insert(k1, v1);
            } else {
                non_equal_fields.push(k1);
            }
        }
        if non_equal_fields.len() > 0 {
            let arr = self.get_unequal_cases(non_equal_fields);
            if arr.len() == 1 {
                let doc = arr.first().unwrap();
                filter.extend(doc.to_owned());
            } else {
                filter.insert("$or", arr);
            }
        }
        filter
    }

    /// wrapper function around recursion
    fn get_unequal_cases(&self, fields: Vec<&String>) -> Vec<bson::Document> {
        let mut cases = Vec::<bson::Document>::new();

        self.recusivly_add_cases(fields, &mut cases);
        cases
    }

    /// recursively add cases, subtracting fields off the end for each iteration until you reach a single field
    fn recusivly_add_cases(&self, mut fields: Vec<&String>, cases: &mut Vec<bson::Document>) {
        let length = fields.len();
        let last_key = fields.pop().unwrap();
        let min_last_value = self.min.get(last_key).unwrap();
        let max_last_value = self.max.get(last_key).unwrap();
        // if this is our outside cases, we need a gte not just gt
        let condition: &str;
        if cases.is_empty() {
            condition = "$gte";
        } else {
            condition = "$gt";
        }

        if length == 1 {
            let bound =
                bson::doc! {last_key.clone(): { condition: min_last_value, "$lt": max_last_value }};

            cases.push(bound);

            return;
        }

        let mut gt = bson::doc! {};
        let mut lt = bson::doc! {};

        for each in fields.iter() {
            let min_val = self.min.get(each).unwrap();
            let max_val = self.max.get(each).unwrap();

            gt.insert(each.clone(), min_val);
            lt.insert(each.clone(), max_val);
        }

        gt.insert(last_key.clone(), bson::doc! { condition: min_last_value});
        lt.insert(last_key.clone(), bson::doc! { "$lt": max_last_value});
        cases.push(gt);
        cases.push(lt);
        self.recusivly_add_cases(fields, cases)
    }
}

pub fn merge_or_add(chunk: &Chunk, chunks: &mut Vec<Chunk>) {
    let mut combined = false;
    for each in chunks.iter_mut() {
        // Let this chunk = {A_min, A_max} and the chunk we are comparing = { B_min, B_max }
        if chunk.shard == each.shard && chunk.max == each.min {
            // if A_max = B_min, make the new megachunk = { A_min, B_max }
            each.min = chunk.min.clone();
            combined = true;
        } else if chunk.shard == each.shard && chunk.min == each.max {
            // if A_min = B_max, make the new megachunk = { B_min, A_max }
            each.max = chunk.max.clone();
            combined = true;
        }
    }
    if combined == false {
        chunks.push(chunk.clone());
    }
}

#[cfg(test)]
mod tests {
    use mongodb::bson;

    use super::Chunk;

    #[test]
    fn four_fields_all_equal() {
        let chunk = Chunk {
            shard: String::from(""),
            min: bson::doc! { "a": 1, "b": 1, "c": 1, "d": 1},
            max: bson::doc! { "a": 1, "b": 1, "c": 1, "d": 1},
        };
        let filter = chunk.filter_for_range();
        assert_eq!(filter, bson::doc! { "a": 1, "b": 1, "c": 1, "d": 1 });
    }

    #[test]
    fn four_fields_three_equal_four_range() {
        let chunk = Chunk {
            shard: String::from(""),
            min: bson::doc! { "a": 1, "b": 1, "c": 1, "d": 1},
            max: bson::doc! { "a": 1, "b": 1, "c": 1, "d": 3},
        };
        let filter = chunk.filter_for_range();
        assert_eq!(
            filter,
            bson::doc! {"a": 1, "b": 1, "c": 1, "d": {"$gte": 1, "$lt": 3}}
        );
    }

    #[test]
    fn four_fields_three_and_four_range() {
        let chunk = Chunk {
            shard: String::from(""),
            min: bson::doc! { "a": 1, "b": 1, "c": 1, "d": 1},
            max: bson::doc! { "a": 1, "b": 1, "c": 3, "d": 3},
        };
        let filter = chunk.filter_for_range();
        assert_eq!(
            filter,
            bson::doc! {"a": 1, "b": 1, "$or": [ {"c": 1, "d": { "$gte": 1 }}, {"c": 3, "d": { "$lt": 3 }}, {"c": {"$gt": 1, "$lt": 3}},  ]}
        );
    }

    #[test]
    fn four_fields_three_range_four_free() {
        let chunk = Chunk {
            shard: String::from(""),
            min: bson::doc! { "a": 1, "b": 1, "c": 1, "d": 1},
            max: bson::doc! { "a": 1, "b": 1, "c": 3, "d": bson::Bson::MinKey},
        };
        let filter = chunk.filter_for_range();
        assert_eq!(
            filter,
            bson::doc! { "a": 1, "b": 1, "$or": [ {"c": 1, "d": { "$gte": 1 }}, {"c": 3, "d": { "$lt": bson::Bson::MinKey }}, {"c": {"$gt": 1, "$lt": 3}},  ]}
        );
    }

    #[test]
    fn four_fields_two_range_three_and_four_free() {
        let chunk = Chunk {
            shard: String::from(""),
            min: bson::doc! { "a": 1, "b": 1, "c": "example", "d": 1},
            max: bson::doc! { "a": 1, "b": 8, "c": 3, "d": bson::Bson::MinKey},
        };
        let filter = chunk.filter_for_range();
        assert_eq!(
            filter,
            bson::doc! { "a": 1, "$or": [ {"b": 1, "c": "example", "d": { "$gte": 1 }}, {"b": 8, "c": 3, "d": { "$lt": bson::Bson::MinKey }},  {"b": 1, "c": { "$gt": "example" }}, {"b": 8, "c": { "$lt": 3 }}, {"b": {"$gt": 1, "$lt": 8}}]}
        );
    }

    #[test]
    fn one_field_range() {
        let chunk = Chunk {
            shard: String::from(""),
            min: bson::doc! { "a": 1 },
            max: bson::doc! { "a": 3 },
        };
        let filter = chunk.filter_for_range();
        assert_eq!(filter, bson::doc! { "a": {"$gte": 1, "$lt": 3}});
    }
}
