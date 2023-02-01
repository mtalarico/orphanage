# orphanage

Usage:

`cargo build --release`

`./target/release/orphanage --help`

```
Find orphan documents on your sharded MongoDB cluster

Usage: orphanage [OPTIONS] --uri <URI> --db <DB> --coll <COLL>

Options:
      --uri <URI>    URI of MongoDB cluster, refer to https://www.mongodb.com/docs/manual/reference/connection-string/ for format
  -d, --db <DB>      Database name
  -c, --coll <COLL>  Collection name
      --verbose      Print individual IDs of each orphan doc in each shard -- *warning* this may produce signficant output
  -h, --help         Print help
  -V, --version      Print version
```
