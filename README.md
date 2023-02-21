# orphanage

**THIS PROGRAM IS CURRENTLY IN AN EXPERIMENTAL STATE AND IS NOT INTENDED TO BE RUN IN PRODUCTION**

This is a rewrite of https://github.com/mongodb/support-tools/tree/master/orphanage in rust with a focus on improve performance via concurrency

## Usage

 ### Building
 ** Note the balancer must be manually disabled before running this program, see todo for future plans **

`cargo build --release`

`cd ./target/release/`

### Running

`orphanage --help`
```
A program to locate orphaned documents on your sharded MongoDB cluster

Usage: orphanage [OPTIONS] <COMMAND>

Commands:
  estimate  Return each shard's orphan count based on metadata [lowest performance impact]
  print     Query each shard's real orphan count or list of IDs [heavier performance impact]
  update    Query and update each shard, marking its orphans or writing their IDs to a designated namespace [heaviest performance impact]
  help      Print this message or the help of the given subcommand(s)

Options:
      --uri <URI>    URI of MongoDB cluster, refer to https://www.mongodb.com/docs/manual/reference/connection-string/ for format [default: mongodb://localhost:27016]
  -d, --db <DB>      Database name [default: test]
  -c, --coll <COLL>  Collection name [default: test]
  -h, --help         Print help
  -V, --version      Print version
```
### Additional Options
To see if a subcommand has any additional parameters that can be passed, run `--help` after the subcommand, e.g.

`orphanage ... print --help`
```
Query each shard's real orphan count or list of IDs [heavier performance impact]

Usage: orphanage print [OPTIONS]

Options:
      --verbose  Set true to print a verbose map of each shard's orphan ID
  -h, --help     Print help
```

## Todo
* ~~add ability to estimate, print, or update orphans~~
* add output of orphan IDs to a namespace
* add balancer check
* optionally disable balancing on specified collection before check, enable it after complete
* [potentially] background thread that checks balancer stays disabled during length of check

---

 ## Disclaimer

   Please note: all tools/ scripts in this repo are released for use "AS
   IS" without any warranties of any kind, including, but not limited to
   their installation, use, or performance. We disclaim any and all
   warranties, either express or implied, including but not limited to
   any warranty of noninfringement, merchantability, and/ or fitness for
   a particular purpose. We do not warrant that the technology will
   meet your requirements, that the operation thereof will be
   uninterrupted or error-free, or that any errors will be corrected.

   Any use of these scripts and tools is at your own risk. There is no
   guarantee that they have been through thorough testing in a
   comparable environment and we are not responsible for any damage
   or data loss incurred with their use.

   You are responsible for reviewing and testing any scripts you run
   thoroughly before use in any non-testing environment.


 ## License

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
