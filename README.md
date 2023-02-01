# orphanage

**THIS PROGRAM IS CURRENTLY IN AN EXPERIMENTAL STATE AND IS NOT INTENDED TO BE RUN IN PRODUCTION**


This is an incomplete rewrite of https://github.com/mongodb/support-tools/tree/master/orphanage in rust

## Usage:

Note the balancer must be manually disabled before running this program, see todo for future plans

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

## Todo
* add output of orphan IDs to a namespace
* add balancer check
* disable balancing on specified collection during check
* background thread that checks balancer stays diabled during length of check ?


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
