# async-redis
A prototype of a high-performance KV database built with Rust.  

Author: `3andero`

11/10/2021

## Overview

The project starts as a fork of [mini-redis](https://github.com/tokio-rs/mini-redis), and then evolves into a lockless actor based design iteratively. The removal of explicit use of Mutex results in better throughput. It can achieve similar or slightly better performance to the original [Redis](https://redis.io/) on x86 Linux, and slightly worse performance on AArch64 machines, partly due to Rust not providing SwissTable on these platforms. When it comes to multiple operations (e.g., mset, mget), there's a significant performance gain for our implementation.

![workflow](https://user-images.githubusercontent.com/31029660/136853355-249eaf08-00e9-443d-92db-3e6b5c864574.png)

## Features

* get
* set
* getset
* setex/setnx
* ttl/pttl
* mget
* mset
* incr/decr/incrby/decrby
* subscribe/unsubscribe
* publish

## License

[GPL-3](https://github.com/3andero/async-redis/blob/main/LICENSE)
