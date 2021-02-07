# versions

* 0.0.1 - d4410334

* 0.1.0 - 93b5c26e
    1. add log
    2. multiple databases to reduce clash

* 0.1.1 - e60e3b87
    1. replace std hashmap with dashmap

* 0.2.0 - 2855381b
    1. implement and tested expiration.
    2. rewrite error handling with anyhow.
    3. change key from String to Bytes.
    4. replace dashmap with forked version.
    
    0.2.0 has a pretty good small packet performance, large packet performance is bad though. There isn't a secret sauce in it - it reduces contention by having tons of hashmaps. It's the last version based on Mutex.

* 0.3.0 - e819202b
    1. change concurreny model from `shared state workers` to `actor`, significantly improve large packet performance.

* 0.3.1 - 87939e58
    1. changed channel mechanism
    
    This commit, however, is not guaranteed to be positive. It's beneficial in `Theory` but I didn't test it thoroughly.

* 0.4.0 - 5798ece2
    1. object pool (for handler).
    2. refactor shutdown mechanism.
    3. rewrite encoder to improve get performance.
    4. rewrite arg parsing.

* 0.4.1 - 9f6600e8
    1. rewrite read by implementing `intermediate token parsing`. 

    This improvement is introduced by `Flamegraph`. Through which I found that `get_line` in decoding is the bottleneck.

* 0.4.2 - 9d3e4a8e
    1. implemented debug: key_num, total_key_len, total_val_len
    2. implement debug - random_keys
    3. implement list as a return type from database.
    4. remove some unused dependencies.