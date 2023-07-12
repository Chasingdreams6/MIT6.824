

#### Definitions

1. All replicas in the group must agree on whether the Put occurred before or after the reconfiguration.
2. `shards`, a set of k-v pairs. Different shards may have same k-v pairs, there may be some shards in the system.
3. `replica groups`, a set of shards. Request will send to only one `shards`. `shards` can be transferred between different `replica groups`. `replica groups` can be add to or leave the system.
4. `reconfiguration`, change the assignment of `shards` to `replica groups`.
5. `shard controller`, a service based on raft,  sending the requests to different `replica groups`.

#### shardctrler

`shardctrler` has 4 RPC for administrator to manage `replica groups`. It generates configuration. 

- Join. The Join RPC is used by an administrator to add new replica groups. The shardctrler should divide the shards equally. GID must reuse.
- Leave
- Move
- Query

