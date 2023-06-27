
##### design

- There are three possible jobs , NO_JOB, MAP_JOB, REDUCE_JOB
- NO_JOB doesn't mean the map-reduce is done, some job may fail, the worker will retry those jobs. When a worker got a NO_JOB, it will do nothing.
- Worker send requests to coordinator, waiting coordinator to allocate an job.
- Use worker-polling mechanism to achieve at-least-once. The coordinator will response the first unallocated job, it's different from the paper.
- The connection is closed by coordinator, if all reduce work's succeed flag is true, then the work is done.
- Only when all map-works are done(which means the map-phase is done), coordinator can allocate a reduce job to a worker. There can be some occasions, all map job are allocated, but not all succeed, then workers can only get "NO_JOB" response from the coordinator.
- The temporary files are "mr-x-y", x is the map-work id, in this scenario, a file is a map-work. y is the reduce id, scales [0, nReduce).  All key-value pairs are sent to different reduce bucket by ihash. Reduce job's number is nReduce, which means a reduce-worker for reduce number `k` will scan all files likes "mr-x-k" , gather all kv pairs, and produce an output file "mr-out-k".
- The data-share is mainly done by file-system, the rpc only send some metadata, such as id and index. We should use distributed file system (such as gfs, nfs) in distributed scenario.

##### making progress prove
- There are three jobs states, unallocated, allocated, succeed.
- Workers will request a job from coordinator, changing a job state from unallocated to "allocated". Workers pull requests every 1 second. 
- There is a checker go-routine in coordinator, it remembers the last allocated time. The checker will scan all allocated but no-succeed jobs, checking the time with the "expire time", which is 10 seconds. The checker does its worker every 5 seconds.
- The checker will mark all expired works from "allocated" to "unallocated", making it can be re-allocated to another worker.
- When a worker done its job, it will send a "FinishJobMessage" to coordinator, marking a job "succeed". Even in unreliable network condition, it will only cause redo, which is harmless.

##### isolation prove
- Use coarse-grained global lock to protect shared data by multiple workers and coordinator.
- The shared data are allocated-flags.
- Different workers will not change same file in the same time, it's proved by the filename design "mr-x-y".

##### crash consistency prove

- Use reduceSucceed flag to achieve atomicity, change a value is not atomic, but we use lock to prove only one worker/coordinator can change this flag. So the commit point is the reduceSucceed flag is true
- The redo of a work is harmless, it will simply overwrite the old file.
- If a coordinator crashed, the code should only simply restart the coordinator. Since the flags are not persistent on disk, the whole task should redo. But the test script only consider worker's crash.
- Different from the paper, we don't ping worker. That's because we use worker-polling mechanism rather than coordinator-allocate mechanism.