

### think
1. A good design is much more significant than coding, a good coding is much more significant than debugging.
2. Debugging from your own code, rather than thinking some complicated solutions.
3. Go-routine is easy for use. There are two ways to communicate between two routine, one is LOCK+POLL, the second is Channel.
4. A pretty output is good for debugging parallelism, A automatic script is good for testing. Ref: https://blog.josejg.com/debugging-pretty/. It really help me a lot.


### Design Principle
1. Use LOCK+POLL rather than Channel. Because Pooling is more simple, but needs design timeout carefully.
2. Use some background go-routine to check some status and do some jobs.
3. Use low heartbeat rate + high frequency poll to increase performance. Some operation such as RP, InstallSnapshot will also be seen as heartbeat.


### Debugging
1. `dlog` is a python3 script for present output.
2. `dtest` is a python3 script for test in parallel.
3. `cat xxx.log | dlog -c n` is to show log.
4. `dest TestXXX -p 16 -n 200 -r -v` is to testXXX, using 16 workers for 200 times. And we also print log and use race detector. The workers number is based on your PC's core.
5. `cat test_test.go| grep 2C | sed 's\(\ \g'|awk '/func/ {printf "%s ",$2;}' | xargs dtest -p 16 -v -r -n 300` is to test all in 2C, using 16 workers.

### Function and Thread

#### Background

*The background thread will check some state, or to maintain some state.* 

1. `ticker()`, background function to check server's heartbeat. If election timeout, it will become candidate and try to send requestVote to others.
2. `sendHB()`, background function for `LEADER` to send heartbeat to every node(include leader itself). We should consider heartbeat rate and election timeout carefully. If The election timeout is too short, the leader may change frequently, which is bad for making progress. When the heartbeat is too fast, it will occupy the network and cause too many RPC, it will also grab the lock too frequently, causing other thread starving.
3. `TryReplica()`, background function for `LEADER` to try to replicate its logs to other node.
4. `SyncSnapshot()`, background function for `LEADER` to send it's snapshot to followers. Only use in 2D
5. `TryApply()`, background function to apply messages which is `commited` but is not `applied`.

#### RPC_Sender
*The RPC_senders should wrap the RPC message, send the RPC message, and handle the reply correctly. Because the RPC is synchronous, we should start a new go routine to send the RPC sometimes. And we must release to lock before long-time waiting.*

1. `TryRequestVote(term, server)`, a candidate try to send RequestVoteRPC to server.
2. `TrySendHB(term, server)`, a leader try to send an empty AppendEntriesRPC to server.
3. `TrySyncSnapshot(server)`, a leader try to send snapshot to follower. 
4. `TrySendRP(server)`, a leader try to send it's log to follower.

#### RPC_Receiver

*The RPC_Receivers should action in respond to RPC, and wrap reply message*

1. `RequestVote()`. Got a request vote from some peers. We must check the states, judge if we can grant vote for him.
2. `AppendEntries()`. Got a heartbeat or RP from some peers(May be right leader, or obsolete leader). The heartbeat should also check some state(mainly the changes of ROLE). If it's RP, we should do more operations about Logs. We can change the commitIndex after an RP's reply.
3. `InstallSnapshot()`. Got a snapshot from leader, try to apply this snapshot.

- We should check the term and the role when we send and receive RPC.
- Even in one function, we can not prove the states are not changed after we regain(release and regain) the lock. So we need to recheck some states and do the right decision.



#### Other Functions
1. `Make()`. Init a server, it should init some states, read from persistent storage, and start some go routine.
2. `Persist()`. Help function to persist some states in RAFT. If the first letter of a variable is capital variable, it should be persistent.
3. `ReadPersist()`. Read persist and restore some states.
4. `Snapshot()`. Help function for the service(upper layer on raft) to make a snapshot of service. The index is where the log can be truncated, we can only remain the last [index+1, ...] area. The snapshot is the snapshot of the service(not the raft), we need raft to save the Log & snapshot.
5. `Start()`. Service try to append a log in the RAFT. We can not prove the action will be committed. If the entry is committed, then the index will be the return value. The `Start()` should return immediately, which means we can't do some heavy logic in this function, we just simply append a log and persist it, waiting RP to replicate it. 
6. `Kill(), Killed()`. Help function for the service to kill the raft gracefully.

#### Lock

Locking is common in raft. Here are some principles.

-  If we want to read or write some states shared in different g-routine in raft, use lock.
-  If we want to send RPC, release the lock before send the RPC.
-  Attention for deadlock, especially when we need to send HB or make snapshot to self. 
-  After we regain the lock, we must check some states `unchanged`. Because it may affect our decision.

#### Persistent

- Persist the state before we affect other's decision. (A change can roll back before it is sawed by others)
- We don't lock in `persist()`, but we prove we only call `persist()`, when the variables are locked.

#### Snapshot

We should change the logic of leader-election and RP.

- If a follower has a newer snapshot then candidate, it will not vote for him. There are two solution, one is the candidate got others vote and become leader(then it will make snapshot on his own), the other is the follower become candidate and become the leader.
- Since we should prove the order of apply command. We can not re-apply a snapshot,  so we should simply ignore the old snapshot.
- We should also change the logic of Log. There are two index now, one is physical index(can be indexed directly in Log array), the other is logical index (grow forever, act as there can be infinite logs). physical index + LastIncludedIndex = logical index.
- When RP, we should compare the logical index (even a leader has longer physical index, a follower has shorter, the leader may not append logs in follower, because the follower may have a newer snapshot version, but the leader haven't made snapshot yet,)


#### Drawback

1. When using 8th intel i5 cpu, it may consume 60~80 seconds in 2D Part. Maybe we can adjust some timeout and use better machine to evaluate.
2. Although `Start()` doesn't prove the agreement of execution. But if a leader is leader now, it response to `start()`'s caller correctly, but it down to follower immediately. The log will lose. We know use very high frequency poll to let replica happen before leader's change. But it can not prevent this situation perfectly.