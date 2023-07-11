

#### drawback
1. The snapshot's size may exceed. That's because we need `DuplicatedMap` to sign the `applied` command.
2. There may be data race on `ck.leader`, but is okay for correctness.
3. The TestSpeed can't satisfy requirement.
4. The GC may affect correctness of linear, we don't know why now...
5. May deadlock sometimes..