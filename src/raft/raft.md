

### think
1. A good design is much more significant than coding, a good coding is much more significant than debugging.
2. Debugging from your own code, rather than thinking some complicated solutions.
3. Go-routine is easy for use. There are two ways to communicate between two routine, one is LOCK+POLL, the second is Channel.
4. A pretty output is good for debugging parallelism, A automatic script is good for testing. Ref: https://blog.josejg.com/debugging-pretty/. It really help me a lot.


### Design
1. Use LOCK+POLL rather than Channel. Because Pooling is more simple, but needs design timeout carefully.


### Debugging
1. `dlog` is a python3 script for present output.
2. `dtest` is a python3 script for test in parallel.
3. `cat xxx.log | dlog -c n` is to show log.
4. `dest TestXXX -p 16 -n 200 -r -v` is to testXXX, using 16 workers for 200 times. And we also print log and use race detector. The workers number is based on your PC's core.
5. `cat test_test.go| grep 2C | sed 's\(\ \g'|awk '/func/ {printf "%s ",$2;}' | xargs dtest -p 16 -v -r -n 300` is to test all in 2C, using 16 workers.

