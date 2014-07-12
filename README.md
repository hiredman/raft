# raft

A implementation of the Raft consensus algorithm written in
Clojure. This implementation has a heavy emphasis on providing an
algorithm disentangled as much as possible from concerns like network
transport.

This implementation provides a function `com.manigfeald.raft/raft`
that creates a value that describes the state of the algorithm, and a
function `com.manigfeald.raft/run-one` that steps a given state of the
algorithm to the next state.

The state of the raft algorithm is described in five records:

1. ImplementationState
   - The overall container of the rest of the state
2. Timer
   - a number representing now
   - a number representing the next timeout
   - the period between timeouts
3. IO
   - a possible message received
   - a PersistentQueue of messages to send out
4. RaftState
   - raft state common to all nodes
5. RaftLeaderState
   - raft state used by the leader node

## Why is this so weird?

You can think of it more as a model of a Raft implementation, but the
model is in clojure so it is easy enough to just wire the model in to
some transports and run it. Like wiring in to a spreadsheet model, but
without the instinctive gag reflex of wiring in to a spreadsheet.

## Usage

call `com.manigfeald.raft/raft` to get an initial state, call
`com.manigfeald.raft/run-one` to advance the state. if you want the
node you are modeling to receive a message, assoc the message in to
`[:io :message]` in the state, `[:io :out-queue]` will contain messages
the node you are modeling wants to send out. Update the value of
`[:timer :now]` to model the passage of time.

the tests have an example using core.async channels as the transport

## Generated Docs

http://ce2144dc-f7c9-4f54-8fb6-7321a4c318db.s3.amazonaws.com/raft/index.html

## See also

https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf

## What is missing

The raft paper also has snapshot generation to avoid logs growing too
large and an interesting scheme for changing cluster membership, both
of which are unimplemented.

The raft paper also talks about providing a fast path for reads that
avoids the entire log and commit process, I have left that out over
fears of screwing it up, so reads and writes both go through the
entire consensus process

## License

Copyright © 2014 Kevin Downey

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
