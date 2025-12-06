# DAG Conversion

Once we have an IR representation, our job is to make a full DAG. It should accept in a full IR representation and create nodes. This DAG conversion is non trivial, because there are some elements of control flow (like spread-gather operations and for loops) that don't typically fit in standard graphs.

Two edge types:
	— state machine destination
	— data flow

State machine destinations define the possible nodes to travel to after this node is completed, perhaps given some gating criteria to tie-break between them. (src, dst) means that dst should follow src in the execution roder.

Data flow destinatinos determine where to push some variables that were set by this node. (src, dst) means that src is pushing some data in to dst. You’ll want to parameterize these edges by which variable of the output is being pushed.

“for” should be a single node, with output state machine edges for “continue” (first node contained within it) and “done”

Any variables needed by the function within the “for” loop should be passed FROM the for loop. Which means any nodes that need to pass values INTO the for loop should just pass them to the for loop head.

The class you build to do this should have a .visualize() which should popup some matplotlib graph of the dependencies that we have here in some tree like structure. solid lines for the state machine dependencies and dotted lines for the data flow.

"spread" should be implemented as the action of interest, then a node after it acting as an aggregator. reason being is we want to allow each action to finsih in order that it finsihes and just write "action_{i}" to the results payload for the next node without us having to block to modify a list.

NOTE: we should only PUSH data to the most recent trailing node that DOESN’T modify the variable. If a variable name is modified downstream, downstream nodes should rely on the modified node value


## spread-gather

TODO: Discussion on frontier nodes. Link to specs/unified-readiness-model.md.

## for loops

In a sense I'm playing fast and loose with the acronym DAG. Our graphs can be cyclic because of our for loop support. For loops
in contrast to spread and gather have to happen syncronously, since they need to build up the values that.

We just set up the DAG so: the for loop head pushes the current i value to the first node in the loop, which in
turn has a data node going back to the head to indicate the just completed i. then we make the head of the for
loop into a frontier node. Then at runtime we just run a guard against the value of i based on the
known full iteration values (which itself should be a var that's pushed into the inbox)?
