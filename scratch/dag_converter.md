Once you are done with that, your job is to make a DAG converter. It should accept in a full IR representation and create nodes.

here are the rules:

Two edge types:
	— state machine destination
	— data flow

State machine destinations define the possible nodes to travel to after this node is completed, perhaps given some gating criteria to tie-break between them. (src, dst) means that dst should follow src in the execution roder.

Data flow destinatinos determine where to push some variables that were set by this node. (src, dst) means that src is pushing some data in to dst. You’ll want to parameterize these edges by which variable of the output is being pushed.

“for” should be a single node, with output state machine edges for “continue” (first node contained within it) and “done”

Any variables needed by the function within the “for” loop should be passed FROM the for loop. Which means any nodes that need to pass values INTO the for loop should just pass them to the for loop head.

The class you build to do this should have a .visualize() which should popup some matplotlib graph of the dependencies that we have here in some tree like structure. solid lines for the state machine dependencies and dotted lines for the data flow.

NOTE: we should only PUSH data to the most recent trailing node that DOESN’T modify the variable. If a variable name is modified downstream, downstream nodes should rely on the modified node value
