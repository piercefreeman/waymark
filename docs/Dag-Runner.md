Now that we have the DAG defined, we want to introduce the notion of "runnable actions". these are the actions that will actually be picked up by workers. they need to fully scope the work that they need to be done so workers can pick them up...

we want to have a separately isolated "queue" of runnable actions in memory that we can write to.

actions will generate at least one runnable action, but sometimes these are:

"inline" within the runner itself, like AST evaluation of terms within our IR

delegated to a worker, in the case of proper @actions which are always pushed

I suggest you split these cases into two different "handler" function that will return the results of this single, then commonly handle the logic of pushing out the data to the dependent nodes in the DAG and determining which node to execute next

pushing metadata should happen in a separate "runnable_action_data". should just be one row per node that has to push values to it.

(for now we're simulating what will happen in a db within memory, so keep that in mind as you write your data structures)

the runner class should handle picking things up from the queue, resolving the ground truth DAG node it's corresponding to, execute it, then handle the results, then queue the proper next action(s). in the case of a spread these will be multiple actions

