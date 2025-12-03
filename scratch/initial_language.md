create a new python script that will implement our new programming language! right now we
just want to define the general structures like in an IR and a host of examples that show off
the language feature flows. we want to implement: every variable is immutalbe and needs to be
assigned with a = . This includes commands like my_list = my_list + [testing] ... dicts same
thing my_dict = my_dict + ["key":"new"] ... & python blocks that define input/output vars &
fucntions that define input/output vars. there are no local functions that pass through
closures and no local functions nested in one another. Also include being able to spread over
some variable. actions should be a first class primitive. add list and dict key based access.
to get some inspo you can check our our current "scratch" dir... but this should be a
standalone file that is runnable with uv. once you have added the examples, you need to write
a simpler lexer for it (keep this incapsolated in a class def). I suggest you break this up
into checkpointable steps where you have inline pytest cases that pass before moving on to the
 next. for loops should also be supported. they should always iterate a single variable over a
 list of them, and the contents of it HAVE to be a function so we can more explicitly define
input and output values. functions should be able to return multiple variables via a list
structure (that we can unpack python style into local variables)...
