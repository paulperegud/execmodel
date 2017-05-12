execmodel
=====

Quick and dirty simulator of execution of set of subtasks by group of machines (providers)

Build
-----

    $ ./rebar3 compile


Run
-----

    $ ./rebar3 release && ./rebar3 dialyzer && ./_build/default/rel/execmodel/bin/execmodel console
    (execmodel@host)1> f(), {T, N} = model:run(100, 10), model:stats({T, N}).
