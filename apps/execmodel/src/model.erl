-module(model).

%% API
-export([run/2]).

-type id() :: pos_integer().
-type res() :: oo | xx.

-record(t, {
          id :: id(),
          work :: float()
         }).
-type t() :: #t{}.

-record(n, {
          id :: id(),
          perf :: float(),
          task :: undefined | t(),
          r :: undefined | res(),
          h = [] :: [{id(), float(), res()}],
          t :: undefined | float()
         }).
%% -type n() :: #n{}.

run(NS, NN) when NN =< NS ->
    Nodes = [prov() || _ <- lists:seq(1, NN)],
    STs = [st() || _ <- lists:seq(1, NS)],
    exec(Nodes, STs).

exec(Nodes0, STs0) ->
    Nodes = [ #n{id = Id, perf = P}
              || {P, Id} <- lists:zip(Nodes0, lists:seq(1, length(Nodes0))) ],
    STs = [ #t{id = Id, work = W}
            || {Id, W} <- lists:zip(lists:seq(1, length(STs0)), STs0) ],
    {Working, [], RestOfTasks} = assign(Nodes, STs),
    true = length(Working) == length(Nodes),
    loop([], RestOfTasks, Working, []).

loop(Done, [], [], Idle) ->
    {Done, Idle};
loop(_Done, _, [], _Idle) ->
    error(there_is_still_work_to_do);
loop(Done, Rest, Nodes0, Idle) ->
    Nodes1 = project(Nodes0), %% maybe computes values of r and t
    Nodes2 = lists:keysort(#n.t, Nodes1), %% ascending
    #n{t = Quantum} = hd(Nodes2), %% get closest t
    io:fwrite("q: ~p~n", [Quantum]),
    Nodes3  = [ N#n{t = T - Quantum}
                || N = #n{t = T} <- Nodes2 ], %% update time
    case reassign(Rest, Nodes3) of
        {NewDone, [], Nodes4, Idle1} ->
            loop(NewDone ++ Done, [], Nodes4, Idle1 ++ Idle);
        {NewDone, Rest1, Nodes4, []} ->
            loop(NewDone ++ Done, Rest1, Nodes4, Idle)
    end.

epsilon() ->
    0.000001.

reassign(Tasks, Nodes) ->
    {Compl, TOs, BusyProviders, FreeProviders} = finalize_execs(Nodes),
    {Working, Idle, Rest} = assign(FreeProviders, TOs++Tasks),
    {Compl, Rest, Working++BusyProviders, Idle}.

finalize_execs(Nodes) ->
    E = epsilon(),
    IsFree =
        fun(#n{t = undefined}) -> true;
           (#n{t = T}) when T < E -> true;
           (_) -> false
        end,
    {Finishing, Working} = lists:partition(IsFree, Nodes),
    R = [ {{R == xx, T}, N#n{task = undefined, r = undefined, t = undefined}}
          || N = #n{task = T, r = R} <- Finishing ],
    {ComplExecs, FreeProviders} = lists:unzip(R),
    {Timeouted0, Finished0} = lists:partition(fun(X) -> element(1, X) end, ComplExecs),
    {_, Timeouted} = lists:unzip(Timeouted0),
    {_, Finished} = lists:unzip(Finished0),
    {Finished, Timeouted, Working, FreeProviders}.

project(Nodes) ->
    lists:map(fun schedule_work/1, Nodes).

schedule_work(N = #n{perf = Perf, task = #t{id = TId, work = Work},
                     t = undefined, h = H}) ->
    T = Work / Perf,
    case T > st_deadline() of
        true ->
            NH = {TId, Work, xx},
            N#n{t = st_deadline(), r = xx, h = [NH | H]};
        false ->
            NH = {TId, Work, oo},
            N#n{t = T, r = oo, h = [NH | H]}
    end;
schedule_work(N = #n{task = #t{id = _Id}}) ->
    N.

st_deadline() ->
    1.0.

assign(Nodes, STs) ->
    assign(Nodes, STs, []).
assign([HN | TN], [HST | TST], Busy) ->
    assign(TN, TST, [HN#n{task = HST} | Busy]);
assign([], STs, Busy) ->
    {Busy, [], STs};
assign(Idlers, [], Busy) ->
    {Busy, Idlers, []}.


%% subtask is modeled as amount of CPU cycles needed to compute it
st() ->
    max(0, (rand:normal() + 1)).

%% provider is modeled as its computational power: [1,0..2,0]
prov() ->
    rand:uniform() + 1.
