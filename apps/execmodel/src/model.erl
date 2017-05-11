-module(model).

%%%%%%%%%%%%%%%%
%%%%% API %%%%%%
%%%%%%%%%%%%%%%%
-export([run/2]).

-type id() :: pos_integer().
-type res() :: oo | xx. %% oo - completed; xx - timeouted
-type work() :: float().
-type perf() :: float().

-record(t, {
          id :: id(),
          work :: work(),
          h = [] :: [{id(), perf(), res()}]
         }).
-type t() :: #t{}.

-record(n, {
          id :: id(),
          perf :: perf(), %% performance of the node
          task :: undefined | t(), %% current task
          r :: undefined | res(), %% result of current task
          h = [] :: [{id(), work(), res()}], %% history
          t :: undefined | float() %% time of end of current computation
         }).

run(NS, NN) when NN =< NS ->
    Nodes = [prov() || _ <- lists:seq(1, NN)],
    STs = [st() || _ <- lists:seq(1, NS)],
    exec(Nodes, STs).

%%%%%%%%%%%%%%%%%%%%%%%%%%
%% generators and rules %%
%%%%%%%%%%%%%%%%%%%%%%%%%%

%% RULE: for second try set subtask timeout to 2.0
st_timeout(#t{h = H}) ->
    max(1.0, math:pow(2, length(H))).

%% RULE: retry subtask at most once
st_status(#t{h = [{_, _, oo} | _]}) ->
    completed;
st_status(#t{h = H}) when length(H) < 2 ->
    timeouted;
st_status(#t{h = H}) when length(H) >= 2 ->
    failed.

%% RULE: place subtasks that time-outed at the end of the subtasks queue
handle_timeouted(Tasks, TOs) ->
    Tasks ++ TOs.

%% generator: subtask is modeled as amount of CPU time needed to compute it
st() ->
    max(0, (rand:normal() + 1)).

%% generator: provider is modeled as its computational power: [1,0..2,0]
prov() ->
    rand:uniform() + 1.

%%%%%%%%%%%%%%%
%% internals %%
%%%%%%%%%%%%%%%

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
    Nodes3  = [ N#n{t = T - Quantum}
                || N = #n{t = T} <- Nodes2 ], %% update time
    case reassign(Rest, Nodes3) of
        {NewDone, Failed, [], Nodes4, Idle1} ->
            loop(Failed ++ NewDone ++ Done, [], Nodes4, Idle1 ++ Idle);
        {NewDone, Failed, Rest1, Nodes4, []} ->
            loop(Failed ++ NewDone ++ Done, Rest1, Nodes4, Idle)
    end.

epsilon() ->
    0.000001.

reassign(Tasks, Nodes) ->
    {Compl, TOs, Failed, BusyProviders, FreeProviders} = finalize_execs(Nodes),
    {Working, Idle, Rest} = assign(FreeProviders, handle_timeouted(Tasks, TOs)),
    {Compl, Failed, Rest, Working++BusyProviders, Idle}.

finalize_execs(Nodes) ->
    E = epsilon(),
    IsFree =
        fun(#n{t = undefined}) -> true;
           (#n{t = T}) when T < E -> true;
           (_) -> false
        end,
    {Finishing, Working} = lists:partition(IsFree, Nodes),
    R = [ begin
              #t{h = H} = T,
              T1 = T#t{h = [{NId, Perf, R} | H]},
              N1 = N#n{task = undefined, r = undefined, t = undefined},
              {T1, N1}
          end || N = #n{id = NId, perf = Perf, task = T, r = R} <- Finishing ],
    {ComplExecs, FreeProviders} = lists:unzip(R),
    {Finished, Other} = lists:partition(fun(T) -> st_status(T) == completed end, ComplExecs),
    {Timeouted, Failed} = lists:partition(fun(T) -> st_status(T) == timeouted end, Other),
    {Finished, Timeouted, Failed, Working, FreeProviders}.

project(Nodes) ->
    lists:map(fun schedule_work/1, Nodes).

schedule_work(N = #n{perf = Perf, task = Task,
                     t = undefined, h = H}) ->
    #t{id = TId, work = Work} = Task,
    T = Work / Perf,
    case T > st_timeout(Task) of
        true ->
            NH = {TId, Work, xx},
            N#n{t = st_timeout(Task), r = xx, h = [NH | H]};
        false ->
            NH = {TId, Work, oo},
            N#n{t = T, r = oo, h = [NH | H]}
    end;
schedule_work(N = #n{task = #t{id = _Id}}) ->
    N.

assign(Nodes, STs) ->
    assign(Nodes, STs, []).
assign([HN | TN], [HST | TST], Busy) ->
    assign(TN, TST, [HN#n{task = HST} | Busy]);
assign([], STs, Busy) ->
    {Busy, [], STs};
assign(Idlers, [], Busy) ->
    {Busy, Idlers, []}.

