-module(model).

%%% ASSUMPTIONS:
%%% * task is embarrassingly parallel
%%% * amount of work in each of subtasks is unknown to providers and requestor
%%% * computational power of providers is unknown to requestor
%%% * providers don't earn anything by timeouting subtasks
%%% * all of the payload is distributed before the computation
%%%   => provider can start next subtask directly after finishing previous

%%%%%%%%%%%%%%%%
%%%%% API %%%%%%
%%%%%%%%%%%%%%%%
-export([run/2, stats/1]).

-type id() :: pos_integer().
-type res() :: oo | xx. %% oo - completed; xx - timeouted
-type work() :: float(). %% normalized amount of work per subtask
-type perf() :: float().
-type cpu_interval() :: float().

%% history entry - represents an attempt of provider to compute a subtask
-record(h, {
          id :: id(),
          perf = 0.0 :: perf(),
          work = 0.0 :: work(),
          cpu :: cpu_interval(),
          res :: res()
         }).
-type h() :: #h{}.

%% subtask
-record(t, {
          id :: id(),
          work :: work(),
          h = [] :: [h()] %% history of the subtask
         }).
-type t() :: #t{}.

%% provider
-record(n, {
          id :: id(),
          perf :: perf(), %% performance of the node
          task :: undefined | t(), %% current task
          r :: undefined | res(), %% result of current task
          h = [] :: [h()], %% history of the node
          t :: undefined | float() %% time of end of current computation
         }).
-type n() :: #n{}.

-spec run(NS, NN) -> {[t()], [n()]} when
      NS :: pos_integer(), %% number of subtasks
      NN :: pos_integer(). %% number of providers
run(NS, NN) when NN =< NS ->
    Nodes = [prov() || _ <- lists:seq(1, NN)],
    STs = [st() || _ <- lists:seq(1, NS)],
    exec(Nodes, STs).

-spec stats({[t()], [n()]}) -> map().
stats({Subtasks, Nodes}) ->
    Finished = length(lists:filter(fun(T) -> st_status(T) == completed end, Subtasks)),
    TOs = lists:flatten([ [ 1 || #h{res=Res} <- Hs, Res =:=xx ] || #t{h = Hs} <- Subtasks ]),
    #{wall => wall_time(Nodes),
      cpu => cpu_time(Subtasks),
      best => best(Nodes),
      worst => worst(Nodes),
      finished => Finished,
      failed => length(Subtasks) - Finished,
      timeouts => length(TOs),
      req_time => req_time(Subtasks),
      total => length(Subtasks)
     }.

%%%%%%%%%%%%%%%%%%%%%%%%%%
%% generators and rules %%
%%%%%%%%%%%%%%%%%%%%%%%%%%

%% RULE: on every timeout duplicate subtask deadline
-spec st_timeout(t()) -> cpu_interval().
st_timeout(#t{h = H}) ->
    1.5*(max(1.0, math:pow(2, length(H)))).

%% RULE: retry subtask at most once
st_status(#t{h = [#h{res=oo} | _]}) ->
    completed;
st_status(#t{h = H}) when length(H) < 2 ->
    timeouted;
st_status(#t{h = H}) when length(H) >= 2 ->
    failed.

%% RULE: place subtasks that time-outed at the end of the subtasks queue
handle_timeouted(Tasks, TOs) ->
    Tasks ++ TOs.

%% generator: subtask is modeled as amount of CPU time needed to compute it
%%            can be seen as number of hours for machine of performance 1.0
st() ->
    max(0.0, (rand:normal() + 0.5)).

%% generator: provider is modeled as its computational power: [0,5..1,5]
prov() ->
    rand:uniform() + 0.5.

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
              N1 = N#n{task = undefined, r = undefined, t = undefined},
              {T, N1}
          end || N = #n{task = T} <- Finishing ],
    {ComplExecs, FreeProviders} = lists:unzip(R),
    {Finished, Other} = lists:partition(fun(T) -> st_status(T) == completed end, ComplExecs),
    {Timeouted, Failed} = lists:partition(fun(T) -> st_status(T) == timeouted end, Other),
    {Finished, Timeouted, Failed, Working, FreeProviders}.

project(Nodes) ->
    lists:map(fun schedule_work/1, Nodes).

schedule_work(N = #n{perf = Perf, task = Task, id = NId,
                     t = undefined, h = NodeHistory}) ->
    #t{id = TId, work = Work, h = TaskHistory} = Task,
    T = Work / Perf,
    Timeout = st_timeout(Task),
    {Res, CPU}
        = case T > Timeout of
              true ->
                  {xx, Timeout};
              false ->
                  {oo, T}
          end,
    NH = #h{id=NId, work=Work, cpu=CPU, res=Res},
    TH = #h{id=TId, perf=Perf, cpu=CPU, res=Res},
    Task1 = Task#t{h = [TH | TaskHistory]},
    N#n{t = CPU, task = Task1, r = Res, h = [NH | NodeHistory]};
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

best(Nodes) when is_list(Nodes) ->
    F = fun2ordering(fun(#n{h=Hs}) -> length(Hs) end),
    node_summary(hd(lists:reverse(lists:sort(F, Nodes)))).

worst(Nodes) when is_list(Nodes) ->
    F = fun2ordering(fun(#n{h=Hs}) -> length(Hs) end),
    node_summary(hd(lists:sort(F, Nodes))).

fun2ordering(F) ->
    fun(A, B) ->
            F(A) =< F(B)
    end.

node_summary(#n{id=Id,perf=Perf,h=H}) ->
    HF = lists:filter(fun(#h{res=R}) -> R == xx end, H),
    #{id => Id, perf => Perf, done => length(H), failed => length(HF)}.

cpu_time(Subtasks) ->
    Sums = [ lists:sum([ CpuI || #h{cpu=CpuI} <- Hs ]) || #t{h = Hs} <- Subtasks ],
    lists:sum(Sums).

wall_time(Nodes) ->
    Sums = [ lists:sum([ CpuI || #h{cpu=CpuI} <- Hs ]) || #n{h = Hs} <- Nodes ],
    lists:max(Sums).

req_time(Subtasks) ->
    lists:sum([ W / 1.0 || #t{work = W} <- Subtasks ]).
