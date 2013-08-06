-module(pivot_mab_riak).

%% -behaviour(pivot_mab_db).

-export([register/3]).
-export([list/0]).
-export([report/1]).
-export([update/2]).

%% gen_server.

-export([start_link/1]).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(BANDIT_BUCKET, <<"pivot:bandits">>).
-define(ARMS_BUCKET(Bandit), <<"pivot:arms:", Bandit/binary>>).
-define(STATE_BUCKET(Bandit), <<"pivot:state:", Bandit/binary>>).
-define(SCORE_RESOLUTION, 1000).

-record (state, {
  conns,
  hosts
}).

register(Name, Arms, BanditEnabled) ->
  % Init the bucket to allow multi
  ok = gen_server:call(?MODULE, {init, ?STATE_BUCKET(Name)}),

  % Create an empty key in our buckets list
  % TODO once riak supports crdt sets we will use that instead
  ok = put(?BANDIT_BUCKET, Name, boolean_to_binary(BanditEnabled)),

  % Store the arms
  % TODO once riak supports crdt sets we will use that instead
  ArmsBucket = ?ARMS_BUCKET(Name),
  % register/3 doesn't get called that much so we won't batch it
  Results = [put(ArmsBucket, Arm, boolean_to_binary(Enabled)) || {Arm, Enabled} <- Arms],
  check_results(Results).

list() ->
  % TODO once riak supports crdt sets we will use that instead
  list_keys(?BANDIT_BUCKET).

report(Bandit) ->
  % TODO once riak supports crdt sets we will use that instead
  % TODO get the arm values so we can tell if the arms are enabled
  case list_keys(?ARMS_BUCKET(Bandit)) of
    {ok, Arms} ->
      Batch = fetch(?STATE_BUCKET(Bandit), batch:create(), Arms),
      Presenter = presenterl:create(),
      batch:exec(Batch, [Presenter]),
      do_report(presenterl:encode(Presenter), Arms, []);
    Error ->
      Error
  end.

do_report(_, [], Acc) ->
  Acc;
do_report(Results, [Arm|Arms], Acc) ->
  ArmResults = proplists:get_all_values(Arm, Results),
  State = {Arm, {fast_key:get(count, ArmResults), fast_key:get(score, ArmResults)}},
  do_report(Results, Arms, [State|Acc]).

fetch(_, Batch, []) ->
  Batch;
fetch(Bucket, Batch, [Arm|Arms]) ->
  batch:push(fun(P) ->
    Value = case counter_val(Bucket, <<Arm/binary,":count">>) of
      {ok, Val} ->
        Val;
      {error, notfound} ->
        0;
      Error ->
        Error
    end,
    P ! [{Arm, {count, Value}}]
  end, Batch),

  batch:push(fun(P) ->
    Value = case counter_val(Bucket, <<Arm/binary,":score">>) of
      {ok, Val} ->
        Val / ?SCORE_RESOLUTION;
      {error, notfound} ->
        0;
      Error ->
        Error
    end,
    P ! [{Arm, {score, Value}}]
  end, Batch),

  fetch(Bucket, Batch, Arms).

update(Bandit, Patches) ->
  Batch = patch(?STATE_BUCKET(Bandit), batch:create(), Patches),

  Presenter = presenterl:create(),
  batch:exec(Batch, [Presenter]),
  Results = presenterl:encode(Presenter),
  check_results(Results).

patch(_, Batch, []) ->
  Batch;
patch(Bucket, Batch, [{Arm, ScoreIncr}|Patches]) ->
  patch(Bucket, Batch, [{Arm, 1, ScoreIncr}|Patches]);
patch(Bucket, Batch, [{Arm, CountIncr, ScoreIncr}|Patches]) ->
  batch:push(fun(P) ->
    P ! [counter_incr(Bucket, <<Arm/binary,":count">>, CountIncr)]
  end, Batch),

  batch:push(fun(P) ->
    P ! [counter_incr(Bucket, <<Arm/binary,":score">>, trunc(ScoreIncr * ?SCORE_RESOLUTION))]
  end, Batch),

  patch(Bucket, Batch, Patches).

check_results([]) ->
  ok;
check_results([ok|Results]) ->
  check_results(Results);
check_results([Error|_]) ->
  Error.

boolean_to_binary(true) ->
  <<1>>;
boolean_to_binary(_) ->
  <<0>>.

% TODO replace with a connection pooler

put(Bucket, Key, Value) ->
  gen_server:call(?MODULE, {put, riakc_obj:new(Bucket, Key, Value)}).

list_keys(Bucket) ->
  gen_server:call(?MODULE, {list_keys, Bucket}).

counter_val(Bucket, Key) ->
  gen_server:call(?MODULE, {counter_val, Bucket, Key}).

counter_incr(Bucket, Key, Amount) ->
  gen_server:call(?MODULE, {counter_incr, Bucket, Key, Amount}).

%% gen_server.

start_link(Hosts) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Hosts, []).

%% TODO support pooling multiple connections with connection blocking
init([{Host, Port}|_] = Hosts) ->
  {ok, Conn} = riakc_pb_socket:start_link(Host, Port, [auto_reconnect]),
  {ok, #state{hosts = Hosts, conns = [Conn]}}.

handle_call(stop, _, State) ->
  {stop, normal, stopped, State};
handle_call({init, Bucket}, _, State = #state{conns = [Conn|_]}) ->
  Result = riakc_pb_socket:set_bucket(Conn, Bucket, [{allow_mult, true}]),
  {reply, Result, State};
handle_call({put, Obj}, _, State = #state{conns = [Conn|_]}) ->
  Result = riakc_pb_socket:put(Conn, Obj),
  {reply, Result, State};
handle_call({list_keys, Bucket}, _, State = #state{conns = [Conn|_]}) ->
  Results = riakc_pb_socket:list_keys(Conn, Bucket),
  {reply, Results, State};
handle_call({counter_incr, Bucket, Key, Amount}, _, State = #state{conns = [Conn|_]}) ->
  Result = riakc_pb_socket:counter_incr(Conn, Bucket, Key, Amount),
  {reply, Result, State};
handle_call({counter_val, Bucket, Key}, _, State = #state{conns = [Conn|_]}) ->
  Result = riakc_pb_socket:counter_val(Conn, Bucket, Key),
  {reply, Result, State};
handle_call(_, _, State) ->
  {reply, ignore, State}.

handle_cast(_, State) ->
  {noreply, State}.

handle_info(_, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
