-module(pivot_mab_riak).

%% -behaviour(pivot_mab_db).

-export([init/2]).
-export([report/3]).
-export([update/3]).

%% gen_server.

-export([start_link/1]).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(STATE_BUCKET(App, Bandit), <<"pivot:state:", App/binary, ":", Bandit/binary>>).
-define(SCORE_RESOLUTION, 1000).

-record (state, {
  conns,
  hosts
}).

init(App, Bandit) ->
  % Init the bucket to allow multi
  gen_server:call(?MODULE, {init, ?STATE_BUCKET(App, Bandit)}).

report(App, Bandit, Arms) ->
  Batch = fetch(?STATE_BUCKET(App, Bandit), batch:create(), Arms),
  Presenter = presenterl:create(),
  batch:exec(Batch, [Presenter]),
  do_report(presenterl:encode(Presenter), Arms, []).

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

do_report(_, [], Acc) ->
  Acc;
do_report(Results, [Arm|Arms], Acc) ->
  ArmResults = proplists:get_all_values(Arm, Results),
  State = {Arm, {fast_key:get(count, ArmResults), fast_key:get(score, ArmResults)}},
  do_report(Results, Arms, [State|Acc]).

update(App, Reward, Assignments) ->
  Batch = update(App, batch:create(), Reward, Assignments),

  Presenter = presenterl:create(),
  batch:exec(Batch, [Presenter]),
  Results = presenterl:encode(Presenter),
  check_results(Results).

update(_, Batch, _, []) ->
  Batch;
update(App, Batch, Reward, [{Bandit, Arm}|Assignments]) ->
  Bucket = ?STATE_BUCKET(App, Bandit),
  batch:push(fun(P) ->
    P ! [counter_incr(Bucket, <<Arm/binary,":count">>, 1)]
  end, Batch),

  batch:push(fun(P) ->
    P ! [counter_incr(Bucket, <<Arm/binary,":score">>, trunc(Reward * ?SCORE_RESOLUTION))]
  end, Batch),

  update(App, Batch, Reward, Assignments).

check_results([]) ->
  ok;
check_results([ok|Results]) ->
  check_results(Results);
check_results([Error|_]) ->
  Error.

% TODO replace with a connection pooler

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
