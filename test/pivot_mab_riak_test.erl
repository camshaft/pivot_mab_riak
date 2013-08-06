-module(pivot_mab_riak_test).

-include_lib ("eunit/include/eunit.hrl").

basic_test() ->
  App = <<"my-app">>,
  Bandit = <<"button">>,
  Arms = [<<"red">>, <<"blue">>, <<"yellow">>],

  gen_batch_sup:start_link(),
  pivot_mab_riak:start_link([{"localhost", 8087}]),
  ok = pivot_mab_riak:init(App, Bandit),

  ok = pivot_mab_riak:update(App, 0.7, [{Bandit, <<"red">>}]),
  Report = pivot_mab_riak:report(App, Bandit, Arms),
  ?debugVal(Report),
  ok.
