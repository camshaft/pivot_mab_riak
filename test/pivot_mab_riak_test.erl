-module(pivot_mab_riak_test).

-include_lib ("eunit/include/eunit.hrl").

basic_test() ->
  Bandit = <<"button">>,

  gen_batch_sup:start_link(),
  pivot_mab_riak:start_link([{"localhost", 8087}]),
  ok = pivot_mab_riak:register(Bandit, [{<<"red">>, true}, {<<"blue">>, true}, {<<"yellow">>, true}], false),

  ok = pivot_mab_riak:update(Bandit, [{<<"red">>, 0.7}, {<<"yellow">>, 3, 0.2}, {<<"blue">>, 2, 0.6}]),
  Report = pivot_mab_riak:report(Bandit),
  ?debugVal(Report),
  ?debugVal(pivot_mab_riak:list()),
  ok.
