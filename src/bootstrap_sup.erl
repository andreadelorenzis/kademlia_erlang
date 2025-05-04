-module(bootstrap_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).


start_link() ->
    supervisor:start_link({global, ?MODULE}, ?MODULE, []).


init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1,
    MaxTime = 5,
    SupFlags = {RestartStrategy, MaxRestarts, MaxTime},
    {ok, {SupFlags, []}}.