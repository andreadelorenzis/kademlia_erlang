-module(bootstrap_node).
-export([start_link/3]).


start_link(Name, ShellPid, IdLength) ->
    Pid = spawn_link(fun() -> init(Name, ShellPid, IdLength) end),
    log:info("Spawned bootstrap node: ~p~n", [{Name, Pid}]),
    {ok, Pid}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init(Name, ShellPid, IdLength) ->
    ID = utils:generate_node_id(IdLength),
    % Save the bootstrap info in an ETS table
    ets:insert(bootstrap_nodes, {Name, ID, self()}),
    log:info("Saved bootstrap node: ~p~n", [Name]),
    BootstrapNode = choose_bootstrap(Name),
    log:info("Chosen bootstrap node: ~p~n", [BootstrapNode]),
    node:init_node(ID, BootstrapNode, Name, ShellPid),
    ok.

choose_bootstrap(OwnName) ->
    % Select a random bootstrap node from all the recors in the ETS table (expect itself)
    AllNodes = ets:match_object(bootstrap_nodes, {'_', '_', '_'}),
    OtherNodes = [{Name, ID, Pid} || {Name, ID, Pid} <- AllNodes, Name =/= OwnName],
    case OtherNodes of
        [] -> 
            undefined;  % No other node available (first bootstrap node in the network)
        _ -> 
            {Name, ID, Pid} = lists:nth(rand:uniform(length(OtherNodes)), OtherNodes),
            {Name, ID, Pid}  
    end.
