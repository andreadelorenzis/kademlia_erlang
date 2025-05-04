-module(bootstrap_node).
-export([start_link/1]).


start_link(Name) ->
    Pid = spawn_link(fun() -> init(Name) end),
    log:info("Spawned bootstrap node: ~p~n", [{Name, Pid}]),
    {ok, Pid}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init(Name) ->
    ID = utils:generate_node_id_160bit(),

    % Save the bootstrap info in an ETS table
    ets:insert(bootstrap_nodes, {Name, ID, self()}),
    BootstrapNode = choose_bootstrap(Name),
    node:init_node(ID, BootstrapNode, Name, self()).


choose_bootstrap(OwnName) ->
    % Select a random bootstrap node from all the recors in the ETS table (expect itself)
    AllNodes = ets:match_object(bootstrap_nodes, {'_', '_', '_'}),
    OtherNodes = [{Name, ID, Pid} || {Name, ID, Pid} <- AllNodes, Name =/= OwnName],
    case OtherNodes of
        [] -> 
            undefined;  % No other node available (first bootstrap node in the network)
        _ -> 
            {_Name, ID, Pid} = lists:nth(rand:uniform(length(OtherNodes)), OtherNodes),
            {ID, Pid}  
    end.
