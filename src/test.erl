-module(test).
-export([prepare_network/2, 
         ping/2, 
         store/2, 
         find_node/2,
         find_value/2, 
         refresh_node/1,
         print_buckets/1,
         print_buckets_with_distance/1,
         print_buckets_with_distance/2,
         print_storage/1, 
         run_lookup_measurements/0,
         run_measurements_with_failure/0,
         run_join_measurements/0]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Creates a network with a given number of nodes and bootstrap nodes and
%% some predefined options.
prepare_network(BootstrapCount, NodeCount) ->
    log:start(),
    Options = #{
        k_param => 10,
        alpha_param => 2,
        id_byte_length => 2,
        republish_interval => 30000,
        expiration_interval => 86400000,
        refresh_interval => 3600000,
        timeout_interval => 2000,
        log_level => important,
        refresh => true
    },
    prepare_network(BootstrapCount, NodeCount, Options),
    ok.


%% Sends a PING from SenderPid to ReceiverPid.
ping(SenderPid, ReceiverPid) ->
    SenderPid ! {'ping_request', ReceiverPid, self()},
    receive
        alive -> alive;
        dead -> dead
    after 10000 -> 
        dead
    end.    


%% Tells NodePid to store Value on the K closest nodes.
store(NodePid, Value) ->
    NodePid ! {store_request, Value, self()},
    receive
        {store_response, ok, AckNodes} -> 
            utils:print_nodes(AckNodes, "Store ACKs: ", debug),
            {store_response, AckNodes}
    after 5000 -> timeout
    end.


%% Tells NodePid to find the K closest nodes to ID.
find_node(NodePid, ID) ->
    NodePid ! {find_node_request, ID, self()},
    receive
        {find_node_response, KClosest} -> 
            % utils:print_nodes(KClosest, "K closest nodes: ", important),
            {find_node_response, KClosest}
    after 5000 -> timeout
    end.


%% Tells NodePid to find the value corresponding to Key.
find_value(NodePid, Key) ->
    NodePid ! {find_value_request, Key, self()},
    receive
        {find_value_response, Entry, Hops} when Entry =/= not_found -> 
            {find_value_response, Entry, Hops};
        {find_value_response, not_found} -> 
            {find_value_response, not_found}
    after 2000 -> timeout
    end.


%% Refreshes the buckets of Node.
refresh_node(Node) ->
    Self = self(),
    {_, _, Pid} = Node, 
    Pid ! {refresh_buckets, Self},
    wait_for_refresh(Node).


%% Prints the buckets of NodePid.
print_buckets(NodePid) ->
    NodePid ! {get_buckets_request, self()},
    receive
        {buckets_dump, Buckets, {NodeName, _NodeId, _NodePid}} ->
            utils:print_buckets(NodeName, NodePid, Buckets, important)
    after 5000 ->
        log:error("Timeout getting buckets for ~p", [NodePid])
    end.


%% Prints the buckets of Node with distance from its ID.
print_buckets_with_distance(Node) ->
    print_buckets_with_distance(Node, undefined).

%% Prints the buckets of Node with distance from its ID and TargetID.
print_buckets_with_distance(Node, TargetID) ->
    {NodeName, NodeId, NodePid} = Node,
    NodePid ! {get_buckets_request, self()},
    receive
        {buckets_dump, Buckets, _} ->
            case TargetID of
                undefined ->
                    utils:print_buckets_with_distance(NodeId, NodeName, 
                        NodePid, Buckets, important);
                _ ->
                    utils:print_buckets_with_distance(NodeId, NodeName, 
                        NodePid, Buckets, TargetID, important)
            end
    after 5000 ->
        log:error("Timeout getting buckets for ~p", [NodePid])
    end.


%% Prints the full storage of NodePid.
print_storage(NodePid) ->
    NodePid ! {get_storage_request, self()},
    receive
        {storage_dump, Storage, NodeName} ->
            utils:print_storage(NodeName, NodePid, Storage, important)
    after 5000 ->
        log:error("Timeout getting storage for ~p", [NodePid])
    end.


%% Runs a simulation to measure average lookup time.
run_lookup_measurements() ->
    process_flag(trap_exit, true),
    log:start(),
    log:set_level_global(important),
    log:clean_console(),

    % Measure average lookup time wrt number of nodes
    % NodeCounts = [8, 16, 32, 128, 256, 512, 1024, 2048, 4096],
    NodeCounts = [8, 16, 32],
    BootstrapCount = 5,
    LookupsPerRun = 100,
    Results = lists:map(
        fun(NodeCount) ->

            {AvgTimes, AvgHops} = measure_avg_random_lookup(LookupsPerRun, 
                BootstrapCount, NodeCount),
            {NodeCount, AvgTimes, AvgHops}
        end,
        NodeCounts
    ),

    % Print final results
    log:clean_console(),
    log:important("~n~n----------- RESULTS AVERAGE LOOKUP -----------~n"),
    [begin log:raw(important, "Node count: ~p, Avg lookup time: ~.2f ms, Avg hops: ~p", 
        [NodeCount, AvgTime, AvgHops]) end
     || {NodeCount, AvgTime, AvgHops} <- Results],

    ok.


%% Runs a simulation to measure average lookup time and node failure rate 
%% with up to K-1 failed nodes.
run_measurements_with_failure() ->
    process_flag(trap_exit, true),
    log:start(),
    log:set_level_global(important),
    log:clean_console(),

    % Measure average lookup time wrt to K closest nodes failure (up to K-1)
    NodeCount = 500,
    BootstrapCount = 5,
    LookupsPerRun = 100,
    Results = measure_avg_random_lookup_with_failure(LookupsPerRun, 
        BootstrapCount, NodeCount),

    % Print final results
    log:clean_console(),
    log:important("~n~n----------- RESULTS AVERAGE LOOKUP WITH NODES FAILURE -----------~n"),
    [begin 
        case Status of
            fail -> log:raw(important, "Failed count: ~p, LOOKUP FAILED", 
                [FailedCount]);
            _ ->
                log:raw(important, "Failed count: ~p, Avg lookup time: ~.2f ms, Avg hops: ~p, Total attempts: ~p", 
                [FailedCount, AvgTime, AvgHops, TotalAttempts]) 
        end
     end
    || {FailedCount, AvgTime, AvgHops, TotalAttempts, Status} <- Results],

    ok.


%% Runs a simulation to measure average join time.
run_join_measurements() ->
    process_flag(trap_exit, true),
    log:start(),
    log:set_level_global(important),
    log:clean_console(),

    % Configuration parameters
    NodeCounts = [8, 16, 32, 64, 128, 256, 512, 1024, 2048],  
    BootstrapCount = 5,                      
    JoinsPerConfiguration = 20,              
    
    % Run measurements for each network size
    Results = lists:map(
        fun(NodeCount) ->
            log:important("~n~n----------- MEASURING JOIN TIME FOR ~p NODES -----------~n", [NodeCount]),
            AvgJoinTime = measure_avg_join_time(NodeCount, BootstrapCount, 
                JoinsPerConfiguration),
            {NodeCount, AvgJoinTime}
        end,
        NodeCounts
    ),

    % Print results
    log:clean_console(),
    log:important("~n~n----------- RESULTS JOIN TIME MEASUREMENTS -----------~n"),
    [begin 
        log:raw(important, "Network size: ~p nodes, Avg join time: ~.2f ms", 
            [NodeCount, AvgJoinTime]) 
     end
     || {NodeCount, AvgJoinTime} <- Results],

    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

prepare_network(BoostrapCount, NodeCount, Options) ->
    log:info("Starting the network with ~p BOOTSTRAP nodes ----", [BoostrapCount]),
    {ok, SupPid} = node:start_network(Options),

    log:info("Creation of ~p BOOTSTRAP NODES", [BoostrapCount]),
    BootstrapNodes = create_bootstrap_nodes(BoostrapCount),

    log:info("Creation of ~p NODES", [NodeCount]),
    Refresh = maps:get(refresh, Options, false),
    log:info("REFRESH ACTIVE: ~p", [Refresh]),
    Nodes = create_nodes(NodeCount, Refresh),

    log:info("~p NODES CREATED AND INITIALIZED", [length(Nodes)]),

    log:info("NETWORK PREPARATION COMPLETE."),

    % Select random lookup node for lookup testing on random values
    RandomNodeIndex = rand:uniform(length(Nodes)),
    RandomNode = lists:nth(RandomNodeIndex, Nodes),

    utils:print_nodes(Nodes, "REACHABLE NODES: ", important),
    log:important("RANDOM NODE: ~p", [RandomNode]),
    % utils:print_nodes([RandomNode], "RANDOM NODE: ", important),

    {Nodes, BootstrapNodes, SupPid}.


measure_avg_random_lookup_with_failure(N, BootstrapCount, NodeCount) ->
    % Initialize the network
    IdByteLength = 2,
    K = 20,
    CustomOpts = #{
        k_param => K,
        alpha_param => 3,
        id_byte_length => IdByteLength,
        republish_interval => 5000,
        expiration_interval => 360000000,
        refresh_interval => 360000000,
        timeout_interval => 500,
        log_level => important,
        refresh => true
    },
    {Nodes, _BootstrapNodes, SupPid} = prepare_network(BootstrapCount, 
        NodeCount, CustomOpts),

    % Save a random value in the K closest nodes
    {KClosest, Key} = store_and_get_k_closest(Nodes, IdByteLength, K),
    log:important("Found ~p nodes in measure_avg_random_lookup_with_failure",
         [length(KClosest)]),

    % Perform measurements
    Results = lists:map(
        fun(FailedCount) ->
            % Kill FailedCount nodes among the closest
            utils:print_nodes(KClosest, "KClosest nodes: ", important),
            log:important("Attempting to kill ~p nodes", [FailedCount]),
            {ToKill, _ToKeep} = lists:split(FailedCount, KClosest),
            terminate_nodes(ToKill),
            utils:print_nodes(ToKill, "Nodes to kill: ", important),

            % Remove killed nodes from lookup candidates
            FilteredNodes = [Node || Node <- Nodes, not lists:member(Node, ToKill)],

            % Perform N random succesfull lookups
            log:important("~n~nINITIATING LOOKUP MEASUREMENTS with ~p FAILED NODES~n", [FailedCount]),
            {SuccessfulTimes, SuccessfulHops, TotalAttempts, Status} = perform_measurements_with_failure(
                N, FilteredNodes, Key, [], [], 0, IdByteLength, 0),

            % Calculate average lookup time
            Length = length(SuccessfulTimes),
            case Length of 
                0 -> {FailedCount, -1, -1, TotalAttempts};
                _ -> 
                    AvgTime = lists:sum(SuccessfulTimes) / Length,
                    AvgHops = round(lists:sum(SuccessfulHops) / Length),
                    log:important("~p Successful Lookup measurements (~p total attempts): ~p~n", 
                            [length(SuccessfulTimes), TotalAttempts, SuccessfulTimes]),
                    log:important("~nAverage lookup time (~p successful runs): ~.2f ms~n", [Length, AvgTime]),
                    log:important("~nAverage number of hops (~p successful runs): ~p~n", [Length, AvgHops]),
                    {FailedCount, AvgTime, AvgHops, TotalAttempts, Status}
            end
        end,
        lists:seq(0, K-1)
    ),

    terminate_nodes_and_sup(Nodes, SupPid),

    Results.


store_and_get_k_closest(Nodes, IdByteLength, K) ->
    RandomNodeIndex = rand:uniform(length(Nodes)),
    {_, _, RandomPid} = lists:nth(RandomNodeIndex, Nodes),
    RandomValue = rand:uniform(1000000000),
    {store_response, AckNodes} = store(RandomPid, RandomValue),
    Key = utils:calculate_key(RandomValue, IdByteLength),
    IsBootstrap = lists:any(
        fun({NodeName, _, _}) -> utils:is_bootstrap(NodeName) 
    end, AckNodes),
    case IsBootstrap of
        true ->
            log:important("Bootstrap node found in AckNodes, retrying..."),
            store_and_get_k_closest(Nodes, IdByteLength, K);
        false ->
            {AckNodes, Key}
    end.


perform_measurements_with_failure(0, _nodes, _Key, AccTimes, AccHops, 
        AccAttempts, _, _) ->
    {lists:reverse(AccTimes), lists:reverse(AccHops), AccAttempts, success};
perform_measurements_with_failure(Remaining, Nodes, Key, AccTimes, AccHops, 
        AccAttempts, IdByteLength, Failures) ->
    case Failures >= 1000 of
        true -> 
            {lists:reverse(AccTimes), lists:reverse(AccHops), AccAttempts, fail};
        false ->
            % Select random lookup node
            RandIndex = rand:uniform(length(Nodes)),
            RandLookupNode = lists:nth(RandIndex, Nodes),

            % Perform measurement with lookup node
            log:important("~n~n---- MEASUREMENT ~p / ~p ----~n", 
                [length(AccTimes) + 1, Remaining + length(AccTimes)]),
            {Time, Result} = measure_random_lookup(RandLookupNode, Key),
            case Result of
                {ok, found, Hops} ->
                    % Successful lookup - add to results and continue
                    perform_measurements_with_failure(Remaining - 1, Nodes, Key, 
                        [Time | AccTimes], [Hops | AccHops], 
                        AccAttempts + 1, IdByteLength, Failures);
                _ ->
                    % Failed lookup - retry with same remaining count
                    log:important("Lookup failed, retrying... Failures: ~p", 
                        [Failures]),
                    perform_measurements_with_failure(Remaining, Nodes, Key, 
                        AccTimes, AccHops,
                        AccAttempts + 1, IdByteLength, Failures + 1)
            end
    end.


measure_avg_random_lookup(N, BootstrapCount, NodeCount) ->
    % Initialize the network
    IdByteLength = 2,
    CustomOpts = #{
        k_param => 10,
        alpha_param => 3,
        id_byte_length => IdByteLength,
        republish_interval => 360000000,
        expiration_interval => 360000000,
        refresh_interval => 360000000,
        timeout_interval => 1000,
        log_level => important,
        refresh => true
    },
    {Nodes, _BootstrapNodes, SupPid} = prepare_network(BootstrapCount, 
        NodeCount, CustomOpts),

    % Select random lookup node for lookup testing on random values
    RandomNodeIndex = rand:uniform(length(Nodes)),
    RandomLookupNode = lists:nth(RandomNodeIndex, Nodes),

    % Perform measurements
    log:important("~n~nINITIATING LOOKUP MEASUREMENTS with ~p NODES~n", 
        [NodeCount]),
    {SuccessfulTimes, SuccessfulHops, TotalAttempts} = perform_measurements(RandomLookupNode, N, 
        Nodes, [], [], 0, IdByteLength),

    % Calculate average lookup time
    Length = length(SuccessfulTimes),
    AvgTime = lists:sum(SuccessfulTimes) / Length,
    AvgHops = round(lists:sum(SuccessfulHops) / Length),

    log:important("~p Successful Lookup measurements (~p total attempts): ~p~n", 
                [length(SuccessfulTimes), TotalAttempts, SuccessfulTimes]),
    log:important("~nAverage lookup time (~p successful runs): ~.2f ms~n", 
        [Length, AvgTime]),
    log:important("~nAverage number of hops (~p successful runs): ~p~n", 
        [Length, AvgHops]),

    % Terminate nodes for this iteration
    terminate_nodes_and_sup(Nodes, SupPid),
    
    {AvgTime, AvgHops}.


perform_measurements(_LookupNode, 0, _nodes, AccTimes, AccHops, 
        AccAttempts, _) ->
    {lists:reverse(AccTimes), lists:reverse(AccHops), AccAttempts};
perform_measurements(LookupNode, Remaining, Nodes, AccTimes, AccHops, 
        AccAttempts, IdByteLength) ->

    % Select a random node and random value for this attempt
    RandomNodeIndex = rand:uniform(length(Nodes)),
    {_, _, RandomPid} = lists:nth(RandomNodeIndex, Nodes),

    % Generate random value and store it in the K closest nodes using random node
    RandomValue = rand:uniform(1000000000),
    StoreResult = store(RandomPid, RandomValue),

    case StoreResult of
        {store_response, _Acks} -> 
            log:important("Store complete in perform_measurements~n"),

            % Perform measurement with lookup node
            log:important("~n~n---- MEASUREMENT ~p / ~p ----~n", 
                [length(AccTimes) + 1, Remaining + length(AccTimes)]),
            TestKey = utils:calculate_key(RandomValue, IdByteLength),
            {Time, Result} = measure_random_lookup(LookupNode, TestKey),
            
            case Result of
                {ok, found, Hops} ->
                    % Successful lookup - add to results and continue
                    perform_measurements(LookupNode, Remaining - 1, Nodes, 
                        [Time | AccTimes], 
                        [Hops | AccHops], AccAttempts + 1, IdByteLength);
                _ ->
                    % Failed lookup - retry with same remaining count
                    log:important("Lookup failed, retrying..."),
                    perform_measurements(LookupNode, Remaining, Nodes, 
                        AccTimes, AccHops, AccAttempts + 1, IdByteLength)
            end;
        _ -> 
            % Failed to store measurement
            perform_measurements(LookupNode, Remaining, Nodes, AccTimes, 
                AccHops, AccAttempts + 1, IdByteLength)
    end.


measure_random_lookup({_NodeName, _NodeId, NodePid}, TestKey) ->
    {Time, Result} = timer:tc(fun() -> 
        NodePid ! {find_value_request, TestKey, self()},
        receive
            {find_value_response, Entry, Hops} when Entry =/= not_found -> 
                {ok, found, Hops};
            {find_value_response, not_found} ->
                {error, not_found}
        % after 100 -> 
        %     {error, timeout}
        end
    end),

    TimeMS = Time / 1000,
    log:important("Result: ~p~n", [Result]),
    log:important("Lookup time: ~.2f ms~n", [TimeMS]),
    {TimeMS, Result}.


measure_avg_join_time(NodeCount, BootstrapCount, JoinsPerConfiguration) ->
    IdByteLength = 2,
    CustomOpts = #{
        k_param => 20,
        alpha_param => 3,
        id_byte_length => IdByteLength,
        republish_interval => 3600000,
        expiration_interval => 3600000,
        refresh_interval => 3600000,
        timeout_interval => 2000,
        log_level => important,
        refresh => true
    },
    {BaseNodes, BootstrapNodes, SupPid} = prepare_network(BootstrapCount, 
        NodeCount, CustomOpts),
    log:important("Base network with ~p nodes prepared", [NodeCount]),
    
    % Measure join time for new nodes
    Refresh = maps:get(refresh, CustomOpts, false),
    JoinTimes = measure_node_joins(JoinsPerConfiguration, BaseNodes, 
        IdByteLength, [], Refresh),
    
    % Calculate average join time
    AvgJoinTime = lists:sum(JoinTimes) / length(JoinTimes),
    log:important("Join measurements completed: ~p", [JoinTimes]),
    log:important("Average join time over ~p measurements: ~.2f ms", 
                 [length(JoinTimes), AvgJoinTime]),
    
    % Terminate the network for this configuration
    terminate_nodes_and_sup(BaseNodes ++ BootstrapNodes, SupPid),
    log:important("Network terminated"),
    
    % Return the average join time
    AvgJoinTime.


measure_node_joins(0, _BaseNodes, _IdByteLength, AccTimes, _Refresh) ->
    lists:reverse(AccTimes);
measure_node_joins(Remaining, BaseNodes, IdByteLength, AccTimes, Refresh) ->
    log:important("~n---- JOIN MEASUREMENT ~p / ~p ----~n", 
                 [length(AccTimes) + 1, Remaining + length(AccTimes)]),
    
    % Generate a unique node name for this measurement
    JoinIndex = length(AccTimes) + 1,
    NodeName = list_to_atom("join_node_" ++ integer_to_list(JoinIndex)),
    
    % Measure join time
    {JoinTime, JoinResult} = timer:tc(fun() -> 
        % Start a new node and wait for its initialization
        case node:start_simple_node(NodeName) of
            {ok, Node} ->
                wait_for_node_initialization(Node, Refresh);
            Error ->
                {error, Error}
        end
    end),
    
    % Convert microseconds to milliseconds
    JoinTimeMs = JoinTime / 1000,
    log:important("Join time: ~.2f ms, Result: ~p", [JoinTimeMs, JoinResult]),
    
    case JoinResult of
        ok ->
            % Successful join, continue with next measurement
            measure_node_joins(Remaining - 1, BaseNodes, IdByteLength, 
                [JoinTimeMs | AccTimes], Refresh);
        _ ->
            % Failed join, retry with same remaining count
            log:important("Join failed, retrying..."),
            measure_node_joins(Remaining, BaseNodes, IdByteLength, AccTimes, 
                Refresh)
    end.


create_nodes(N, Refresh) ->
    create_nodes(1, N, [], Refresh).

create_nodes(I, N, Acc, _) when I > N ->
    lists:reverse(Acc);
create_nodes(I, N, Acc, Refresh) ->
    Name = list_to_atom("node_" ++ integer_to_list(I)),
    case node:start_simple_node(Name) of
        {ok, Node} ->
            timer:sleep(50),
            Result = wait_for_node_initialization(Node, Refresh),
            case Result of
                ok -> create_nodes(I + 1, N, [Node | Acc], Refresh);
                _ -> create_nodes(I + 1, N, Acc, Refresh)
            end;
        _ ->
            log:error("Failed to create node ~p", [I]),
            create_nodes(I + 1, N, Acc, Refresh)
    end.


wait_for_refresh({NodeName, _, NodePid}) ->
    receive
        {refresh_complete} -> 
            log:important("~n~n~p (~p) REFRESHED SUCCESFULLY.~n", [NodeName, 
                NodePid]),
            ok
    after 5000 -> 
        log:important("~n~n~p (~p) FAILED TO REFRESH.~n", [NodeName,
             NodePid]),
        timeout
    end.


create_bootstrap_nodes(N) ->
    create_bootstrap_nodes(1, N, [], self()).

create_bootstrap_nodes(I, N, Acc, _) when I > N ->
    lists:reverse(Acc);
create_bootstrap_nodes(I, N, Acc, Self) ->
    Name = list_to_atom("bootstrap_" ++ integer_to_list(I)),
    case node:start_bootstrap_node(Name, Self) of
        {ok, Node} ->
            timer:sleep(50),
            wait_for_bootstrap_initialization(Name, Node),
            create_bootstrap_nodes(I + 1, N, [Node | Acc], Self);
        _ ->
            log:error("Failed to create bootstrap node ~p. Terminating.", [I]),
            exit(bootstrap_node_creation_failed)
    end.


terminate_nodes_and_sup(Nodes, SupPid) ->
    log:info("~n~n========== TERMINATING ALL NODES ==========~n"),

    % Terminate regular nodes
    terminate_nodes(Nodes),
     log:important("All regular nodes terminated"),

    % Terminate supervisor and bootstrap nodes
    log:important("Terminating bootstrap nodes and supervisor: ~p", [SupPid]),
    Ref = monitor(process, SupPid),
    exit(SupPid, shutdown),
    receive
        {'DOWN', Ref, process, _Pid, _Reason} ->
            log:important("Supervisor terminated"),
            ok
    after 1000 ->
            log:important("Supervisor not terminated"),
           timeout
    end,

    % Clean resources
    ets:delete(bootstrap_nodes),
    log:info("Network termination complete"),
    ok.


terminate_nodes(Nodes) ->
    log:info("~n~n========== TERMINATING ~p NODES ==========~n", 
        [length(Nodes)]),

    % Terminate regular nodes
    Pids = [begin
        case is_process_alive(Pid) of
            true ->
                exit(Pid, kill),
                log:important("Terminated node: ~p", [Pid]),
                Pid;
            false -> Pid
        end
     end || {_, _, Pid} <- Nodes],
     wait_until_dead(Pids).


wait_until_dead([]) ->
    ok;
wait_until_dead(Pids) ->
    Alive = [Pid || Pid <- Pids, is_process_alive(Pid)],
    case Alive of
        [] -> ok;
        _ -> 
            timer:sleep(100),
            wait_until_dead(Alive)
    end.


wait_for_node_initialization(Node, Refresh) ->
    {NodeName, _, NodePid} = Node,
    receive
        {find_node_response, _ClosestK} -> 
            log:info("~n~n~p (~p) INITIALIZED SUCCESFULLY.~n", 
                [NodeName, NodePid]),
            case Refresh of 
                false -> ok;
                _ -> refresh_node(Node)
            end
    after 5000 -> 
        log:info("~n~n~p (~p) FAILED TO INITIALIZE.~n", [NodeName, NodePid]),
        exit(NodePid, kill),
        timeout
    end.


wait_for_bootstrap_initialization(NodeName, NodePid) ->
    receive
        {find_node_response, _ClosestK} -> 
            log:info("~n~n~p (~p) INITIALIZED SUCCESFULLY.~n", [NodeName, 
                NodePid]),
            ok
    after 5000 -> 
        log:info("~n~n~p (~p) FAILED TO INITIALIZE.~n", [NodeName, NodePid]),
        % exit(bootstrap_failed_init),
        timeout
    end.