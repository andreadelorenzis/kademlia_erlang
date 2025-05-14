-module(test).
-export([run_protocol_tests/0, measure_random_lookup/2, 
        prepare_network/2, measure_avg_random_lookup/3, run_measurements/0,
        print_buckets/1, print_storage/1, ping/2, store/2, find_value/2, find_node/2]).


run_protocol_tests() ->
    log:start(),
    log:set_level_global(info),
    process_flag(trap_exit, true),

    log:info("~n========== RUNNING PROTOCOL TESTS ==========~n"),

    % Start network with some options
    IdByteLength = 5,
	CustomOpts = #{
	    k_param => 20,
        alpha_param => 3,
        id_byte_length => IdByteLength,
        republish_interval => 3600000,    % 1h
	    expiration_interval => 3600000,   % 1h
        refresh_interval => 3600000,      % 1h 
        timeout_interval => 2000,
        log_level => debug
	},
    {ok, SupPid} = node:start_network(CustomOpts),

    log:info("~n~n----- NODES initialization -----~n"),

    % Creation of one bootstrap node
    IdLength = 5,
    {ok, Bootstrap1} = node:start_bootstrap_node(bootstrap1, self(), IdLength),
    wait_for_bootstrap_initialization(bootstrap1, Bootstrap1),
    BootstrapPids = [Bootstrap1],

    % Start a few regular nodes
    {ok, Node1} = node:start_simple_node(nodo1, IdLength),
    wait_for_node_initialization(Node1),
    {ok, Node2} = node:start_simple_node(nodo2, IdLength),
    wait_for_node_initialization(Node2),
    Nodes = [Node1, Node2],

    % Validation tests for Kademlia protocol
    Value = 123456,
    Key = utils:calculate_key(Value, IdByteLength),

    PingTest = test_ping(Node1, Node2),
    FindNodeTest = test_find_node(Node1, Key, Nodes, BootstrapPids),
    StoreTest = test_store(Node1, Value),
    FindValueTest = test_find_value(Node1, Key, Value),

    % Terminate the network
    terminate_nodes(Nodes, SupPid),

    % % Print test results
    io:format("~n~n~n========== ALL TESTS COMPLETED ==========~n"),
    print_test_result("PING should responde with 'alive'", PingTest),
    print_test_result("FIND_NODE should return all expected nodes", FindNodeTest),
    print_test_result("STORE should respond with 'store_complete'", StoreTest),
    print_test_result("FIND_VALUE should return the correct value", FindValueTest).


print_buckets(NodePid) ->
    NodePid ! {get_buckets_request, self()},
    receive
        {buckets_dump, Buckets, NodeName} ->
            utils:print_buckets(NodeName, NodePid, Buckets, important)
    after 5000 ->
        log:error("Timeout getting buckets for ~p", [NodePid])
    end.

print_storage(NodePid) ->
    NodePid ! {get_storage_request, self()},
    receive
        {storage_dump, Storage, NodeName} ->
            utils:print_storage(NodeName, NodePid, Storage, important)
    after 5000 ->
        log:error("Timeout getting storage for ~p", [NodePid])
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

print_test_result(Title, true) ->
    io:format("~n-------[OK]------- ~s~n", [Title]);
print_test_result(Title, false) ->
    io:format("~n-------[FAIL]------- ~s~n", [Title]).


wait_for_node_initialization({NodeName, _, NodePid}) ->
    receive
        {find_node_response, _ClosestK} -> 
            log:info("~n~n~p (~p) INITIALIZED SUCCESFULLY.~n", [NodeName, NodePid]),
            ok
    after 5000 -> 
        log:info("~n~n~p (~p) FAILED TO INITIALIZE.~n", [NodeName, NodePid]),
        exit(NodePid, kill),
        timeout
    end.

wait_for_bootstrap_initialization(NodeName, NodePid) ->
    receive
        {find_node_response, _ClosestK} -> 
            log:info("~n~n~p (~p) INITIALIZED SUCCESFULLY.~n", [NodeName, NodePid]),
            ok
    after 5000 -> 
        log:info("~n~n~p (~p) FAILED TO INITIALIZE.~n", [NodeName, NodePid]),
        exit(bootstrap_failed_init),
        timeout
    end.

% Send a ping from Sender node to Receiver node to check if the latter is alive
test_ping(SenderNode, ReceiverNode) ->
    log:info("~n~n----- Testing PING operation -----~n"),
    Result = ping(SenderNode, ReceiverNode),
    case Result of
        alive -> 
            log:debug("~nReceived 'alive'~n"), 
            true;
        dead -> 
            log:debug("~nReceived 'dead'~n"),  
            false
    end.    

% Store value on K closest nodes to the corresponding hash key
test_store(Node, Value) ->
    log:info("~n~n----- Testing STORE operation -----~n"),
    Result = store(Node, Value),
    case Result of
        {store_response, ok} -> 
            log:debug("Store complete in test_store~n"),
            true;
        _ -> 
            false
    end.

% Search K closest nodes to ID
test_find_node(Node, ID, Nodes, BootstrapPids) ->
    log:info("~n~n----- Testing FIND_NODE operation -----~n"),
    Result = find_node(Node, ID),
    {_, _NodeId, NodePid} = Node,
    case Result of
        {find_node_response, KClosest} -> 
            utils:print_nodes(KClosest, "test - find_node_response: ", debug),

            % Remove bootstrap nodes and myself from lookup result
            FilteredNodes = [Entry || Entry = {_Name, _Id, Pid} <- KClosest, 
                not lists:member(Pid, BootstrapPids)],
            ExpectedNodes = [{Name, Pid} || {Name, _Id, Pid} <- Nodes, Pid =/= NodePid],
            ResultNodes = [{Name, Pid} || {Name, _Id, Pid} <- FilteredNodes, Pid =/= NodePid],

            % Check if response contains all the other expected nodes
            log:debug("ExpectedNodes = ~p", [ExpectedNodes]),
            log:debug("ResultNodes = ~p", [ResultNodes]),
            lists:sort(ResultNodes) =:= lists:sort(ExpectedNodes);
        _ -> false
    end.

% Find value by key
test_find_value(Node, Key, Value) ->
    log:info("~n~n----- Testing FIND_VALUE operation -----~n"),

    {_, _NodeId, NodePid} = Node,

    % First I delete the value from the node I'm testing, if it exists in its storage
    Self = self(),
    NodePid ! {delete_storage_entry_request, Key, Self},
    receive
        {delete_storage_entry_response, ok} -> ok
    end,

    % Test the actual value lookup
    Result = find_value(Node, Key),
    case Result of
        {find_value_response, Entry, Hops} when Entry =/= not_found -> 
            {ValueFound, _Expiry, _Owner} = Entry,
            log:debug("Entry found in test_find_value in ~p hops: ~p~n", 
                [Hops, {ValueFound, _Expiry, _Owner}]),
            ValueFound =:= Value;
        {find_value_response, not_found} -> false;
        _ -> false
    end.


ping({_, _SenderId, SenderPid}, ReceiverNode) ->
    SenderPid ! {'ping_request', ReceiverNode, self()},
    receive
        alive -> alive;
        dead -> dead
    after 10000 -> 
        dead
    end.    

store({_, _NodeId, NodePid}, Value) ->
    NodePid ! {store_request, Value, self()},
    receive
        {store_response, ok, AckNodes} -> 
            utils:print_nodes(AckNodes, "Store ACKs: ", debug),
            {store_response, ok}
    after 5000 -> timeout
    end.

find_node({_, _NodeId, NodePid}, ID) ->
    NodePid ! {find_node_request, ID, self()},
    receive
        {find_node_response, KClosest} -> 
            utils:print_nodes(KClosest, "K closest nodes: ", debug),
            {find_node_response, KClosest}
    after 5000 -> timeout
    end.

find_value({_, _NodeId, NodePid}, Key) ->
    NodePid ! {find_value_request, Key, self()},
    receive
        {find_value_response, Entry, Hops} when Entry =/= not_found -> 
            {find_value_response, Entry, Hops};
        {find_value_response, not_found} -> 
            {find_value_response, not_found}
    after 2000 -> timeout
    end.


terminate_nodes(Nodes, SupPid) ->
    log:info("~n~n========== TERMINATING ALL NODES ==========~n"),

    % Terminate regular nodes
    Pids = [begin
        case is_process_alive(Pid) of
            true ->
                exit(Pid, kill),
                log:debug("Terminated node: ~p", [Pid]),
                Pid;
            false -> Pid
        end
     end || {_, _, Pid} <- Nodes],
     wait_until_dead(Pids),
     log:debug("All regular nodes terminated"),

    % Terminate supervisor and bootstrap nodes
    log:debug("Terminating bootstrap nodes and supervisor: ~p", [SupPid]),
    Ref = monitor(process, SupPid),
    exit(SupPid, shutdown),
    receive
        {'DOWN', Ref, process, _Pid, _Reason} ->
            log:debug("Supervisor terminated"),
            ok
    after 1000 ->
            log:debug("Supervisor not terminated"),
           timeout
    end,

    % Clean resources
    ets:delete(bootstrap_nodes),
    log:info("Network termination complete"),
    ok.


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


prepare_network(BootstrapCount, NodeCount) ->
    log:start(),

    Options = #{
        k_param => 20,
        alpha_param => 3,
        id_byte_length => 2,
        republish_interval => 3600000,
        expiration_interval => 86400000,
        refresh_interval => 3600000,
        timeout_interval => 2000,
        log_level => info
    },
    prepare_network(BootstrapCount, NodeCount, Options),
    ok.

prepare_network(BoostrapCount, NodeCount, Options) ->
    log:info("Starting the network with ~p BOOTSTRAP nodes ----", [BoostrapCount]),
    {ok, SupPid} = node:start_network(Options),

    IdByteLength = maps:get(id_byte_length, Options),
    log:info("ID bit length = ~p", [IdByteLength * 8]),

    log:info("Creation of ~p BOOTSTRAP NODES", [BoostrapCount]),
    BootstrapNodes = create_bootstrap_nodes(BoostrapCount, IdByteLength),

    log:info("Creation of ~p NODES", [NodeCount]),
    Nodes = create_nodes(NodeCount, IdByteLength),

    log:info("All ~p NODES CREATED AND INITIALIZED", [length(Nodes)]),

    % RefreshSample = lists:sublist(Nodes, max(1, length(Nodes) div 5)),
    % log:info("SHELL (~p): REFRESHING ~p NODES ----", [Self, length(RefreshSample)]),
    % lists:foreach(fun({_, _, Pid}) -> Pid ! {refresh_buckets, Self} end, RefreshSample),
    % Refreshed = gather_refresh_acks(length(RefreshSample), 5000),

    % log:info("SHELL (~p): received ~p / ~p refresh_buckets_complete responses", 
    %     [Self, Refreshed, length(RefreshSample)]),

    log:info("NETWORK PREPARATION COMPLETE."),

    % Select random lookup node for lookup testing on random values
    RandomNodeIndex = rand:uniform(length(Nodes)),
    RandomNode = lists:nth(RandomNodeIndex, Nodes),

    utils:print_nodes(Nodes, "REACHABLE NODES: ", important),
    log:info("RANDOM NODE: ~p", [RandomNode]),

    {Nodes, BootstrapNodes, SupPid}.


run_measurements() ->
    process_flag(trap_exit, true),
    log:start(),
    log:set_level_global(important),
    log:clean_console(),

    % NodeCounts = [32, 64, 128, 256, 512, 1024],
    NodeCounts = [8, 16, 32, 128, 256, 512, 1024],
    BootstrapCount = 3,
    LookupsPerRun = 50,
    % NumTrialsPerNodeCount = 1,

    Results = lists:map(
        fun(NodeCount) ->
            % {AvgTimes, AvgHops} = [measure_avg_random_lookup(LookupsPerRun, BootstrapCount, NodeCount)
            %            || _ <- lists:seq(1, NumTrialsPerNodeCount)],
            % AvgTrialsTimes = lists:sum(AvgTimes) / length(AvgTimes),
            % AvgTrialsHops = lists:sum(AvgHops) / length(AvgHops),
            % {NodeCount, AvgTrialsTimes, AvgTrialsHops}
            {AvgTimes, AvgHops} = measure_avg_random_lookup(LookupsPerRun, BootstrapCount, NodeCount),
            {NodeCount, AvgTimes, AvgHops}
        end,
        NodeCounts
    ),
    log:clean_console(),
    log:important("~n~n----------- RESULTS -----------~n"),
    [begin log:raw(important, "Node count: ~p, Avg lookup time: ~.2f ms, Avg hops: ~p", 
        [NodeCount, AvgTime, AvgHops]) end
     || {NodeCount, AvgTime, AvgHops} <- Results],
    ok.


measure_avg_random_lookup(N, BootstrapCount, NodeCount) ->
    CustomOpts = #{
        k_param => 50,
        alpha_param => 3,
        id_byte_length => 5,
        republish_interval => 3600000,
        expiration_interval => 3600000,
        refresh_interval => 3600000,
        timeout_interval => 2000,
        log_level => important
    },

    {Nodes, _BootstrapNodes, SupPid} = prepare_network(BootstrapCount, NodeCount, CustomOpts),

    % Select random lookup node for lookup testing on random values
    RandomNodeIndex = rand:uniform(length(Nodes)),
    RandomLookupNode = lists:nth(RandomNodeIndex, Nodes),
    
    log:important("INITIATING LOOKUP MEASUREMENTS~n"),
    {SuccessfulTimes, SuccessfulHops, TotalAttempts} = perform_measurements(RandomLookupNode, N, 
        Nodes, [], [], 0),
    
    Results = case SuccessfulTimes of
        [] ->
            log:important("Failed to complete any successful lookups after ~p total attempts~n", 
                [TotalAttempts]),
            {0, 0};
        _ ->
            % Use truncated average to reduce outliers
            SortedTimes = lists:sort(SuccessfulTimes),
            SortedHops = lists:sort(SuccessfulHops),
            Length = length(SortedTimes),

            % Calculate elements to cut
            ToDrop = trunc(Length * 0.05),

            % Take only central part (90% of data)
            TruncatedTimes = lists:sublist(SortedTimes, ToDrop + 1, Length - 2 * ToDrop),
            TruncatedHops = lists:sublist(SortedHops, ToDrop + 1, Length - 2 * ToDrop),
            TruncatedLength = length(TruncatedTimes),
            AvgTime = lists:sum(TruncatedTimes) / TruncatedLength,
            AvgHops = round(lists:sum(TruncatedHops) / TruncatedLength),
            log:important("~p Successful Lookup measurements (~p total attempts): ~p~n", 
                     [length(SuccessfulTimes), TotalAttempts, SuccessfulTimes]),
            log:important("~nAverage lookup time (~p successful runs): ~.2f ms~n", [TruncatedLength, AvgTime]),
            log:important("~nAverage number of hops (~p successful runs): ~p ms~n", [TruncatedLength, AvgHops]),
            {AvgTime, AvgHops}
    end,

    terminate_nodes(Nodes, SupPid),
    
    Results.


perform_measurements(_LookupNode, 0, _nodes, AccTimes, AccHops, AccAttempts) ->
    {lists:reverse(AccTimes), lists:reverse(AccHops), AccAttempts};
perform_measurements(LookupNode, Remaining, Nodes, AccTimes, AccHops, AccAttempts) ->

    % Select a random node and random value for this attempt
    RandomNodeIndex = rand:uniform(length(Nodes)),
    RandomNode = lists:nth(RandomNodeIndex, Nodes),

    % Generate random value and store it in the K closest nodes using random node
    RandomValue = rand:uniform(1000000000),
    StoreResult = store(RandomNode, RandomValue),
    case StoreResult of
        {store_response, ok} -> 
            log:important("Store complete in perform_measurements~n"),

            % Perform measurement with lookup node
            log:important("~n~n---- MEASUREMENT ~p / ~p ----~n", [length(AccTimes) + 1, 
                Remaining + length(AccTimes)]),
            TestKey = utils:calculate_key(RandomValue, 5),
            {Time, Result} = measure_random_lookup(LookupNode, TestKey),
            
            case Result of
                {ok, found, Hops} ->
                    % Successful lookup - add to results and continue
                    perform_measurements(LookupNode, Remaining - 1, Nodes, [Time | AccTimes], 
                        [Hops | AccHops], AccAttempts + 1);
                _ ->
                    % Failed lookup - retry with same remaining count
                    log:important("Lookup failed, retrying..."),
                    perform_measurements(LookupNode, Remaining, Nodes, AccTimes, AccHops,
                        AccAttempts + 1)
            end;
        _ -> 
            % Failed to store measurement
            perform_measurements(LookupNode, Remaining, Nodes, AccTimes, AccHops,
                AccAttempts + 1)
    end.


measure_random_lookup({_NodeName, _NodeId, NodePid}, TestKey) ->
    {Time, Result} = timer:tc(fun() -> 
        NodePid ! {find_value_request, TestKey, self()},
        receive
            {find_value_response, Entry, Hops} when Entry =/= not_found -> 
                {ok, found, Hops};
            {find_value_response, not_found} ->
                {error, not_found}
        after 1000 -> 
            {error, timeout}
        end
    end),

    TimeMS = Time / 1000,
    log:important("Result: ~p~n", [Result]),
    log:important("Lookup time: ~.2f ms~n", [TimeMS]),
    {TimeMS, Result}.


create_nodes(N, IdByteLength) ->
    create_nodes(1, N, [], IdByteLength).

create_nodes(I, N, Acc, _) when I > N ->
    lists:reverse(Acc);
create_nodes(I, N, Acc, IdByteLength) ->
    Name = list_to_atom("node_" ++ integer_to_list(I)),
    case node:start_simple_node(Name, IdByteLength) of
        {ok, Node} ->
            timer:sleep(50),
            Result = wait_for_node_initialization(Node),
            case Result of
                ok -> create_nodes(I + 1, N, [Node | Acc], IdByteLength);
                _ -> create_nodes(I + 1, N, Acc, IdByteLength)
            end;
        _ ->
            log:error("Failed to create node ~p", [I]),
            create_nodes(I + 1, N, Acc, IdByteLength)
    end.


create_bootstrap_nodes(N, IdByteLength) ->
    create_bootstrap_nodes(1, N, [], self(), IdByteLength).

create_bootstrap_nodes(I, N, Acc, _, _) when I > N ->
    lists:reverse(Acc);
create_bootstrap_nodes(I, N, Acc, Self, IdByteLength) ->
    Name = list_to_atom("bootstrap_" ++ integer_to_list(I)),
    case node:start_bootstrap_node(Name, Self, IdByteLength) of
        {ok, Node} ->
            timer:sleep(50),
            wait_for_bootstrap_initialization(Name, Node),
            create_bootstrap_nodes(I + 1, N, [Node | Acc], Self, IdByteLength);
        _ ->
            log:error("Failed to create bootstrap node ~p. Terminating.", [I]),
            exit(bootstrap_node_creation_failed)
    end.
