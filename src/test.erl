-module(test).
-export([run_protocol_tests/0, run_republish_test/0, run_refresh_test/0, print_storage/2,
         print_buckets/2, measure_random_lookup/2, 
        measure_avg_random_lookup/3]).


run_protocol_tests() ->
    process_flag(trap_exit, true),

    % Start the logger and set it to debug
    log:start(),
    log:set_level_global(info),

    log:info("~n========== RUNNING PROTOCOL TESTS ==========~n"),
    Self = self(),

    % Test bootstrap and normal nodes initialization
    BootstrapCount = 1,
	CustomOpts = #{
	    k_param => 20,
        alpha_param => 3,
        republish_interval => 60000,     % 2 sec 
	    expiration_interval => 300000,   % 5 min
        refresh_interval => 60000       % 60 sec  
	},
    {NodesInitTest, Nodes, SupPid, BootstrapNodes} = 
        test_network_initialization(Self, BootstrapCount, CustomOpts),
    print_test_result("All nodes should be initialized correctly", NodesInitTest),
    case NodesInitTest of
        false -> exit(normal);
        _ -> ok
    end,
    [Node1, Node2] = Nodes,
    log:info("SupPid: ~p", [SupPid]),
    
    % Validation tests for Kademlia protocol
    Value = 123456,
    Key = utils:calculate_key(Value),

    PingTest = test_ping(Node1, Node2),
    FindNodeTest = test_find_node(Node1, Key, Nodes, BootstrapNodes),
    StoreTest = test_store(Node1, Value),
    FindValueTest = test_find_value(Node1, Key, Value),

    % Terminate the network
    terminate_nodes(Nodes, SupPid),

    % Print test results
    timer:sleep(2000),
    io:format("~n~n~n========== ALL TESTS COMPLETED ==========~n"),
    print_test_result("All nodes should be initialized correctly", NodesInitTest),
    print_test_result("PING should responde with 'alive'", PingTest),
    print_test_result("FIND_NODE should return all expected nodes", FindNodeTest),
    print_test_result("STORE should respond with 'store_complete'", StoreTest),
    print_test_result("FIND_VALUE should return the correct value", FindValueTest).



run_republish_test() ->
    process_flag(trap_exit, true),  

    % Start the logger and set it to debug
    log:start(),
    log:set_level(important),

    log:info("~n========== RUNNING REPUBLISH TEST ==========~n"),
    Self = self(),

    % Test bootstrap and normal nodes initialization
    BootstrapCount = 1,
	CustomOpts = #{
	    k_param => 20,
        alpha_param => 1,
        republish_interval => 2000,     % 2 sec 
	    expiration_interval => 300000,   % 5 min
        refresh_interval => 60000       % 60 sec  
	},
    {NodesInitTest, Nodes, SupPid, _BootstrapNodes} = 
        test_network_initialization(Self, BootstrapCount, CustomOpts),
    print_test_result("All nodes should be initialized correctly", NodesInitTest),
    case NodesInitTest of
        false -> exit(normal);
        _ -> ok
    end,
    [Node1, _Node2] = Nodes,
    log:info("SupPid: ~p", [SupPid]),

    % Store a value
    Value = 123456,
    Key = utils:calculate_key(Value),
    StoreTest = test_store(Node1, Value),
    FindValueTest = test_find_value(Node1, Key, Value),



    {RepublishValueTest, NewNode} = test_value_republishing(Key),
    NewNodes = [NewNode | Nodes],

    % Terminate the network
    terminate_nodes(NewNodes, SupPid),

    % Print test results
    timer:sleep(2000),
    io:format("~n~n~n========== ALL TESTS COMPLETED ==========~n"),
    print_test_result("STORE should respond with 'store_complete'", StoreTest),
    print_test_result("FIND_VALUE should return the correct value", FindValueTest),
    print_test_result("Values should be republished correctly", RepublishValueTest).


run_refresh_test() ->
    process_flag(trap_exit, true),  

    % Start the logger and set it to debug
    log:start(),
    log:set_level(error),

    log:info("~n========== RUNNING REFRESH TEST ==========~n"),
    Self = self(),

    % Initialization with custom parameters
    BootstrapCount = 1,
	CustomOpts = #{
	    k_param => 1,
        alpha_param => 1,
        republish_interval => 60000,     % 60 sec 
	    expiration_interval => 300000,   % 5 min
        refresh_interval => 300000       % 5 min  
	},

    % Start the network with 2 initial nodes
    {NodesInitTest, [Node1, Node2], SupPid, _BootstrapNodes} = 
        test_network_initialization(Self, BootstrapCount, CustomOpts),
    print_test_result("All nodes should be initialized correctly", NodesInitTest),
    NodesInitTest orelse exit(normal),

    % Add a third node (test subject)
    {ok, Node3, Node3Name} = node:start_simple_node(nodo3, self()),
    {_Node3Id, Node3Pid} = Node3,
    AllNodes = [Node1, Node2, Node3],

    % Wait for its initialization
    NodesInitialized = wait_for_node_initialization([Node3], [], 1),
    log:debug("~n----- NodesInitialized = ~p~n", [NodesInitialized]),

    % First check: node3 should NOT have all other nodes in its buckets list initially
    log:info("~n----- FIRST CHECK: Initial state -----~n"),
    FirstCheck = verify_buckets(Node3Pid, Node3Name, [Node1, Node2], false),
    print_test_result("Buckets should NOT contain all nodes initially", FirstCheck),

    % Wait for refresh
    WaitTime  = 30000,
    log:info("~n----- WAITING ~pms FOR REFRESH -----~n", [WaitTime]),
    timer:sleep(WaitTime),

    % Second check: after the refresh, node 3 should have all the other nodes in its bucket list
    log:info("~n----- SECOND CHECK: After refresh -----~n"),
    SecondCheck = verify_buckets(Node3Pid, Node3Name, [Node1, Node2], true),
    print_test_result("Buckets SHOULD contain all nodes after refresh", SecondCheck),

    % Termination and show results
    terminate_nodes(AllNodes, SupPid),
    timer:sleep(2000),
    io:format("~n~n~n========== ALL TESTS COMPLETED ==========~n"),
    print_test_result("Initially should NOT know all the nodes state correct", FirstCheck),
    print_test_result("After refresh, should kwow all the other nodes", SecondCheck),
    FinalResult = FirstCheck and SecondCheck,
    print_test_result("OVERALL TEST RESULT", FinalResult),
    FinalResult.


print_storage(NodePid, NodeName) ->
    NodePid ! {get_storage_request, self()},
    receive
        {storage_dump, Storage} when Storage =/= error -> 
            log:info("---- Printing storage of ~p ---- ~n~n~p~n~n", [NodeName, Storage]),
            ok;
        {storage_dump, error} -> error
    after 2000 -> not_found
    end.


print_buckets(NodePid, NodeName) ->
    NodePid ! {get_buckets_request, self()},
    receive
        {get_buckets_response, Buckets} when Buckets =/= error -> 
            log:info("---- Printing buckets of ~p ---- ~n~n~p~n~n", [NodeName, Buckets]),
            ok;
        {get_buckets_response, error} -> error
    after 2000 -> not_found
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


verify_buckets(NodePid, NodeName, ExpectedNodes, ShouldContain) ->
    NodePid ! {get_buckets_request, self()},
    receive
        {get_buckets_response, Buckets} when Buckets =/= error ->
            log:info("Buckets for ~p: ~p", [NodeName, Buckets]),
            
            % Extract all the nodes from the buckets
            AllNodesInBuckets = lists:flatten(Buckets),
            
            % Check if it is the expected result
            CheckResults = [lists:member(Node, AllNodesInBuckets) || Node <- ExpectedNodes],
            case ShouldContain of
                true -> lists:all(fun(X) -> X end, CheckResults);
                false -> not lists:all(fun(X) -> X end, CheckResults)
            end;

        {get_buckets_response, error} ->
            log:error("Failed to get buckets for ~p", [NodeName]),
            false
    after 50000 ->
        log:error("Timeout getting buckets for ~p", [NodeName]),
        false
    end.


print_test_result(Title, true) ->
    io:format("~n-------[OK]------- ~s~n", [Title]);
print_test_result(Title, false) ->
    io:format("~n-------[FAIL]------- ~s~n", [Title]).


test_network_initialization(Self, BootstrapCount, CustomOpts) ->
    % Start network by intializing bootstrap nodes
    {ok, SupNode, BootstrapNodes} = node:start_network(BootstrapCount, CustomOpts),
    log:debug("~n----- start_network result = ~p~n", [BootstrapNodes]),
    timer:sleep(1000),


    % Start a few regular nodes
    log:info("~n~n----- Testing NODE initialization -----~n"),
    {ok, Node1, _Nodo1Name} = node:start_simple_node(nodo1, Self),
    timer:sleep(500),
    {ok, Node2, _Nodo2Name} = node:start_simple_node(nodo2, Self),
    %timer:sleep(500),
    %{ok, Node3, _Nodo3Name} = node:start_simple_node(nodo3, Self),
    timer:sleep(500),
    Nodes = [Node1, Node2],

    NodesInitialized = wait_for_node_initialization(Nodes, [], length(Nodes)),
    log:debug("~n----- NodesInitialized = ~p~n", [NodesInitialized]),
    NodesInitTest = Nodes =:= NodesInitialized,
    log:debug("~n----- NodesInitTest = ~p~n", [NodesInitTest]),

    {NodesInitTest, Nodes, SupNode, BootstrapNodes}.


wait_for_node_initialization(_, NodesInitialized, 0) -> NodesInitialized;
wait_for_node_initialization([Node | Rest], NodesInitialized, NodesNum) ->
    receive
        node_initialized -> 
            wait_for_node_initialization(Rest, NodesInitialized ++ [Node], NodesNum - 1)
    after 10000 -> 
        wait_for_node_initialization(Rest, NodesInitialized, NodesNum - 1)
    end.


% Send a ping from Sender node to Receiver node to check if the latter is alive
test_ping({_SenderId, SenderPid}, {_ReceiverId, ReceiverPid}) ->
    log:info("~n~n----- Testing PING operation -----~n"),
    SenderPid ! {'ping_request', ReceiverPid, self()},
    receive
        alive -> 
            log:debug("~nReceived 'alive'~n"), 
            true;
        dead -> 
            log:debug("~nReceived 'dead'~n"),  
            false
    after 10000 -> 
        log:debug("Timeout in test_ping~n"), 
        false
    end.    

% Store value on K closest nodes to the corresponding hash key
test_store({_NodeId, NodePid}, Value) ->
    log:info("~n~n----- Testing STORE operation -----~n"),
    Ref1 = make_ref(),
    NodePid ! {store_request, Value, self(), Ref1},
    receive
        {store_complete, Ref2} when Ref2 =:= Ref1 -> 
            log:debug("Store complete in test_store~n"),
            true
    after 5000 -> 
        log:debug("Timeout in test_store~n"),
        false
    end.

% Search K closest nodes to ID
test_find_node({_NodeId, NodePid}, ID, Nodes, BootstrapNodes) ->
    log:info("~n~n----- Testing FIND_NODE operation -----~n"),
    NodePid ! {find_node_request, ID, self()},
    receive
        {find_node_response, Response} -> 
            log:debug("Received find_node_response with response = ~p", [Response]),

            % Remove bootstrap nodes and myself  from lookup result
            BootstrapPids = [Pid || {ok, Pid} <- BootstrapNodes],
            FilteredResponse = [Entry || Entry = {_Id, Pid} <- Response, 
                not lists:member(Pid, BootstrapPids)],
            NodeIds = [Id || {Id, Pid} <- Nodes, Pid =/= NodePid],
            ResponseIds = [Id || {Id, _Pid} <- FilteredResponse],

            % Check if response contains all the other expected nodes
            log:debug("NodeIds = ~p", [NodeIds]),
            log:debug("ResponseIds = ~p", [ResponseIds]),
            lists:sort(ResponseIds) =:= lists:sort(NodeIds)
    after 5000 -> false
    end.

% Find value by key
test_find_value({_NodeId, NodePid}, Key, Value) ->
    log:info("~n~n----- Testing FIND_VALUE operation -----~n"),
    NodePid ! {find_value_request, Key, self()},
    receive
        {find_value_response, {ValueFound, _Expiry, _Owner}} -> 
            log:debug("Entry found in test_find_value: ~p~n", [{ValueFound, _Expiry, _Owner}]),
            ValueFound =:= Value;
        {find_value_not_found} -> false
    after 2000 -> false
    end.

test_value_republishing(Key) ->
    log:info("~n~n----- Testing REPUBLISH operation -----~n"),
    % Add a new node to the network
    {ok, Node3, _Nodo3Name} = node:start_simple_node(nodo3, self()),
    {_NodeId, NodePid} = Node3,

    % Wait for node initialization and republish from another node
    SleepTime = 10000,
    Seconds = SleepTime/1000,
    log:info("~n~n----- WAITING ~p SECONDS FOR REPUBLISH -----~n", [Seconds]),
    timer:sleep(SleepTime),

    % Check if this new node has the value which was republished by other nodes
    NodePid ! {get_storage_entry_request, Key, self()},
    receive 
        {'get_storage_entry_response', {entry, Entry}} -> 
            log:info("Value found in test_value_republishing: ~p~n", [Entry]),
            {true, Node3};
        {'get_storage_entry_response', not_found} ->
            log:info("Value NOT found in test_value_republishing~n"),
            {false, Node3}
    after 5000 -> 
        log:info("Timeout in test_value_republishing~n"),
        {false, Node3}
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
     end || {_, Pid} <- Nodes],
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


prepare_network(Options, BoostrapCount, NodeCount, TestValue) ->
    
    log:important("---- STARTING THE NETWORK WITH ~p  BOOTSTRAP NODES ----", [BoostrapCount]),
    {ok, SupPid, _} = node:start_network(BoostrapCount, Options),

    log:important("---- CREATION OF ~p NODES ----", [NodeCount]),
    Nodes = case create_nodes(NodeCount - 1) of
        [] -> 
            log:error("Failed to create nodes"),
            exit(no_nodes_created);
        Ns -> 
            case wait_for_nodes_initialization(Ns) of
                ok -> Ns;
                error -> exit(nodes_initialization_failed)
            end
    end,
    log:important("---- ALL ~p NODES CREATED AND INITIALIZED ----", [length(Nodes)]),

    Self = self(),
    RefreshSample = lists:sublist(Nodes, max(1, length(Nodes) div 5)),
    log:important("---- REFRESHING ~p NODES ----", [length(RefreshSample)]),
    lists:foreach(fun({_, Pid}) -> Pid ! {refresh_buckets, Self} end, RefreshSample),
    Refreshed = gather_refresh_acks(length(RefreshSample), 10000),
    log:important("Received ~p / ~p refresh_buckets_complete responses", [Refreshed, length(RefreshSample)]),

    log:important("---- SENDING STORE_REQUEST TO RANDOM NODE ----"),

    RandomIndex = rand:uniform(NodeCount - 1),
    RandomStoreNode = lists:nth(RandomIndex, Nodes),
    {_, RandomPid} = RandomStoreNode,
    log:important("Selected a random store node: ~p", [RandomStoreNode]),
    
    Ref1 = make_ref(),
    RandomPid ! {store_request, TestValue, self(), Ref1},
    StoreNodes = receive 
        {store_complete, AckNodes, Ref2} when Ref1 =:= Ref2 -> 
            AckNodes 
    after 10000 -> 
        []
    end,
    log:important("Received ~p STORE acks", [length(StoreNodes)]),

    {Nodes, [RandomStoreNode | StoreNodes], SupPid}.

measure_random_lookup({_RandomId, RandomPid}, TestKey) ->
    log:important("---- INITIATING LOOKUP FOR VALUE ----"),

    {Time, Result} = timer:tc(fun() -> 
        RandomPid ! {find_value_request, TestKey, self()},
        receive
            {find_value_response, _} -> 
                log:important("---- VALUE FOUND ----"),
                {ok, found};
            {find_value_not_found} -> 
                log:important("---- VALUE NOT FOUND ----"),
                {error, not_found}
        after 5000 -> 
            log:important("---- TIMEOUT ----"),
            {error, timeout}
        end
    end),

    TimeMS = Time / 1000,
    log:important("Lookup time: ~.2f ms~n", [TimeMS]),
    {TimeMS, Result}.

measure_avg_random_lookup(N, BootstrapCount, NodeCount) ->
    process_flag(trap_exit, true),
    log:start(),
    log:set_level(important),

    TestValue = 1234567,
    TestKey = utils:calculate_key(TestValue),
    CustomOpts = #{
        k_param => 20,
        alpha_param => 3,
        republish_interval => 3600000,
        expiration_interval => 86400000,
        refresh_interval => 3600000
    },

    {Nodes, StoreNodes, SupPid} = prepare_network(CustomOpts, BootstrapCount, NodeCount, TestValue),

    % Select random node without the store value
    EligibleNodes = [Node || Node <- Nodes, not lists:member(Node, StoreNodes)],
    
    % Perform N successful measurements with retries
    {SuccessfulTimes, TotalAttempts} = perform_measurements(N, EligibleNodes, TestKey, [], 0),
    
    case SuccessfulTimes of
        [] ->
            io:format("Failed to complete any successful lookups after ~p total attempts~n", [TotalAttempts]),
            Avg = 0;
        _ ->
            Avg = lists:sum(SuccessfulTimes) / length(SuccessfulTimes),
            io:format("~p Successful Lookup measurements (~p total attempts): ~p~n", 
                     [length(SuccessfulTimes), TotalAttempts, SuccessfulTimes]),
            io:format("~nAverage lookup time (~p successful runs): ~.2f ms~n", [length(SuccessfulTimes), Avg])
    end,

    terminate_nodes(Nodes, SupPid),
    
    Avg.

perform_measurements(0, _EligibleNodes, _TestKey, AccTimes, AccAttempts) ->
    {lists:reverse(AccTimes), AccAttempts};
perform_measurements(Remaining, EligibleNodes, TestKey, AccTimes, AccAttempts) ->
    % Select a random node for this attempt
    RandomIndex = rand:uniform(length(EligibleNodes)),
    RandomLookupNode = lists:nth(RandomIndex, EligibleNodes),
    log:important("---- MEASUREMENT ~p / ~p ----", [length(AccTimes) + 1, Remaining + length(AccTimes)]),
    log:important("Selected a random lookup node: ~p", [RandomLookupNode]),

    {Time, Result} = measure_random_lookup(RandomLookupNode, TestKey),
    
    case Result of
        {ok, found} ->
            % Successful lookup - add to results and continue
            perform_measurements(Remaining - 1, EligibleNodes, TestKey, [Time | AccTimes], AccAttempts + 1);
        _ ->
            % Failed lookup - retry with same remaining count
            log:important("Lookup failed, retrying..."),
            perform_measurements(Remaining, EligibleNodes, TestKey, AccTimes, AccAttempts + 1)
    end.

create_nodes(0) -> [];
create_nodes(N) ->
    case node:start_simple_node(list_to_atom("node_" ++ integer_to_list(N)), self()) of
        {ok, Node, _} -> [Node | create_nodes(N-1)];
        _ -> 
            log:error("Failed to create node ~p", [N]),
            create_nodes(N-1)
    end.

wait_for_nodes_initialization([]) -> ok;
wait_for_nodes_initialization([{_Id, Pid}|Rest]) ->
    Pid ! {get_buckets_request, self()},
    receive
        {get_buckets_response, _} -> wait_for_nodes_initialization(Rest)
    after 5000 -> 
        log:error("Timeout waiting for node ~p initialization", [Pid]),
        wait_for_nodes_initialization(Rest)
    end.

gather_refresh_acks(0, _) -> 0;
gather_refresh_acks(Remaining, Timeout) ->
    receive
        {refresh_buckets_complete} ->
            1 + gather_refresh_acks(Remaining - 1, Timeout)
    after Timeout ->
        gather_refresh_acks(Remaining - 1, Timeout)
    end.
