-module(node).
-export([start_network/1, start_network/2, start_simple_node/2, init_node/4]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


start_network(BootstrapCount) ->
    DefaultOptions = #{
        k_param => 20,
        alpha_param => 3,
        republish_interval => 3600000, % 1 hour
        expiration_interval => 86400000, % 24 hours
        refresh_interval => 3600000 % 1 hour
    },
    start_network(BootstrapCount, DefaultOptions).


start_network(BootstrapCount, Options) ->
    % Create logger
    log:start(),

    % Create ETS table for shared options
    case ets:info(network_options) of
        undefined -> 
            % Table doesn't exist, I create it
            ets:new(network_options, [named_table, public, {keypos, 1}]);
        _ ->
            ets:delete_all_objects(network_options)
    end,
    ets:insert(network_options, {global_options, Options}),

    % Start bootstrap supervisor
    log:info("Starting bootstrap supervisor "),
    {ok, SupPid} = bootstrap_sup:start_link(),

    % Create ETS table for bootstrap nodes
    case ets:info(bootstrap_nodes) of
        undefined ->
            ets:new(bootstrap_nodes, [named_table, public, {keypos, 1}]),
            log:info("Table bootstrap_nodes created");
        _ ->
            log:info("Table bootstrap_nodes already created")
    end,

    % Start bootstrap workers
    BootstrapNodes = [start_bootstrap_node(list_to_atom("bootstrap" ++ integer_to_list(N)))
        || N <- lists:seq(1, BootstrapCount)],
    {ok, SupPid, BootstrapNodes}.


% Create a new Kademlia node with a random NodeID
start_simple_node(NodeName, From) ->
    log:info("Starting node with name: ~p", [NodeName]),

    % Select a bootstrap node from available bootstrap nodes
    BootstrapNodes = ets:match_object(bootstrap_nodes, {'_', '_', '_'}),
    BootstrapNode = case BootstrapNodes of
        [] -> exit(no_bootstrap_node_found);
        _ -> 
            {_Name, ID, Pid} = lists:nth(rand:uniform(length(BootstrapNodes)), BootstrapNodes),
            {ID, Pid}  
    end,
    
    log:info("Selected Bootstrap node ~p for node ~p", [BootstrapNode, NodeName]),

    % Generate random 160 bit Kademlia ID
	NodeId = utils:generate_node_id_160bit(),

	NodePid = spawn(fun() -> init_node(NodeId, BootstrapNode, NodeName, From) end),
    log:info("Spawned node: ~p", [{NodeName, NodePid}]),
	{ok, {NodeId, NodePid}, NodeName}.


% Initialization function for a Kademlia node
init_node(ID, BootstrapNode, NodeName, From) ->
    log:info("~p: node initialization", [NodeName]),

    % Get global options from ETS table
    [{global_options, NetworkOpts}] = ets:lookup(network_options, global_options),

    % Combine with node specific variables
    Storage = #{}, 
    Buckets = lists:duplicate(160, []),
    State = maps:merge(NetworkOpts, #{
        local_id => ID,
        main_pid => self(),
        node_name => NodeName,
        buckets => Buckets, 
        storage => Storage
    }),
    K = maps:get(k_param, State), 
    LocalId = maps:get(local_id, State), 
    RefreshInterval = maps:get(refresh_interval, State), 

    % Start a timer that will send 'refresh_buckets' every RefreshInterval
    timer:send_interval(RefreshInterval, self(), refresh_buckets),

    % Start a timer that will send 'check_expiration' every minute
    timer:send_interval(60000, self(), check_expiration),

    case BootstrapNode of
        undefined ->
            % First bootstrap node
            node_loop(State);
        _ -> 
            % Simple node (or bootstrap node other than first) 
            % Insert bootstrap node in buckets
            log:debug("~p: update buckets  with bootstrap node ~p", [NodeName, BootstrapNode]),
            UpdatedBuckets = update_routing_table([BootstrapNode], LocalId, Buckets, K),
            log:debug("~p:-------------updated buckets ~p", [NodeName, UpdatedBuckets]),

            % Search nodes closer to local ID (lookup of itself)
            log:info("Executing self lookup for node ~p", [NodeName]),
            {nodes, _, NewBuckets} = nodes_lookup(ID, [], [], false, find_node, 
                State#{buckets => UpdatedBuckets}),
            log:debug("~p:-------------updated buckets after initial lookup = ~p", [NodeName, NewBuckets]),
            From ! node_initialized,
            node_loop(State#{buckets => NewBuckets})
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


node_loop(State) ->
    NodeName = maps:get(node_name, State),
    K = maps:get(k_param, State),
    LocalId = maps:get(local_id, State),
    Alpha = maps:get(alpha_param, State),
    
    Expiration = maps:get(expiration_interval, State),
    RepublishInterval = maps:get(republish_interval, State),
    Storage = maps:get(storage, State),   
    Buckets = maps:get(buckets, State),   
    Self = self(),

    % calculate next republish time
    NextRepublishTime  = calculate_next_check_time(Storage, RepublishInterval),
    CurrentTime = erlang:system_time(millisecond),
	Timeout = max(0, NextRepublishTime  - CurrentTime),

	receive

        {store_request, Value, FromPid, Ref} ->
            log:info("~p: received message 'store_request' from ~p", [NodeName, FromPid]),
            % First find k closest nodes to the key
            Key = utils:calculate_key(Value),
            {nodes, ClosestNodes, NewBuckets} = nodes_lookup(Key, [], [], false, find_node, State),

            Now = erlang:system_time(millisecond),
            Expiry = Now + Expiration,

            % Data: Key, Value, Expiration time, Owner PID
            Data = {Key, {Value, Expiry, {LocalId, Self}}},

            % Send STORE message to closest nodes and wait for Ack
            log:important("Sending store to ~p nodes", [length(ClosestNodes)]),
            StoreNodes = [begin
                NodePid ! {'STORE', Data, {LocalId, Self}},
                {NodeId, NodePid}
            end || {NodeId, NodePid} <- ClosestNodes],

            % Return nodes that aknowledged the store
            AckNodes = collect_store_acks(length(StoreNodes), [], StoreNodes, 3000),
            log:important("Collected ~p store acks", [length(AckNodes)]),
            FromPid ! {store_complete, AckNodes, Ref},

            node_loop(State#{buckets => NewBuckets});

        {check_expiration} ->
            % Remove expired entries
            CurrentTime = erlang:system_time(millisecond),
            maps:filter( 
                fun(_, {_, ExpiryTime, _, _}) -> 
                    ExpiryTime > CurrentTime 
                end, 
                Storage 
            );

        {refresh_buckets, FromPid} ->
            log:info("~p: initiating buckets REFRESH", [NodeName]),
            NumBucketsToRefresh = 5,
            log:important("~p: REFRESHING ~p RANDOM BUCKETS", [NodeName, NumBucketsToRefresh]),
            NewState = refresh_buckets(NumBucketsToRefresh, [], State, NodeName),
            FromPid ! {refresh_buckets_complete},
            node_loop(NewState);

        {find_node_request, TargetId, FromPid} ->
            log:info("~p: received message 'find_node_request' from ~p", [NodeName, FromPid]),
            {nodes, ClosestNodes, NewBuckets} = nodes_lookup(TargetId, [], [], false, find_node, State),
            log:debug("~p: closest nodes found = ~p", [NodeName, ClosestNodes]),
            FromPid ! {find_node_response, ClosestNodes},
            node_loop(State#{buckets => NewBuckets});

        {find_value_request, Key, FromPid} ->
            log:info("~p: received message 'find_value_request' from ~p", [NodeName, FromPid]),

            % First check local storage
            Result = get_entry_from_storage(Key, Storage, NodeName),
            UpdatedBuckets = case Result of
                {entry, Entry} when Entry =/= not_found ->
                    FromPid ! {find_value_response, Entry},
                    Buckets;
                {entry, not_found} ->
                    % Not found locally, do network lookup
                    log:debug("---Not found locally, do network lookup "),
                    case value_lookup(Key, State) of
                        {value, Value, NewBuckets} when Value =/= not_found ->
                            FromPid ! {find_value_response, Value},
                            NewBuckets;
                        {value, not_found, NewBuckets} ->
                            FromPid ! {find_value_not_found},
                            NewBuckets
                    end
            end,
            node_loop(State#{buckets => UpdatedBuckets});

        {ping_request, NodePid, FromPid} ->
            log:info("~p: received message 'ping_request' from ~p", [NodeName, FromPid]),
            Result = ping_request(NodePid, LocalId),
            case Result of
                alive -> 
                    log:info("~p: received 'PONG' from ~p", [NodeName, NodePid]),
                    FromPid ! alive;
                dead ->
                    FromPid ! dead
            end,
            node_loop(State);

        {get_storage_request, FromPid} ->
            log:info("~p: received message 'get_storage_request' from ~p", [NodeName, FromPid]),
            FromPid ! {storage_dump, Storage},
            node_loop(State);

        {get_storage_entry_request, Key, FromPid} ->
            log:info("~p: received message 'get_storage_entry_request' from ~p", [NodeName, FromPid]),
            Result = get_entry_from_storage(Key, Storage, NodeName),
            case Result of
                {entry, Entry} when Entry =/= not_found ->
                    FromPid ! {get_storage_entry_response, {entry, Entry}};
                {entry, not_found} ->
                    FromPid ! {get_storage_entry_response, not_found}
            end,
            node_loop(State);

        {get_buckets_request, FromPid} ->
            log:info("~p: received message 'get_buckets_request' from ~p", [NodeName, FromPid]),
            FromPid ! {get_buckets_response, Buckets},
            node_loop(State);

		{'PING', {FromID, FromPid}} -> 
            log:info("~p: received 'PING' from ~p", [NodeName, FromPid]),
			FromPid ! {'PONG', Self},
            NewBuckets = update_routing_table([{FromID, FromPid}], LocalId, Buckets, K),
			node_loop(State#{buckets => NewBuckets});

		{'STORE', Data, FromNode} ->
            log:info("~p: received 'STORE' from ~p", [NodeName, FromNode]),
            log:debug("~p: entry to store -> ~p", [NodeName, Data]),

			% save pair inside the hashtable
            {Key, Entry} = Data,
            UpdatedStorage = handle_store(Key, Entry, Storage, RepublishInterval),

            NewBuckets = update_routing_table([FromNode], LocalId, Buckets, K),

            % Send ACK back to sender
            {_, FromPid} = FromNode,
            FromPid ! {'STORE_ACK', self()},

			node_loop(State#{buckets => NewBuckets, storage => UpdatedStorage});

		{'FIND_NODE', TargetId, FromNode} ->
            {FromId, FromPid} = FromNode,
            log:info("~p: received 'FIND_NODE' from ~p", [NodeName, FromNode]),

			% add the node 'From' to the routing table and wait for ack
            NewBuckets = update_routing_table([{FromId, FromPid}], LocalId, Buckets, K),

            % Send K closest nodes to target ID in the routing table
            ClosestNodes = get_closest_from_buckets(Buckets, TargetId, 159, Alpha, []),
            FromPid ! {'FIND_NODE_RESULT', ClosestNodes, TargetId},

			node_loop(State#{buckets => NewBuckets});

		{'FIND_VALUE', Key, FromNode} ->
            {FromId, FromPid} = FromNode,
            log:info("~p: received 'FIND_VALUE' from ~p", [NodeName, FromNode]),

			% add the node 'From' to the routing table and wait for ack
			NewBuckets = update_routing_table([{FromId, FromPid}], LocalId, Buckets, K),

            case get_entry_from_storage(Key, Storage, NodeName) of
                {entry, Entry} when Entry =/= not_found ->
                    FromPid ! {'FIND_VALUE_RESULT', {value, Entry}, Key};
                {entry, not_found} ->
                    ClosestNodes = get_closest_from_buckets(Buckets, Key, 159, Alpha, []),
                    FromPid ! {'FIND_NODE_RESULT', ClosestNodes, Key}
            end,
			node_loop(State#{buckets => NewBuckets})
    after Timeout  ->
        % Find values to republish
        ValuesToRepublish = find_values_to_republish(Storage),

        % Republish values
        {NewStorage, NewBuckets} = republish_values(ValuesToRepublish, Storage, State),
        node_loop(State#{storage => NewStorage, buckets => NewBuckets})
	end.


start_bootstrap_node(Name) ->
    log:info("Starting bootstrap node: ~p", [Name]),
    ChildSpec = {Name,
                 {bootstrap_node, start_link, [Name]}, 
                 transient,
                 5000,
                 worker,
                 [bootstrap_node]},
    supervisor:start_child({global, bootstrap_sup}, ChildSpec).


ping_request(NodePid, LocalId) ->
    NodePid ! {'PING', {LocalId, self()}},
    receive 
        {'PONG', _FromPingNode} -> 
            alive
    after 10000 ->
        dead
    end.


value_lookup(Key, State) ->
    case nodes_lookup(Key, [], [], false, find_value, State) of
        {value, Entry, NewBuckets} when Entry =/= not_found ->
            {value, Entry, NewBuckets};
        {value, not_found, NewBuckets} ->
            {value, not_found, NewBuckets}
    end.


nodes_lookup(TargetID, VisitedNodes, OrderedUniqueNodes, Terminate, Mode, State) ->
    Storage = maps:get(storage, State),
    Buckets = maps:get(buckets, State),
    NodeName = maps:get(node_name, State),
    
    case Mode of
		find_value ->
			case get_entry_from_storage(TargetID, Storage, NodeName) of
				{entry, Value} when Value =/= not_found ->
					{value, Value, Buckets};
				{entry, not_found} ->
					perform_lookup(TargetID, VisitedNodes, OrderedUniqueNodes, Terminate, 
                        Mode, State)
			end;
		find_node ->
			perform_lookup(TargetID, VisitedNodes, OrderedUniqueNodes, Terminate, Mode, State)
	end.


perform_lookup(TargetID, VisitedNodes, OrderedUniqueNodes, Terminate, Mode, State) ->
    K = maps:get(k_param, State),
    Alpha = maps:get(alpha_param, State),
    LocalId = maps:get(local_id, State),
    Buckets = maps:get(buckets, State),
    Self = self(),

    % Get closest nodes
    ClosestNodes = case OrderedUniqueNodes of
        [] ->
            % first lookup of Alpha closest nodes
			get_closest_from_buckets(Buckets, TargetID, 159, Alpha, []);
        [_|_] -> 
			% next lookups of Alpha closest nodes not yet visited
			NodesNotVisited = [Node || {_NodeId, NodePid} = Node <- OrderedUniqueNodes, 
                not lists:member(Node, VisitedNodes), NodePid =/= Self],
			lists:sublist(NodesNotVisited, Alpha)
	end,

    % If no more nodes to query, return what we have
    case ClosestNodes of
        [] ->
            case Mode of
                find_node -> 
                    {nodes, lists:sublist(OrderedUniqueNodes, K), Buckets};
                find_value ->
                    {value, not_found, Buckets}
            end;
        _ ->
            % Query closest nodes
            QueryResults = find_query(ClosestNodes, TargetID, Mode, LocalId),

            case QueryResults of
                {value, Value} ->
                    {value, Value, Buckets};
                {nodes, ReturnedNodes} ->
                    % Remove myself from returned node
                    FilteredNodes = [Node || {_NodeId, NodePid} = Node <- ReturnedNodes,
                        NodePid =/= Self],

                    % Update routing table with new nodes returned
                    NewBuckets = update_routing_table(FilteredNodes, LocalId, Buckets, K),

                    % update visited nodes list and ordered nodes list
                    NewVisitedNodes = VisitedNodes ++ ClosestNodes,
                    NewOrderedUniqueNodes = utils:order_unique(OrderedUniqueNodes ++ FilteredNodes, 
                        TargetID),

                    case Terminate of
                        true ->
                            % return nodes or value found and stop recursion
                            case Mode of
                                find_node ->
                                    {nodes, lists:sublist(NewOrderedUniqueNodes, K), NewBuckets};
                                find_value ->
                                    {value, not_found, NewBuckets}
                            end;
                        false ->
                            KeepQuerying = checkCloserNodes(NewOrderedUniqueNodes, NewVisitedNodes, 
                                TargetID),
                            case KeepQuerying of
                                true -> 
                                    % lookup for Alpha other nodes not yet visited
                                    nodes_lookup(TargetID, NewVisitedNodes, NewOrderedUniqueNodes, 
                                        false, Mode, State#{buckets => NewBuckets});
                                false ->
                                    % I make one last query for all k closer nodes not yet visited and 
                                    % terminate lookup process
                                    NewState = State#{alpha_param => K, buckets => NewBuckets},
                                    nodes_lookup(TargetID, NewVisitedNodes, NewOrderedUniqueNodes, 
                                        true, Mode, NewState)
                            end
                    end
            end
    end.


% Return {value, Value} or {nodes, Nodes}
find_query(Nodes, TargetID, Mode, LocalId) ->
    Self = self(),

    % Send queries to nodes
    lists:foreach(fun({_NodeId, NodePid}) ->
        case Mode of
            find_node ->
                NodePid ! {'FIND_NODE', TargetID, {LocalId, Self}};
            find_value ->
                NodePid ! {'FIND_VALUE', TargetID, {LocalId, Self}}
        end
    end, Nodes),

    collect_results(length(Nodes), Mode, [], undefined, TargetID).


collect_results(0, _Mode, CollectedNodes, Value, _TargetID) ->
    case Value of
        undefined -> {nodes, CollectedNodes};
        _ -> {value, Value}
    end;
collect_results(RemainingResponses, Mode, CollectedNodes, Value, TargetID) ->
    % If value has been found, don't wait for other responses
    case Mode =:= find_value andalso Value =/= undefined of
        true -> 
            {value, Value};
        false ->
            receive
                {'FIND_NODE_RESULT', Nodes, TargetID} ->
                    % add nodes to collected results
                    NewNodes = lists:usort(CollectedNodes ++ Nodes),
                    collect_results(RemainingResponses - 1, Mode, NewNodes, Value, TargetID);
                
                {'FIND_VALUE_RESULT', {value, FoundValue}, TargetID} ->
                    % this node returned the value
                    collect_results(RemainingResponses - 1, Mode, CollectedNodes, FoundValue, TargetID)
            after 2000 ->
                % Timeout for unresponsive nodes
                collect_results(RemainingResponses - 1, Mode, CollectedNodes, Value, TargetID)
            end
    end.


% checks if list L1 has closer nodes than list L2
checkCloserNodes(L1, L2, TargetID) ->
    case {L1, L2} of
        {[], _} -> false;
        {_, []} -> true;
        _ ->
            OrderedL1 = utils:order_unique(L1, TargetID),
            OrderedL2 = utils:order_unique(L2, TargetID),
            {ID1, _Pid1} = hd(OrderedL1),
            {ID2, _Pid2} = hd(OrderedL2),
            utils:xor_distance(ID1, TargetID) < utils:xor_distance(ID2, TargetID)
    end.


% Simple and direct implementation
collect_store_acks(0, SuccessfulNodes, _, _Timeout) ->
    % All nodes processed (either ACK received or timed out)
    SuccessfulNodes;
collect_store_acks(RemainingCount, SuccessfulNodes, AllNodes, Timeout) ->
    receive
        {'STORE_ACK', AckPid} ->
            % Find the node that sent the ACK in the complete nodes list
            case lists:keyfind(AckPid, 2, AllNodes) of
                {_AckID, AckPid} = Node ->
                    % Add to successful nodes and decrease counter
                    collect_store_acks(RemainingCount - 1, [Node | SuccessfulNodes], 
                                        AllNodes, Timeout);
                false ->
                    % Unexpected ACK, ignore
                    collect_store_acks(RemainingCount, SuccessfulNodes, 
                                        AllNodes, Timeout)
            end
    after Timeout ->
        collect_store_acks(RemainingCount - 1, SuccessfulNodes, 
                            AllNodes, Timeout)
    end.


update_routing_table([], _, RT, _) -> RT;
update_routing_table([Node | Rest], LocalId, RT, K) ->
    {NodeId, _Pid} = Node,
    case NodeId =:= LocalId of
        true ->
            update_routing_table(Rest, LocalId, RT, K);
        false ->
            BucketIndex = utils:find_bucket_index(LocalId, NodeId),
            Bucket = lists:nth(BucketIndex + 1, RT),
            NewRT = case lists:keyfind(NodeId, 1, Bucket) of
                {NodeId, _} = ExistingNode ->
                    NewBucket = lists:delete(ExistingNode, Bucket) ++ [Node],
                    replace_bucket(RT, BucketIndex, NewBucket);
                false ->
                    case length(Bucket) < K of
                        true ->
                            NewBucket = Bucket ++ [Node],
                            replace_bucket(RT, BucketIndex, NewBucket);
                        false ->
                            [LRUNode | RestBucket] = Bucket,
                            {_, LRUPid} = LRUNode,
                            log:debug("~p: request PING to ~p", [self(), LRUPid]),
                            Result = ping_request(LRUPid, LocalId),
                            case Result of
                                alive ->
                                    NewBucket = RestBucket ++ [LRUNode],
                                    replace_bucket(RT, BucketIndex, NewBucket);
                                _ ->
                                    NewBucket = RestBucket ++ [Node],
                                    replace_bucket(RT, BucketIndex, NewBucket)
                            end
                    end
            end,
            update_routing_table(Rest, LocalId, NewRT, K)
    end.


replace_bucket(RT, BucketIndex, NewBucket) ->
    % Split the routing table into parts before and after the bucket to replace
    {Prefix, [_OldBucket|Suffix]} = lists:split(BucketIndex, RT),
    
    % Combine the prefix, new bucket, and suffix to form the new routing table
    Prefix ++ [NewBucket] ++ Suffix.


% found Alpha nodes
get_closest_from_buckets(_, _, _, 0, Acc) ->
	Acc;
% traversed all the buckets,, returning the nodes found
get_closest_from_buckets(_, _, 0, _, Acc) -> 
    Acc;
get_closest_from_buckets(Buckets, TargetID, CurrentBucketIndex, Remaining, Acc) ->
	% get remaining nodes for the current bucket, or less if not available
	CurrentBucket = lists:nth(CurrentBucketIndex, Buckets),
	SortedBucket = lists:sort(
		fun({ID1, _Pid1}, {ID2, _Pid2}) -> 
			utils:xor_distance(ID1, TargetID) < utils:xor_distance(ID2, TargetID) 
		end,
		CurrentBucket
	),
	Available = length(SortedBucket),
	ToTake = min(Available, Remaining),
	NewNodes = lists:sublist(SortedBucket, ToTake),

	% update accumulator and remaining node
	NewAcc = Acc ++ NewNodes,
	NewRemaining = Remaining - ToTake,

	case NewRemaining of
		R when R =< 0 ->
			% got enough nodes
			NewAcc;
		_ ->
			% get nodes from the next closer bucket (expanding the search)
			NextBucketIndex = CurrentBucketIndex - 1,
			get_closest_from_buckets(Buckets, TargetID,  NextBucketIndex, NewRemaining, NewAcc)
    end.


get_entry_from_storage(Key, Storage, NodeName) ->
    case maps:find(Key, Storage) of
        {ok, Entry} ->
			log:debug("~p: Entry found in handle_get", [NodeName]),
			{Value, Expiry, _Republish, Owner} = Entry,
            {entry, {Value, Expiry, Owner}};
        error ->
			log:debug("~p: Entry NOT found in handle_get", [NodeName]),
            {entry, not_found}
    end.


% Handle a received STORE message
handle_store(Key, Entry, Storage, RepublishInterval) ->
	CurrentTime = erlang:system_time(millisecond),
	{Value, Expiry, Owner} = Entry,

	% Reset the republish time for the received value
    NewRepublishTime = CurrentTime + RepublishInterval,

    % Store the value with the provided expiry time and new republish time
    UpdatedStorage = maps:put(Key, {Value, Expiry, NewRepublishTime, Owner}, Storage),
    
    UpdatedStorage.


refresh_buckets(0, _, State, _NodeName) ->
    State;
refresh_buckets(Counter, Indexes, State, NodeName) ->
    LocalId = maps:get(local_id, State),
    Buckets = maps:get(buckets, State),
    MaxIndex = length(Buckets) - 1,

    % Trova un indice casuale che non sia già stato usato
    Available = [I || I <- lists:seq(0, MaxIndex), not lists:member(I, Indexes)],
    case Available of
        [] ->
            log:warning("No more buckets available to refresh"),
            State;
        _ ->
            RandomIndex = lists:nth(rand:uniform(length(Available)), Available),

            % Genera ID fittizio nel bucket selezionato
            TargetId = utils:generate_random_id_in_bucket(LocalId, RandomIndex),

            % Esegui il lookup su quell’ID
            {nodes, ClosestNodes, NewBuckets} = nodes_lookup(TargetId, [], [], false, find_node, State),
            log:important("~p: refresh_buckets - Bucket ~p: Got ~p closest nodes", [NodeName, RandomIndex, length(ClosestNodes)]),

            NewState = State#{buckets => NewBuckets},
            refresh_buckets(Counter - 1, [RandomIndex | Indexes], NewState, NodeName)
    end.



find_values_to_republish(Storage) ->
	CurrentTime = erlang:system_time(millisecond),
	maps:fold(fun(Key, {Value, Expiry, NextRepublishTime, Owner}, Acc) ->
		case NextRepublishTime =< CurrentTime of
			true -> 
				% this value needs to be republished
				[{Key, Value, Expiry, Owner} | Acc];
			false ->
				% not yet to be republished, skip this
				Acc
		end
	end,
	[],
	Storage
).	


republish_values([], Storage, State) ->
    Buckets = maps:get(storage, State),
    {Storage, Buckets};
republish_values([{Key, Value, Expiry, Owner} | Rest], Storage, State) ->
	CurrentTime = erlang:system_time(millisecond),
	LocalId = maps:get(local_id, State),
	RepublishInterval = maps:get(republish_interval, State),
    ExpirationInterval = maps:get(expiration_interval, State),
	{OwnerID, _OwnerPid} = Owner,

	% If the value republished is mine, update expiration time
	NewExpiry = case OwnerID =:= LocalId of
        true ->
            CurrentTime + ExpirationInterval;
        false ->
            Expiry
    end,

	% Update republish time and expiration time of the value in the local storage
	NextRepublishTime = CurrentTime + RepublishInterval,
	UpdatedStorage = maps:put(Key, {Value, NewExpiry, NextRepublishTime, Owner}, Storage),

	% Republish the entry to closest nodes
    {nodes, ClosestNodes, NewBuckets} = nodes_lookup(Key, [], [], false, find_node, State),

    % Send STORE message to K closest nodes
    Entry = {Value, NewExpiry, Owner},
    [NodePid ! {'STORE', Entry, {LocalId, self()}} || {_, NodePid} <- ClosestNodes],

	% continue with other values
	republish_values(Rest, UpdatedStorage, State#{buckets => NewBuckets}).


calculate_next_check_time(Storage, RepublishInterval) ->
	CurrentTime = erlang:system_time(millisecond),
	maps:fold(
        fun(_, {_, _, NextRepublishTime, _}, Earliest) ->
            min(NextRepublishTime, Earliest)
        end,
        CurrentTime + RepublishInterval, 
        Storage
    ).
