-module(node).
-export([start_network/0, start_network/1, start_simple_node/2, start_bootstrap_node/3, init_node/4]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%
%% Starts the Kademlia network with a certain number of bootstrap nodes and predefined options.
%%
start_network() ->
    DefaultOptions = #{
        k_param => 20,
        alpha_param => 3,
        id_byte_length => 5, % 5 bytes (40 bits)
        republish_interval => 3600000, % 1 hour
        expiration_interval => 86400000, % 24 hours
        refresh_interval => 3600000, % 1 hour
        check_expiration_interval => 60000, % 1 minute
        timeout_interval => 2000,
        log_level => info
    },
    start_network(DefaultOptions).


%%
%% Starts the Kademlia network with a certain number of bootstrap nodes and custom options.
%%
start_network(Options) ->
    % Set logger
    LogLevel = maps:get(log_level, Options, info),
    log:set_level_global(LogLevel),

    log:info("~n~nSTARTING NETWORK~n"),
    IdByteLength = maps:get(id_byte_length, Options, 20),
    log:info("ID byte length = ~p", [IdByteLength]),

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
    SupPid = try
        {ok, Pid} = bootstrap_sup:start_link(),
        Pid
    catch
        error:{badmatch, {error, {already_started, AlreadyStartedPid}}} ->
            log:warn("bootstrap_sup già avviato"),
            AlreadyStartedPid;
        _:Reason ->
            error_logger:error_msg("Errore nell'avvio di bootstrap_sup: ~p", [Reason]),
            erlang:error(Reason)
    end,

    % Create ETS table for bootstrap nodes
    case ets:info(bootstrap_nodes) of
        undefined ->
            ets:new(bootstrap_nodes, [named_table, public, {keypos, 1}]),
            log:info("Table bootstrap_nodes created");
        _ ->
            log:info("Table bootstrap_nodes already exists")
    end,

    {ok, SupPid}.


%%
%% Creates a new Kademlia node with a random NodeID.
%%
start_simple_node(LocalName, IdLength) ->
    log:info("~p: starting..", [LocalName]),
    ShellPid = self(),

    % Select a bootstrap node from available bootstrap nodes
    BootstrapNodes = ets:match_object(bootstrap_nodes, {'_', '_', '_'}),
    BootstrapNode = case BootstrapNodes of
        [] -> exit(no_bootstrap_node_found);
        _ -> 
            {Name, ID, Pid} = lists:nth(rand:uniform(length(BootstrapNodes)), BootstrapNodes),
            {Name, ID, Pid}  
    end,
    {BootstrapName, _BootstrapId, BootstrapPid} = BootstrapNode,
    log:info("~p: selected Bootstrap node ~p (~p)", [LocalName, BootstrapName, BootstrapPid]),

    % Generate random binary Kademlia ID
	NodeId = utils:generate_node_id(IdLength),

	NodePid = spawn(fun() -> init_node(NodeId, BootstrapNode, LocalName, ShellPid) end),
    log:info("~p (~p): spawned node", [LocalName, NodePid]),
	{ok, {LocalName, NodeId, NodePid}}.


%%
%% Startup of bootstrap node using Supervisor OTP
%%
start_bootstrap_node(Name, ShellPid, IdLength) ->
    log:info("~p: starting..", [Name]),
    ChildSpec = {Name,
                 {bootstrap_node, start_link, [Name, ShellPid, IdLength]}, 
                 transient,
                 5000,
                 worker,
                 [bootstrap_node]},
    supervisor:start_child({global, bootstrap_sup}, ChildSpec).



%%
%% Initializes a spawned Kademlia node before calling the node loop.
%%
init_node(ID, BootstrapNode, LocalName, ShellPid) ->
    Self = self(),
    log:info("~p (~p): node initialization", [LocalName, Self]),

    % Get global network options from ETS table
    [{global_options, NetworkOpts}] = ets:lookup(network_options, global_options),

    % Combine with node specific options
    IdByteLength = maps:get(id_byte_length, NetworkOpts, 20),
    IdBitLength = IdByteLength * 8,
    Storage = #{}, 
    Buckets = lists:duplicate(IdBitLength, []),
    VisitedNodes = [],
    NodesCollected = [],
    PendingRequests = [],
    LookupStatus = done,
    TimeoutRefs = #{},
    State = maps:merge(NetworkOpts, #{
        local_id => ID,
        self => self(),
        local_name => LocalName,
        buckets => Buckets, 
        storage => Storage,
        visited_nodes => VisitedNodes,
        nodes_collected => NodesCollected,
        lookup_status => LookupStatus,
        pending_requests => PendingRequests,
        timeout_refs => TimeoutRefs
    }),
    K = maps:get(k_param, State), 
    Alpha = maps:get(alpha_param, State),
    LocalId = maps:get(local_id, State), 
    CheckExpirationinterval = maps:get(check_expiration_interval, State, 60000),

    % Start a timer that will send 'refresh_buckets' every refresh_interval
    % timer:send_interval(RefreshInterval, self(), refresh_buckets),

    % Start a timer that will send 'check_expiration' every check_expiration_interval
    timer:send_interval(CheckExpirationinterval, self(), check_expiration),

    case BootstrapNode of
        undefined ->
            % First bootstrap node
            log:info("~p (~p): first bootstrap", [LocalName, Self]),
            ShellPid ! {find_node_response, []},
            node_loop(State);
        _ -> 
            % Simple node (or bootstrap node other than first) 

            % Insert bootstrap node in this node's bucket list
            {BootName, _BootId, BootPid} = BootstrapNode,
            log:debug("~p (~p): inserting bootstrap node ~p (~p)", [LocalName, Self, BootName, BootPid]),
            UpdatedBuckets = update_routing_table([BootstrapNode], LocalId, Buckets, K, LocalName,
                IdBitLength),
            utils:print_buckets(LocalName, Self, UpdatedBuckets),

            % Search nodes closer to local ID (lookup of itself)
            log:debug("~p (~p): executing self lookup..", [LocalName, Self]),
            NewState = nodes_lookup(LocalId, 
                                    State#{lookup_status => in_progress, 
                                            lookup_mode => find_node,
                                            requester => ShellPid,
                                            buckets => UpdatedBuckets}, 
                                    Alpha),
            node_loop(NewState)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%
%% Manages the Kademlia node's behaviour.
%%
node_loop(State) ->
    LocalName = maps:get(local_name, State),
    K = maps:get(k_param, State),
    LocalId = maps:get(local_id, State),
    Alpha = maps:get(alpha_param, State),
    IdByteLength = maps:get(id_byte_length, State, 20),
    IdBitLength = IdByteLength * 8,
    
    RepublishInterval = maps:get(republish_interval, State),
    Storage = maps:get(storage, State),   
    Buckets = maps:get(buckets, State),   
    Self = self(),

    % calculate next republish time
    NextRepublishTime  = calculate_next_check_time(Storage, RepublishInterval),
    CurrentTime = erlang:system_time(millisecond),
	Timeout = max(0, NextRepublishTime  - CurrentTime),

	receive

        {find_value_request, Key, FromPid} ->
            log:info("~p (~p): find_value_request from ~p", [LocalName, Self, FromPid]),

            Result = get_entry_from_storage(Key, Storage, LocalName),
            case Result of
                {entry, Entry} when Entry =/= not_found ->
                    % Entry found locally
                    log:debug("~p (~p): found value locally", [LocalName, Self]),
                    Hops = maps:get(num_hops, State, 0),
                    FromPid ! {find_value_response, Entry, Hops};
                {entry, not_found} ->
                    % Entry not found locally, do network lookup
                    log:debug("~p (~p): executing first VALUE lookup", [LocalName, Self]),
                    NewState = nodes_lookup(Key, 
                                            State#{lookup_status => in_progress, 
                                                    lookup_mode => find_value,
                                                    requester => FromPid},
                                            Alpha),
                    node_loop(NewState)
            end,
            node_loop(State);

        {find_node_request, TargetId, FromPid} ->
            log:info("~p (~p): find_node_request from ~p", [LocalName, Self, FromPid]),
            log:debug("~p (~p): executing first NODE lookup", [LocalName, Self]),
            NewState = nodes_lookup(TargetId, 
                                    State#{lookup_status => in_progress, 
                                            lookup_mode => find_node,
                                            requester => FromPid},
                                    Alpha),
            node_loop(NewState);

        {store_request, Value, FromPid} ->
            log:info("~p: store_request from ~p", [LocalName, FromPid]),
            Key = utils:calculate_key(Value, IdByteLength),
            NewState = nodes_lookup(Key, 
                                    State#{lookup_status => in_progress, 
                                            lookup_mode => store,
                                            requester => FromPid,
                                            value_to_store => Value},
                                    Alpha),
            node_loop(NewState);

        {ping_request, ReceiverNode, FromPid} ->
            Result = ping_request(ReceiverNode, LocalId, LocalName),
            case Result of
                alive -> 
                    FromPid ! alive;
                dead ->
                    FromPid ! dead
            end,
            node_loop(State);

        {check_expiration} ->
            log:info("~p (~p): check_expiration", [LocalName, Self]),
            % Remove expired entries
            CurrentTime = erlang:system_time(millisecond),
            NewStorage = maps:filter( 
                fun(_, {_, ExpiryTime, _, _}) -> 
                    ExpiryTime > CurrentTime 
                end, 
                Storage 
            ),
            node_loop(State#{storage => NewStorage});

        {'FIND_VALUE_RESPONSE', {value, Value}, _TargetID, FromNode} ->
            {FromName, _FromId, FromPid} = FromNode,
            case maps:get(lookup_status, State) of
                in_progress ->
                    % Value found, return value and terminate lookup process
                    log:debug("~p (~p): received VALUE from ~p (~p). Stopping lookup process", 
                        [LocalName, Self, FromName, FromPid]),
                    Requester = maps:get(requester, State),
                    Hops = maps:get(num_hops, State),
                    Requester ! {find_value_response, Value, Hops},
                    NewState = reset_lookup_state(State),
                    node_loop(NewState);
                done ->
                    % Lookup already terminated
                    log:debug("~p (~p): ignoring FIND_VALUE_RESPONSE from ~p (~p)", 
                        [LocalName, Self, FromName, FromPid]),
                    node_loop(State)
            end;

        {'FIND_VALUE_RESPONSE', {nodes, ReturnedNodes}, TargetID, FromNode} ->
            {FromName, _FromId, FromPid} = FromNode,
            case maps:get(lookup_status, State) of
                in_progress ->
                    % No value found, perform another lookup on returned nodes
                    log:debug("~p (~p): received a FIND_VALUE_RESPONSE from ~p (~p)", 
                        [LocalName, Self, FromName, FromPid]),
                    handle_lookup_response(ReturnedNodes, TargetID, FromNode, State);
                done ->
                    % Lookup already terminated
                    log:debug("~p (~p): ignoring FIND_VALUE_RESPONSE from ~p (~p)", 
                        [LocalName, Self, FromName, FromPid]),
                    node_loop(State)
            end;

        {'FIND_NODE_RESPONSE', {nodes, ReturnedNodes}, TargetID, FromNode} ->
            {FromName, _FromId, FromPid} = FromNode,
            case maps:get(lookup_status, State) of
                in_progress ->
                    log:debug("~p (~p): received a FIND_NODE_RESPONSE from ~p (~p)", 
                        [LocalName, Self, FromName, FromPid]),
                    handle_lookup_response(ReturnedNodes, TargetID, FromNode, State);
                done ->
                    % Lookup already terminated
                    log:debug("~p (~p): ignoring FIND_NODE_RESPONSE from ~p (~p)", 
                        [LocalName, Self, FromName, FromPid]),
                    node_loop(State)
            end;

        trigger_final_response -> 
            log:debug("~p (~p): lookup finalization", [LocalName, Self]),
            finalize_lookup(State);

        {republish_response, ok, {_AckNodes, NewBuckets}} ->
            log:debug("~p (~p): received a republish_response", [LocalName, Self]),
            node_loop(State#{buckets => NewBuckets});

        {refresh_response, ok, NewBuckets} ->
            log:debug("~p (~p): received a refresh_response", [LocalName, Self]),
            Requester = maps:get(requester, State),
            ToRefresh = maps:get(to_refresh, State),
            NewToRefresh = ToRefresh - 1,
            log:debug("~p (~p): ~p remaining buckets to refresh", [LocalName, Self, NewToRefresh]),
            case NewToRefresh of
                R when R =< 0 ->
                    % Refreshed all buckets
                    log:debug("~p (~p): REFRESH COMPLETE", [LocalName, Self]),
                    Requester ! {refresh_complete},
                    node_loop(State#{buckets => NewBuckets});
                _ ->
                    node_loop(State#{buckets => NewBuckets, to_refresh => NewToRefresh})
            end;

        % Message received when a pending request from a node goes in Timeout
        {timeout, NodePid} ->
            log:debug("~p (~p): lookup TIMEOUT for the node ~p", [LocalName, Self, NodePid]),
            handle_timeout(NodePid, State);

        {get_storage_request, FromPid} ->
            FromPid ! {storage_dump, Storage, LocalName},
            node_loop(State);

        {get_storage_entry_request, Key, FromPid} ->
            Result = get_entry_from_storage(Key, Storage, LocalName),
            case Result of
                {entry, Entry} when Entry =/= not_found ->
                    FromPid ! {get_storage_entry_response, {entry, Entry}};
                {entry, not_found} ->
                    FromPid ! {get_storage_entry_response, not_found}
            end,
            node_loop(State);

        {delete_storage_entry_request, Key, FromPid} ->
            log:debug("~p (~p): deleting key from storage", [LocalName, Self]),
            NewStorage = delete_entry_from_storage(Key, Storage),
            utils:print_storage(LocalName, Self, NewStorage),
            FromPid ! {delete_storage_entry_response, ok},
            node_loop(State#{storage => NewStorage});

        {get_buckets_request, FromPid} ->
            FromPid ! {buckets_dump, Buckets, {LocalName, LocalId, Self}},
            node_loop(State);

        {is_alive_request, FromPid} ->
            FromPid ! {is_alive_response, alive},
            node_loop(State);

        {refresh_buckets, ToRefresh, FromPid} ->
            NewState = State#{requester => FromPid, to_refresh => ToRefresh},
            handle_refresh(NewState),
            node_loop(NewState);

        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
        %% PROTOCOL INTERFACE MESSAGES %%
        %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

		{'PING', FromNode} -> 
            {FromName, _FromID, FromPid} = FromNode,
            log:info("~p (~p): PING message from ~p (~p)", [LocalName, Self, FromName, FromPid]),
			FromPid ! {'PONG', {LocalName, LocalId, Self}},

            % BUG: this sometimes generates a loop
            % NewBuckets = update_routing_table([FromNode], LocalId, Buckets, K, LocalName),
            % utils:print_buckets(LocalName, Self, NewBuckets),
            % node_loop(State#{buckets => NewBuckets});

			node_loop(State);

		{'STORE', Data, FromNode} ->
            {FromName, _FromId, FromPid} = FromNode,
            log:info("~p (~p): STORE message from ~p (~p)", [LocalName, Self, FromName, FromPid]),

			% Save pair inside the local storage
            {Key, Entry} = Data,
            UpdatedStorage = store_locally(Key, Entry, Storage, RepublishInterval),
            NewBuckets = update_routing_table([FromNode], LocalId, Buckets, K, LocalName, IdBitLength),

            % Print new state
            utils:print_buckets(LocalName, Self, NewBuckets),
            utils:print_storage(LocalName, Self, UpdatedStorage),

            % Send ACK back to sender
            log:debug("~p (~p): sending STORE_ACK to ~p (~p)", [LocalName, Self, FromName, FromPid]),
            FromPid ! {'STORE_ACK', self()},

			node_loop(State#{buckets => NewBuckets, storage => UpdatedStorage});

		{'FIND_NODE', TargetId, FromNode} ->
            {FromName, _FromId, FromPid} = FromNode,
            log:info("~p (~p): FIND_NODE message from ~p (~p)", [LocalName, Self, FromName, FromPid]),

			% Add the node 'FromPid' to the routing table
            NewBuckets = update_routing_table([FromNode], LocalId, Buckets, K, LocalName, IdBitLength),
            utils:print_buckets(LocalName, Self, NewBuckets),

            % Send back the K closest nodes to the target ID from this node's buckets
            ClosestNodes = get_closest_from_buckets(NewBuckets, TargetId, IdBitLength, K, [], LocalName),
            log:debug("~p (~p) - FIND_NODE: sending back ~p closest nodes", 
                [LocalName, Self, length(ClosestNodes)]),
            FromPid ! {'FIND_NODE_RESPONSE', {nodes, ClosestNodes}, TargetId, {LocalName, LocalId, Self}},

			node_loop(State#{buckets => NewBuckets});

		{'FIND_VALUE', Key, FromNode} ->
            {FromName, _FromId, FromPid} = FromNode,
            log:info("~p (~p): FIND_VALUE message from ~p (~p)", [LocalName, Self, FromName, FromPid]),

			% Add the node 'FromPid' to the routing table
			NewBuckets = update_routing_table([FromNode], LocalId, Buckets, K, LocalName, IdBitLength),
            utils:print_buckets(LocalName, Self, NewBuckets),

            % Search locally for value. If not found, return closest nodes.
            case get_entry_from_storage(Key, Storage, LocalName) of
                {entry, Entry} when Entry =/= not_found ->
                    log:debug("~p (~p) - FIND_VALUE: sending value found locally", [LocalName, Self]),
                    FromPid ! {'FIND_VALUE_RESPONSE', {value, Entry}, Key, {LocalName, LocalId, Self}};
                {entry, not_found} ->
                    ClosestNodes = get_closest_from_buckets(NewBuckets, Key, IdBitLength, K, [], LocalName),
                    log:debug("~p (~p) - FIND_VALUE: value NOT found, returning ~p closest nodes", 
                        [LocalName, Self, length(ClosestNodes)]),
                    FromPid ! {'FIND_VALUE_RESPONSE', {nodes, ClosestNodes}, Key, {LocalName, LocalId, Self}}
            end,
			node_loop(State#{buckets => NewBuckets})
    after Timeout  ->
        log:info("~p (~p): timeout for value REPUBLISHING", [LocalName, Self]),
        handle_republish(State)
	end.


reset_lookup_state(State) ->
    State#{lookup_status => done,
            pending_requests => [],
            nodes_collected => [],
            visited_nodes => [],
            timeout_refs => #{},
            is_last_query => false,
            num_hops => 0}.


handle_lookup_response(ReturnedNodes, TargetID, FromNode, State) ->
    LocalName = maps:get(local_name, State),
    LocalId = maps:get(local_id, State),
    IdByteLength = maps:get(id_byte_length, State, 20),
    IdBitLength = IdByteLength * 8,
    Buckets = maps:get(buckets, State),
    K = maps:get(k_param, State),
    Alpha = maps:get(alpha_param, State),
    PendingRequests = maps:get(pending_requests, State),
    NodesCollected = maps:get(nodes_collected, State),
    VisitedNodes = maps:get(visited_nodes, State),
    LookupMode = maps:get(lookup_mode, State),
    IsLastQuery = maps:get(is_last_query, State, false),
    Main = maps:get(self, State),

    % Remove FromNode from pending requests
    NewPending = lists:delete(FromNode, PendingRequests),
    {FromName, _, FromPid} = FromNode,
    log:debug("~p (~p): removed ~p (~p) from pending requests", [LocalName, Main, FromName, FromPid]),
    utils:print_nodes(NewPending, 
                        io_lib:format("~p (~p) - NewPending: ", [LocalName, Main])),

    % Remove my self from nodes received
    % log:debug("~p (~p): remove myself ~p from returned nodes", [LocalName, Main, self()]),
    FilteredNodes = [Node || {_, _NodeId, NodePid} = Node <- ReturnedNodes, 
        (NodePid =/= Main) andalso (NodePid =/= self())],
    % Update routing table
    NewBuckets = update_routing_table(FilteredNodes, LocalId, Buckets, K, LocalName, IdBitLength),
    utils:print_buckets(LocalName, Main, NewBuckets),

    % Update collected nodes
    NewNodesCollected = utils:order_unique(NodesCollected ++ ReturnedNodes, TargetID),
    utils:print_nodes(NewNodesCollected, io_lib:format("~p (~p) - nodes_lookup - NewNodesCollected: ", 
        [LocalName, Main])),

    NewState = State#{
        buckets => NewBuckets,
        nodes_collected => NewNodesCollected,
        pending_requests => NewPending
    },

    % Check if there are closer nodes
    case checkCloserNodes(FilteredNodes, VisitedNodes, TargetID) of
        true ->
            % Continue with lookup of Alpha closer nodes not yet visited
            log:debug("~p (~p): CONTINUE lookup - still some closer nodes", [LocalName, Main]),
            UpdatedState = nodes_lookup(TargetID, NewState, Alpha),
            case LookupMode of
                republish -> republish_loop(UpdatedState);
                refresh -> refresh_loop(UpdatedState);
                _ -> node_loop(UpdatedState)
            end;
        false ->
            log:debug("~p (~p): NO CLOSER NODES to lookup", [LocalName, Main]),

            % Check if there are pending requests
            case length(NewPending) of
                0 ->
                    % No pending requests 
                    case IsLastQuery of
                        false -> 
                            % Perform one last query for the K closest nodes not yet visited
                            log:debug("~p (~p): LAST lookup for K nodes not visited", [LocalName, Main]),
                            UpdatedState = nodes_lookup(TargetID, NewState#{is_last_query => true}, K),
                            case LookupMode of
                                republish -> republish_loop(UpdatedState);
                                refresh -> refresh_loop(UpdatedState);
                                _ -> node_loop(UpdatedState)
                            end;
                        true ->
                            finalize_lookup(NewState)
                    end;
                _ ->
                    % There are still pending requests
                    log:debug("~p (~p): CONTINUE - still some pending requests", [LocalName, Main]),
                    % utils:print_nodes(NewPending, 
                    %     io_lib:format("~p (~p) - NewPending: ", [LocalName, Main])),
                    case LookupMode of
                        republish -> republish_loop(NewState);
                        refresh -> refresh_loop(NewState);
                        _ -> node_loop(NewState)
                    end
            end
    end.


nodes_lookup(TargetID, State, Alpha) ->
    IdByteLength = maps:get(id_byte_length, State, 20),
    IdBitLength = IdByteLength * 8,
    Buckets = maps:get(buckets, State),
    LocalName = maps:get(local_name, State, unknown),
    VisitedNodes = maps:get(visited_nodes, State),
    NodesCollected = maps:get(nodes_collected, State),
    LookupMode = maps:get(lookup_mode, State),
    Hops = maps:get(num_hops, State, 0),
    Main = maps:get(self, State),
    Self = self(),

    ClosestNodes = select_closest_nodes(TargetID, Alpha, Buckets, NodesCollected, VisitedNodes, 
        Main, LocalName, IdBitLength),
    utils:print_nodes(ClosestNodes, io_lib:format("~p (~p) - nodes_lookup - ClosestNodes: ", 
        [LocalName, Main])),

    NewState = case ClosestNodes of
        [] ->
            log:debug("~p (~p): No more nodes to lookup", [LocalName, Main]),

            % Check if this was the last query and no pending requests
            IsLastQuery = maps:get(is_last_query, State, false),
            Pending = maps:get(pending_requests, State, []),
            case {IsLastQuery, Pending} of
                {true, []} ->
                    Self ! trigger_final_response,
                    State;
                _ ->
                    State
            end;
        _ ->
            % Query closest nodes
            {TimeoutRefs, Pending} = case LookupMode of
                find_value ->
                    find_query(ClosestNodes, TargetID, find_value, State);
                _ -> 
                    find_query(ClosestNodes, TargetID, find_node, State)
            end,
            NewVisitedNodes = VisitedNodes ++ ClosestNodes,
            utils:print_nodes(NewVisitedNodes, io_lib:format("~p (~p) - nodes_lookup - VisitedNodes: ", 
                [LocalName, Main])),
            NewHops = Hops + 1,
            State#{
                visited_nodes => NewVisitedNodes,
                timeout_refs => TimeoutRefs,
                pending_requests => Pending,
                num_hops => NewHops
            }
    end,
    NewState.

select_closest_nodes(TargetID, Alpha, Buckets, NodesCollected, VisitedNodes, Self, LocalName, IdBitLength) ->
    case NodesCollected of
        [] ->
            % First lookup of Alpha closest nodes from buckets
            log:debug("~p (~p) - nodes_lookup: get_closest_from_buckets", [LocalName, Self]),
            ClosestFromBuckets = get_closest_from_buckets(Buckets, TargetID, IdBitLength, Alpha, [], 
                LocalName),
            [Node || {_, _, NodePid} = Node <- ClosestFromBuckets, 
             not lists:member(Node, VisitedNodes), (NodePid =/= Self) andalso (NodePid =/= self())];
        _ ->
            % Next lookups of Alpha closest nodes not yet visited
            NodesNotVisited = [Node || {_, _, NodePid} = Node <- NodesCollected, 
                               not lists:member(Node, VisitedNodes), 
                               (NodePid =/= Self) andalso (NodePid =/= self())],
            lists:sublist(NodesNotVisited, Alpha)
    end.



%%
%% Query the nodes and set timeouts. Returns updated State.
%%
find_query(Nodes, TargetID, Mode, State) ->
    LocalName = maps:get(local_name, State),
    LocalId = maps:get(local_id, State),
    TimeoutInterval = maps:get(timeout_interval, State),
    TimeoutRefs0 = maps:get(timeout_refs, State),
    Pending0 = maps:get(pending_requests, State),
    MainPid = maps:get(self, State),
    Self = self(),

    log:debug("~p (~p) - find_query: sending ~p queries to ~p nodes", 
        [LocalName, MainPid, Mode, length(Nodes)]),

    % For each node, send the query, start a timeout timer and add to pending requests
    {TimeoutRefs, Pending} =
        lists:foldl(
            fun({_Name, _NodeId, NodePid} = Node, {RefsAcc, PendingAcc}) ->
                Ref = erlang:start_timer(TimeoutInterval, Self, {timeout, NodePid}),
                % Send the message
                case Mode of
                    find_node ->
                        NodePid ! {'FIND_NODE', TargetID, {LocalName, LocalId, Self}};
                    find_value ->
                        NodePid ! {'FIND_VALUE', TargetID, {LocalName, LocalId, Self}}
                end,
                {maps:put(NodePid, Ref, RefsAcc), [Node | PendingAcc]}
            end,
            {TimeoutRefs0, Pending0},
            Nodes
        ),
    {TimeoutRefs, Pending}.


finalize_lookup(State) ->
    LocalName = maps:get(local_name, State),
    Buckets = maps:get(buckets, State),
    K = maps:get(k_param, State),
    NodesCollected = maps:get(nodes_collected, State),
    LookupMode = maps:get(lookup_mode, State),
    Requester = maps:get(requester, State),
    ValueToStore = maps:get(value_to_store, State, undefined),
    Main = maps:get(self, State),

    % return results and stop lookup process
    log:debug("~p (~p): STOP LOOKUP - ZERO pending requests", [LocalName, Main]),
    case LookupMode of
        find_value -> 
            log:debug("~p (~p): sending not_found to shell (~p)", 
                [LocalName, Main, Requester]),
            Requester ! {find_value_response, not_found},
            ResetState = reset_lookup_state(State),
            node_loop(ResetState);
        find_node  -> 
            log:debug("~p (~p): sending K closest to shell (~p)", 
                [LocalName, Main, Requester]),
            ClosestK = lists:sublist(NodesCollected, K),
            Requester ! {find_node_response, ClosestK},
            ResetState = reset_lookup_state(State),
            node_loop(ResetState);
        store ->
            % Store value at closest nodes
            ClosestK = lists:sublist(NodesCollected, K),
            AckNodes = store_value_at_nodes(ClosestK, ValueToStore, State),

            log:debug("~p (~p): sending store ACKS to shell (~p)", 
                [LocalName, Main, Requester]),
            Requester ! {store_response, ok, AckNodes},
            ResetState = reset_lookup_state(State),
            node_loop(ResetState);
        republish ->
            % Store value at closest nodes
            ClosestK = lists:sublist(NodesCollected, K),
            AckNodes = store_value_at_nodes(ClosestK, ValueToStore, State),

            log:debug("~p (~p): sending republish ACKS to main process (~p)", 
                [LocalName, Main, Main]),
            Main ! {republish_response, ok, {AckNodes, Buckets}},
            ResetState = reset_lookup_state(State),
            republish_loop(ResetState);
        refresh ->
            log:debug("~p (~p): sending refresh ACK to main process (~p)", 
                [LocalName, Main, Main]),
            Main ! {refresh_response, ok, Buckets},
            ResetState = reset_lookup_state(State),
            refresh_loop(ResetState)
    end.

    
handle_timeout(NodePid, State) ->
    LocalName = maps:get(local_name, State),
    Main = maps:get(self, State),
    log:debug("~p (~p): timeout from ~p", [LocalName, Main, NodePid]),
    TimeoutRefs = maps:get(timeout_refs, State),
    PendingRequests = maps:get(pending_requests, State),

    % Remove node from timers and pending requests
    NewTimeouts = maps:remove(NodePid, TimeoutRefs),
    NewPending = lists:delete(NodePid, PendingRequests),
    NewState = State#{
        timeout_refs => NewTimeouts,
        pending_requests => NewPending
    },

    Mode = maps:get(lookup_mode, NewState),

    % If lookup still going and no more pending requests -> STOP
    case maps:get(lookup_status, NewState) of
        in_progress ->
            case NewPending of
                [] ->
                    Requester = maps:get(requester, NewState),
                    case Mode of
                        find_value ->
                            Requester ! {find_value_response, not_found};
                        find_node ->
                            Nodes = maps:get(nodes_collected, NewState),
                            Requester ! {find_node_response, Nodes};
                        republish ->
                            republish_loop(NewState);
                        refresh ->
                            refresh_loop(NewState)
                    end,
                    node_loop(NewState#{lookup_status => done});
                _ -> 
                    case Mode of
                        republish -> republish_loop(NewState);
                        refresh -> refresh_loop(NewState);
                        _ -> node_loop(NewState)
                    end
            end;
        _ -> 
            case Mode of
                republish -> republish_loop(NewState);
                refresh -> refresh_loop(NewState);
                _ -> node_loop(NewState)
            end
    end.



handle_republish(State) ->
    LocalName = maps:get(local_name, State),
    LocalId = maps:get(local_id, State),
    Storage = maps:get(storage, State), 
    RepublishInterval = maps:get(republish_interval, State),
    ExpirationInterval = maps:get(expiration_interval, State),
    Alpha = maps:get(alpha_param, State),
    Self = self(),

    % Find values to republish
    ValuesToRepublish = find_values_to_republish(Storage, LocalName),
    
    % Update republish times in storage
    CurrentTime = erlang:system_time(millisecond),
    NewRepublishTime = CurrentTime + RepublishInterval,
    UpdatedStorage = lists:foldl(
        fun({Key, {Value, Expiry, _, Owner}}, AccStorage) ->
            maps:put(Key, {Value, Expiry, NewRepublishTime, Owner}, AccStorage)
        end,
        Storage,
        ValuesToRepublish
    ),

    % Print new storage
    utils:print_storage(LocalName, Self, UpdatedStorage),

    % Spawn separate lookup process for each value to republish
    lists:foreach(
        fun({Key, {Value, Expiry, _, Owner}}) ->
            % If the value republished is mine, update also expiration time
            {_OwnerName, OwnerID, _OwnerPid} = Owner,
            NewExpiry = case OwnerID =:= LocalId of
                true -> CurrentTime + ExpirationInterval;
                false -> Expiry
            end,
            Entry = {Key, {Value, NewExpiry, Owner}},
            spawn(fun() ->
                LookupState = State#{
                    lookup_status => in_progress,
                    lookup_mode => republish,
                    main_process => Self,
                    value_to_store => Entry
                },
                NewState = nodes_lookup(Key, LookupState, Alpha),
                republish_loop(NewState)
            end)
        end,
        ValuesToRepublish
    ),
    node_loop(State#{storage => UpdatedStorage}).

republish_loop(State) ->
    LocalName = maps:get(local_name, State),
    LookupStatus = maps:get(lookup_status, State),
    MainPid = maps:get(main_process, State),

    % If lookup has terminated, kill this lookup process
    case LookupStatus of
        done -> exit(normal)
    end,

    receive
        {'FIND_NODE_RESPONSE', {nodes, ReturnedNodes}, TargetID, FromNode} ->
            {_FromId, FromPid} = FromNode,
            log:debug("~p (~p) - REPUBLISH LOOP: received a FIND_NODE_RESPONSE from ~p", 
                [LocalName, MainPid, FromPid]),
            handle_lookup_response(ReturnedNodes, TargetID, FromNode, State);

        % Message received when a pending request from a node goes in Timeout
        {timeout, NodePid} ->
            log:debug("~p (~p) - REPUBLISH LOOP: lookup TIMEOUT for the node ~p", 
                [LocalName, MainPid, NodePid]),
            handle_timeout(NodePid, State)
    end.

handle_refresh(State) ->
    LocalId = maps:get(local_id, State),
    Buckets = maps:get(buckets, State),
    LocalName = maps:get(local_name, State),
    IdByteLength = maps:get(id_byte_length, State, 20),
    Alpha = maps:get(alpha_param, State),
    ToRefresh = maps:get(to_refresh, State, 5),
    Self = self(),

    % Find N unique random bucket indexes
    N = ToRefresh,
    MaxIndex = length(Buckets),
    RandomIndexes = get_unique_random_indexes(N, MaxIndex),

    % Spawn a separate lookup process for each bucket to refresh
    lists:foreach(fun(Index) ->
        spawn(fun() -> 
                % Generate random ID for selected bucket
                log:info("~p (~p): REFRESHING bucket ~p", [LocalName, Self, Index]),
                TargetId = utils:generate_random_id_in_bucket(LocalId, Index, IdByteLength),

                % Initiate lookup loop
                LookupState = State#{
                    lookup_status => in_progress,
                    lookup_mode => refresh,
                    main_process => Self
                },
                NewState = nodes_lookup(TargetId, LookupState, Alpha),
                refresh_loop(NewState)
        end)
    end, RandomIndexes),
    node_loop(State).


refresh_loop(State) ->
    LocalName = maps:get(local_name, State),
    LookupStatus = maps:get(lookup_status, State),
    Main = maps:get(self, State),

    case LookupStatus of
        % If lookup has terminated, kill this lookup process
        done -> exit(normal);
        % otherwise
        _ ->
            receive
                {'FIND_NODE_RESPONSE', {nodes, ReturnedNodes}, TargetID, FromNode} ->
                    {FromName, _FromId, FromPid} = FromNode,
                    log:debug("~p (~p) - REFRESH LOOP: received a FIND_NODE_RESPONSE from ~p (~p)", 
                        [LocalName, Main, FromName, FromPid]),
                    handle_lookup_response(ReturnedNodes, TargetID, FromNode, State);

                % Message received when a pending request from a node goes in Timeout
                {timeout, NodePid} ->
                    log:debug("~p (~p) - REFRESH LOOP: lookup TIMEOUT for the node ~p", 
                        [LocalName, Main, NodePid]),
                    handle_timeout(NodePid, State);

                trigger_final_response -> 
                    log:debug("~p (~p): lookup finalization", [LocalName, Main]),
                    finalize_lookup(State)
            end
    end.


get_unique_random_indexes(N, Max) ->
    get_unique_random_indexes(N, Max, sets:new()).

get_unique_random_indexes(0, _, Acc) ->
    sets:to_list(Acc);
get_unique_random_indexes(N, Max, Acc) ->
    R = rand:uniform(Max) - 1,
    case sets:is_element(R, Acc) of
        true -> get_unique_random_indexes(N, Max, Acc);
        false -> get_unique_random_indexes(N - 1, Max, sets:add_element(R, Acc))
    end.


store_value_at_nodes(Nodes, Entry, State) ->
    LocalId = maps:get(local_id, State),
    IdByteLength = maps:get(id_byte_length, State, 20),
    ExpirationInterval = maps:get(expiration_interval, State),
    LocalName = maps:get(local_name, State),
    LookupMode = maps:get(lookup_mode, State),
    Self = self(),
    Now = erlang:system_time(millisecond),

    AckNodes = case LookupMode of
        store ->
            % Data to store: Key, Value, Expiration time, Owner PID
            Value = Entry,
            Expiry = Now + ExpirationInterval,
            Key = utils:calculate_key(Value, IdByteLength),
            Data = {Key, {Value, Expiry, {LocalId, Self}}},

            % Perform store on nodes

            % First, save the value in this node
            % {Key, {Value, Expiry, {LocalId, Self}}} = Data,
            % Entry = {Value, Expiry, {LocalId, Self}},
            % NewStorage = store_locally(Key, Entry, Storage, RepublishInterval),

            % Send STORE message to closest nodes and wait for Ack
            StoreNodes = send_store_queries(Nodes, Data, LocalName, LocalId, Self),

            % Return nodes that aknowledged the store
            AckNodes1 = collect_store_acks(length(StoreNodes), [], StoreNodes, 500),
            log:debug("~p (~p): collected ~p store acks", [LocalName, Self, length(AckNodes1)]),
            AckNodes1;

        republish ->
            StoreNodes = send_store_queries(Nodes, Entry, LocalName, LocalId, Self),

            % Return nodes that aknowledged the store
            AckNodes1 = collect_store_acks(length(StoreNodes), [], StoreNodes, 500),
            log:debug("~p (~p): collected ~p store acks", [LocalName, Self, length(AckNodes1)]),
            AckNodes1
    end,
    AckNodes.


send_store_queries(Nodes, Data, LocalName, LocalId, Self) ->
    log:debug("~p (~p): sending STORE to ~p nodes", [LocalName, self(), length(Nodes)]),
    [begin
        NodePid ! {'STORE', Data, {LocalName, LocalId, Self}},
        {Name, NodeId, NodePid}
    end || {Name, NodeId, NodePid} <- Nodes].


% All nodes processed (either ACK received or timed out)
collect_store_acks(0, SuccessfulNodes, _, _Timeout) ->
    SuccessfulNodes;
collect_store_acks(RemainingCount, SuccessfulNodes, AllNodes, Timeout) ->
    receive
        {'STORE_ACK', AckPid} ->
            % Find the node that sent the ACK in the list
            case lists:keyfind(AckPid, 3, AllNodes) of
                {_AckName, _AckID, AckPid} = Node ->
                    % Add to successful nodes and decrease counter
                    collect_store_acks(RemainingCount - 1, [Node | SuccessfulNodes], 
                                        AllNodes, Timeout);
                false ->
                    % Unexpected ACK, ignore
                    collect_store_acks(RemainingCount, SuccessfulNodes, 
                                        AllNodes, Timeout)
            end
    after Timeout ->
        % Timeout, decrease counter
        collect_store_acks(RemainingCount - 1, SuccessfulNodes, 
                            AllNodes, Timeout)
    end.


%%
%% Checks if list L1 has closer nodes than list L2
%%
checkCloserNodes(L1, L2, TargetID) ->
    case {L1, L2} of
        {[], _} -> false;
        {_, []} -> true;
        _ ->
            OrderedL1 = utils:order_unique(L1, TargetID),
            OrderedL2 = utils:order_unique(L2, TargetID),
            {_Name1, ID1, _Pid1} = hd(OrderedL1),
            {_Name2, ID2, _Pid2} = hd(OrderedL2),
            utils:xor_distance(ID1, TargetID) < utils:xor_distance(ID2, TargetID)
    end.


%%
%% Sends a PING request to NodePid
%%
ping_request({ReceiverName, _ReceiverId, ReceiverPid}, LocalId, LocalName) ->
    Self = self(),
    log:debug("~p (~p): send PING to ~p (~p)", [LocalName, Self, ReceiverName, ReceiverPid]),
    ReceiverPid ! {'PING', {LocalName, LocalId, self()}},
    receive 
        {'PONG', {FromName, _FromId, FromPid}} -> 
            log:debug("~p (~p): received PONG from ~p (~p)", [LocalName, Self, FromName, FromPid]),
            alive
    after 5000 ->
        log:debug("~p (~p): node ~p is DEAD", [LocalName, Self, ReceiverPid]),
        dead
    end.


%%
%% Updates the list of buckets with new nodes information
%%
update_routing_table([], _, Buckets, _, _, _) -> Buckets;
update_routing_table([Node | Rest], LocalId, Buckets, K, LocalName, IdBitLength) ->
    {_LocalName, NodeId, _Pid} = Node,
    case NodeId =:= LocalId of
        true ->
            % If node is this node, skip
            update_routing_table(Rest, LocalId, Buckets, K, LocalName, IdBitLength);
        false ->
            BucketIndex = utils:find_bucket_index(LocalId, NodeId, IdBitLength),
            case BucketIndex + 1 =< length(Buckets) of
                true ->
                    Bucket = lists:nth(BucketIndex + 1, Buckets),
                    NewBuckets = case lists:keyfind(NodeId, 2, Bucket) of
                        % If the node exists, move it to the end of the list
                        {_, NodeId, _} = ExistingNode ->
                            NewBucket = lists:delete(ExistingNode, Bucket) ++ [Node],
                            replace_bucket(Buckets, BucketIndex, NewBucket);
                        false ->
                            case length(Bucket) < K of
                                true ->
                                    % Bucket not full, insert node at the end
                                    NewBucket = Bucket ++ [Node],
                                    replace_bucket(Buckets, BucketIndex, NewBucket);
                                false ->
                                    % Bucket full, ping least recentrly contacted node
                                    [LRUNode | RestBucket] = Bucket,
                                    Result = ping_request(LRUNode, LocalId, LocalName),
                                    case Result of
                                        alive ->
                                            % LRU node responded, move it to the end
                                            NewBucket = RestBucket ++ [LRUNode],
                                            replace_bucket(Buckets, BucketIndex, NewBucket);
                                        _ ->
                                            % LRU node didn't responde, add new node to the end
                                            NewBucket = RestBucket ++ [Node],
                                            replace_bucket(Buckets, BucketIndex, NewBucket)
                                    end
                            end
                    end,
                    % Process other nodes
                    update_routing_table(Rest, LocalId, NewBuckets, K, LocalName, IdBitLength);
            false ->
                update_routing_table(Rest, LocalId, Buckets, K, LocalName, IdBitLength)
        end
    end.


%%
%% Replaces NewBucket in BucketIndex position in the list of buckets
%%
replace_bucket(Buckets, BucketIndex, NewBucket) ->
    % Split the routing table into parts before and after the bucket to replace
    {Prefix, [_OldBucket|Suffix]} = lists:split(BucketIndex, Buckets),
    
    % Combine the prefix, new bucket, and suffix to form the new list of buckets
    Prefix ++ [NewBucket] ++ Suffix.


%%
%% Gets a specified number (Remaining) of closest nodes to a Target ID from the list of buckets
%%
% Found all remaining nodes, return nodes found
get_closest_from_buckets(_, _, _, 0, Acc, _) ->
	Acc;
% Traversed all the buckets, returning nodes found
get_closest_from_buckets(_, _, 0, _, Acc, _) -> 
    Acc;
% Get remaining nodes for the current bucket, or less if not available
get_closest_from_buckets(Buckets, TargetID, CurrentBucketIndex, Remaining, Acc, LocalName) ->
	CurrentBucket = lists:nth(CurrentBucketIndex, Buckets),
	SortedBucket = lists:sort(
		fun({_Name1, ID1, _Pid1}, {_Name2, ID2, _Pid2}) -> 
			utils:xor_distance(ID1, TargetID) < utils:xor_distance(ID2, TargetID) 
		end,
		CurrentBucket
	),
    % Prefix = lists:flatten(io_lib:format("get_closest_from_buckets, index ~p", [CurrentBucketIndex])),
    % utils:print_nodes(SortedBucket, LocalName, Prefix),

	Available = length(SortedBucket),
	ToTake = min(Available, Remaining),
	NewNodes = lists:sublist(SortedBucket, ToTake),

	NewAcc = Acc ++ NewNodes,
	NewRemaining = Remaining - ToTake,
    %utils:print_nodes(NewAcc, LocalName, "NewAcc"),

	case NewRemaining of
		R when R =< 0 ->
			% Got enough nodes from this bucket
			NewAcc;
		_ ->
			% Get remaining nodes from the next bucket
			NextBucketIndex = CurrentBucketIndex - 1,
			get_closest_from_buckets(Buckets, TargetID,  NextBucketIndex, NewRemaining, NewAcc, LocalName)
    end.


%%
%% Checks if there are entries that need to be republished (CurrentTime >= RepublishTime)
%%
find_values_to_republish(Storage, _LocalName) ->
	CurrentTime = erlang:system_time(millisecond),
	maps:fold(fun(Key, {Value, Expiry, NextRepublishTime, Owner}, Acc) ->
		case NextRepublishTime =< CurrentTime of
			true -> 
				% This value needs to be republished
				[{Key, Value, Expiry, Owner} | Acc];
			false ->
				% Not yet to be republished, skip this
				Acc
		end
	end,
	[],
	Storage
).	


%%
%% Finds the minimum republish time of the values in the storage
%%
calculate_next_check_time(Storage, RepublishInterval) ->
	CurrentTime = erlang:system_time(millisecond),
	maps:fold(
        fun(_, {_, _, NextRepublishTime, _}, Earliest) ->
            min(NextRepublishTime, Earliest)
        end,
        CurrentTime + RepublishInterval, 
        Storage
    ).


get_entry_from_storage(Key, Storage, _LocalName) ->
    case maps:find(Key, Storage) of
        {ok, Entry} ->
			{Value, Expiry, _Republish, Owner} = Entry,
            {entry, {Value, Expiry, Owner}};
        error ->
            {entry, not_found}
    end.


delete_entry_from_storage(Key, Storage) ->
    maps:remove(Key, Storage).


store_locally(Key, Entry, Storage, RepublishInterval) ->
	CurrentTime = erlang:system_time(millisecond),
	{Value, Expiry, Owner} = Entry,

	% Set/Reset the republish time for the received value and store the entry
    NewRepublishTime = CurrentTime + RepublishInterval,
    UpdatedStorage = maps:put(Key, {Value, Expiry, NewRepublishTime, Owner}, Storage),
    
    UpdatedStorage.