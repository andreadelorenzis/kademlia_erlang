-module(node).
-export([start_network/0, start_network/1, start_simple_node/1, 
     start_bootstrap_node/2, init_node/4, choose_bootstrap/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Starts the Kademlia network with predefined options.
start_network() ->
  DefaultOptions = #{
    k_param => 20,
    alpha_param => 3,
    id_byte_length => 5, 					% 40 bits
    republish_interval => 3600000, 			% 1 hour
    expiration_interval => 86400000, 		% 24 hours
    refresh_interval => 3600000, 			% 1 hour
    check_expiration_interval => 60000, 	% 1 minute
    timeout_interval => 2000,
    log_level => important
  },
  start_network(DefaultOptions).


%% Starts the Kademlia network with custom options.
start_network(Options) ->
  IdByteLength = maps:get(id_byte_length, Options, 20),

  % Set logger
  LogLevel = maps:get(log_level, Options, info),
  log:start(),
  log:set_level_global(LogLevel),
  log:info("~n~nSTARTING NETWORK~n"),
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
      log:debug("bootstrap_sup already started"),
      AlreadyStartedPid;
    _:Reason ->
      error_logger:error_msg("Error during startup of bootstrap_sup: ~p", 
        [Reason]),
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

  log:important("Kademlia network started."),

  {ok, SupPid}.


%% Creates a new Kademlia node with a random node ID.
start_simple_node(LocalName) ->
  log:info("~p: starting..", [LocalName]),
  ShellPid = self(),

  % Get ID length from global options
  [{global_options, NetworkOpts}] = ets:lookup(network_options, 
                      global_options),
  IdByteLength = maps:get(id_byte_length, NetworkOpts, 20),

  % Select a bootstrap node from available bootstrap nodes
  BootstrapNode = choose_bootstrap(LocalName),
  {BootstrapName, _BootstrapId, BootstrapPid} = BootstrapNode,
  log:info("~p: selected Bootstrap node ~p (~p)", [LocalName, 
    BootstrapName, BootstrapPid]),

  % Generate random binary Kademlia ID
  NodeId = utils:generate_node_id(IdByteLength),

  NodePid = spawn(fun() -> init_node(NodeId, BootstrapNode, LocalName, 
                ShellPid) end),
  log:info("~p (~p): spawned node", [LocalName, NodePid]),
  {ok, {LocalName, NodeId, NodePid}}.


%% Creates a new bootstrap node using the Supervisor OTP behaviour.
start_bootstrap_node(Name, ShellPid) ->
  log:info("~p: starting..", [Name]),
  ChildSpec = {Name,
         {bootstrap_node, start_link, [Name, ShellPid]}, 
         transient,
         5000,
         worker,
         [bootstrap_node]},
  supervisor:start_child({global, bootstrap_sup}, ChildSpec).


%% Initializes a spawned Kademlia node before calling the node loop.
init_node(ID, BootstrapNode, LocalName, ShellPid) ->
  Self = self(),
  log:info("~p (~p): node initialization", [LocalName, Self]),

  % Get global network options from ETS table
  [{global_options, NetworkOpts}] = ets:lookup(network_options, 
                      global_options),

  % Storage and buckets list creation
  Storage = #{}, 
  IdByteLength = maps:get(id_byte_length, NetworkOpts, 20),
  IdBitLength = IdByteLength * 8,
  Buckets = lists:duplicate(IdBitLength, []),

  % Lookup state variables
  VisitedNodes = [],
  NodesCollected = [],
  PendingRequests = [],
  LookupStatus = done,
  TimeoutRefs = #{},

  % Combine global options with with node specific state variables
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
  RefreshInterval = maps:get(refresh_interval, State),
  TimeoutInterval = maps:get(timeout_interval, State),
  CheckExpirationinterval = maps:get(check_expiration_interval, State, 60000),

  % Start a timer that will send 'refresh_buckets' every refresh_interval
  timer:send_interval(RefreshInterval, Self, {refresh_buckets, Self}),

  % Start a timer that will send 'check_expiration' every interval
  timer:send_interval(CheckExpirationinterval, Self, check_expiration),

  case BootstrapNode of
    undefined -> % first bootstrap node
      log:info("~p (~p): first bootstrap", [LocalName, Self]),
      ShellPid ! {find_node_response, []},
      node_loop(State);
    _ -> 
      % Insert bootstrap node in this node's bucket list
      {BootName, _BootId, BootPid} = BootstrapNode,
      log:debug("~p (~p): inserting bootstrap node ~p (~p)", 
        [LocalName, Self, BootName, BootPid]),
      UpdatedBuckets = update_routing_table([BootstrapNode], 
        LocalId, Buckets, K, LocalName, IdBitLength, TimeoutInterval),
      utils:print_buckets_with_distance(LocalId, LocalName, Self, 
        UpdatedBuckets, debug),

      % Search nodes closer to local ID (self-lookup)
      log:debug("~p (~p): executing self lookup..", [LocalName, Self]),
      NewState = node_lookup(LocalId, 
                  State#{lookup_status => in_progress, 
                      lookup_mode => find_node,
                      requester => ShellPid,
                      buckets => UpdatedBuckets}, 
                  Alpha),
      node_loop(NewState)
  end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Manages the main loop of a Kademlia node, handling all incoming messages 
%% and state transitions.
node_loop(State) ->
  LocalName = maps:get(local_name, State),
  K = maps:get(k_param, State),
  LocalId = maps:get(local_id, State),
  Alpha = maps:get(alpha_param, State),
  IdByteLength = maps:get(id_byte_length, State, 20),
  IdBitLength = IdByteLength * 8,
  
  RepublishInterval = maps:get(republish_interval, State),
  TimeoutInterval = maps:get(timeout_interval, State),
  Storage = maps:get(storage, State),   
  Buckets = maps:get(buckets, State),   
  Self = self(),

  % calculate next republish time
  NextRepublishTime  = calculate_next_check_time(Storage, 
  RepublishInterval),
  CurrentTime = erlang:system_time(millisecond),
  Timeout = max(0, NextRepublishTime - CurrentTime),

  receive

    {find_value_request, Key, FromPid} ->
      log:info("~p (~p): find_value_request from ~p", [LocalName, 
        Self, FromPid]),

      Result = get_entry_from_storage(Key, Storage, LocalName),
      case Result of
        {entry, Entry} when Entry =/= not_found ->
          % Entry found locally
          log:debug("~p (~p): found value locally", 
            [LocalName, Self]),
          Hops = maps:get(num_hops, State, 0),
          FromPid ! {find_value_response, Entry, Hops};
        {entry, not_found} ->
          % Entry not found locally, do network lookup
          log:debug("~p (~p): executing first VALUE lookup", 
            [LocalName, Self]),
          NewState = node_lookup(Key, 
                    State#{lookup_status => in_progress, 
                        lookup_mode => find_value,
                        requester => FromPid},
                    Alpha),
          node_loop(NewState)
      end,
      node_loop(State);

    {find_node_request, TargetId, FromPid} ->
      log:info("~p (~p): find_node_request from ~p", 
        [LocalName, Self, FromPid]),
      log:debug("~p (~p): executing first NODE lookup", 
        [LocalName, Self]),
      NewState = node_lookup(TargetId, 
                  State#{lookup_status => in_progress, 
                      lookup_mode => find_node,
                      requester => FromPid},
                  Alpha),
      node_loop(NewState);

    {store_request, Value, FromPid} ->
      log:info("~p: store_request from ~p", [LocalName, FromPid]),
      Key = utils:calculate_key(Value, IdByteLength),
      NewState = node_lookup(Key, 
                  State#{lookup_status => in_progress, 
                      lookup_mode => store,
                      requester => FromPid,
                      value_to_store => Value},
                  Alpha),
      node_loop(NewState);

    {ping_request, ReceiverPid, FromPid} ->
      Result = ping_request(ReceiverPid, LocalId, LocalName, TimeoutInterval),
      case Result of
        alive -> 
          FromPid ! alive;
        dead ->
          FromPid ! dead
      end,
      node_loop(State);

    check_expiration ->
      log:info("~p (~p): check_expiration", [LocalName, Self]),
      % Remove expired entries
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
          log:debug("~p (~p): received VALUE from ~p (~p).", 
            [LocalName, Self, FromName, FromPid]),
          Requester = maps:get(requester, State),
          Hops = maps:get(num_hops, State),
          Requester ! {find_value_response, Value, Hops},
          NewState = reset_lookup_state(State),
          node_loop(NewState);
        done ->
          % Lookup already terminated
          log:debug("~p (~p): ignore FIND_VALUE_RESPONSE from ~p (~p)", 
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
      log:debug("~p (~p): ~p remaining buckets to refresh", [LocalName, Self, 
        NewToRefresh]),
      case NewToRefresh of
        R when R =< 0 ->
          % Refreshed all buckets
          log:info("~p (~p): REFRESH COMPLETE", [LocalName, Self]),
          Requester ! {refresh_complete},
          node_loop(State#{buckets => NewBuckets});
        _ ->
          node_loop(State#{buckets => NewBuckets, to_refresh => NewToRefresh})
      end;

    % Message received when a pending request from a node goes in Timeout
    {timeout, _Ref, NodePid} ->
      % log:debug("~p (~p): lookup TIMEOUT for the node ~p", [LocalName, 
      % 	Self, NodePid]),
      NewState = handle_timeout(NodePid, State),
      node_loop(NewState);

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

    {refresh_buckets, FromPid} ->
      NewState = State#{requester => FromPid, to_refresh => length(Buckets)},
      handle_refresh(NewState),
      node_loop(NewState);

    {republish_values, FromPid} ->
      NewState = State#{requester => FromPid},
      handle_republish(NewState),
      node_loop(NewState);

    {'PING', FromNode} -> 
      {FromName, _FromID, FromPid} = FromNode,
      log:info("~p (~p): PING message from ~p (~p)", [LocalName, Self, 
        FromName, FromPid]),
      FromPid ! {'PONG', {LocalName, LocalId, Self}},
      node_loop(State);

    {'STORE', Data, Node, FromPid} ->
      {NodeName, _NodeId, NodePid} = Node,
      log:info("~p (~p): STORE message from ~p (~p)", [LocalName, Self, 
        NodeName, NodePid]),

      % Save pair inside the local storage
      {Key, Entry} = Data,
      UpdatedStorage = store_locally(Key, Entry, Storage, RepublishInterval),
      NewBuckets = update_routing_table([Node], LocalId, Buckets, K, 
        LocalName, IdBitLength, TimeoutInterval),

      % Print new state
      utils:print_buckets_with_distance(LocalId, LocalName, Self, NewBuckets, 
        Key, debug),
      utils:print_storage(LocalName, Self, UpdatedStorage),

      % Send ACK back to sender
      log:debug("~p (~p): sending STORE_ACK to ~p (~p)", [LocalName, Self, 
        NodeName, NodePid]),
      FromPid ! {'STORE_ACK', self()},

      node_loop(State#{buckets => NewBuckets, storage => UpdatedStorage});

    {'FIND_NODE', TargetId, Node, FromPid} ->
      {NodeName, _NodeId, NodePid} = Node,
      log:info("~p (~p): FIND_NODE message from ~p (~p)", [LocalName, Self, 
        NodeName, NodePid]),

      % Add the sender node to the routing table
      NewBuckets = update_routing_table([Node], LocalId, Buckets, K, 
        LocalName, IdBitLength, TimeoutInterval),
      % utils:print_buckets(LocalName, Self, NewBuckets),
      utils:print_buckets_with_distance(LocalId, LocalName, Self, 
        NewBuckets, TargetId, debug),

      % Send back the K closest nodes to target ID from this node's buckets
      ClosestNodes=get_closest_from_buckets(NewBuckets, TargetId, K, Node),
      utils:print_nodes_with_distance(ClosestNodes, 
                TargetId,
                io_lib:format("~p (~p) - FIND_NODE: return ~p closest", 
                    [LocalName, Self, length(ClosestNodes)])),
      % log:debug("~p (~p) - FIND_NODE: sending back ~p closest nodes", 
      %     [LocalName, Self, length(ClosestNodes)]),
      FromPid ! {'FIND_NODE_RESPONSE', {nodes, ClosestNodes}, TargetId, 
        {LocalName, LocalId, Self}},

      node_loop(State#{buckets => NewBuckets});

    {'FIND_VALUE', Key, Node, FromPid} ->
      {NodeName, _NodeId, NodePid} = Node,
      log:info("~p (~p): FIND_VALUE message from ~p (~p)", [LocalName, 
        Self, NodeName, NodePid]),

      % Add the sender node to the routing table
      NewBuckets = update_routing_table([Node], LocalId, Buckets, K, 
        LocalName, IdBitLength, TimeoutInterval),
      % utils:print_buckets(LocalName, Self, NewBuckets),
      utils:print_buckets_with_distance(LocalId, LocalName, Self, 
        NewBuckets, Key, debug),

      % Search locally for value. If not found, return closest nodes.
      case get_entry_from_storage(Key, Storage, LocalName) of
        {entry, Entry} when Entry =/= not_found ->
          log:debug("~p (~p) - FIND_VALUE: sending value found locally", 
            [LocalName, Self]),
          FromPid ! {'FIND_VALUE_RESPONSE', {value, Entry}, Key, 
            {LocalName, LocalId, Self}};
        {entry, not_found} ->
          ClosestNodes = get_closest_from_buckets(NewBuckets, Key, K, Node),
          log:debug("~p (~p) - FIND_VALUE: NOT found, returning ~p closest", 
            [LocalName, Self, length(ClosestNodes)]),
          FromPid ! {'FIND_VALUE_RESPONSE', {nodes, ClosestNodes}, Key, 
            {LocalName, LocalId, Self}}
      end,
      node_loop(State#{buckets => NewBuckets})
  after Timeout  ->
    log:important("~p (~p): value republish timeout", [LocalName, Self]),
    Self ! {republish_values, Self},
    node_loop(State)
  end.


%% Resets the lookup-related state variables to their initial values.
reset_lookup_state(State) ->
  State#{lookup_status => done,
      pending_requests => [],
      nodes_collected => [],
      visited_nodes => [],
      timeout_refs => #{},
      is_last_query => false,
      num_hops => 0}.


%% Handles a lookup response, updating the state accordingly.
handle_lookup_response(ReturnedNodes, TargetID, FromNode, State) ->
  LocalName = maps:get(local_name, State),
  LocalId = maps:get(local_id, State),
  IdByteLength = maps:get(id_byte_length, State, 20),
  IdBitLength = IdByteLength * 8,
  Buckets = maps:get(buckets, State),
  K = maps:get(k_param, State),
  Alpha = maps:get(alpha_param, State),
  TimeoutInterval = maps:get(timeout_interval, State),
  PendingRequests = maps:get(pending_requests, State),
  TimeoutRefs = maps:get(timeout_refs, State),
  NodesCollected = maps:get(nodes_collected, State),
  VisitedNodes = maps:get(visited_nodes, State),
  LookupMode = maps:get(lookup_mode, State),
  IsLastQuery = maps:get(is_last_query, State, false),
  Main = maps:get(self, State),
  {FromName, _, FromPid} = FromNode,

  % Remove received node from pending requests
  NewPending = lists:delete(FromNode, PendingRequests),
  log:debug("~p (~p): removed ~p (~p) from pending requests", 
    [LocalName, Main, FromName, FromPid]),
  utils:print_nodes(NewPending, 
            io_lib:format("~p (~p) - NewPending: ", [LocalName, Main])),

  % Cancel timer
  NewTimeouts = case maps:find(FromPid, TimeoutRefs) of
    {ok, TimeoutRef} ->
        erlang:cancel_timer(TimeoutRef),
        maps:remove(FromPid, TimeoutRefs);
    _ ->
        TimeoutRefs
  end,

  % Update routing table
  FilteredNodes = [Node || {_, NodeId, _} = Node <- ReturnedNodes, 
                  NodeId =/= LocalId], 	% Remove myself
  NewBuckets = update_routing_table(FilteredNodes, LocalId, Buckets, K,
     LocalName, IdBitLength, TimeoutInterval),
  utils:print_buckets_with_distance(LocalId, LocalName, Main, NewBuckets,
     TargetID, debug),

  % Update collected nodes
  NewNodesCollected = utils:order_unique(NodesCollected ++ ReturnedNodes,
     TargetID),
  utils:print_nodes_with_distance(NewNodesCollected, 
            TargetID,
            io_lib:format("~p (~p) - node_lookup - NewNodesCollected: ", 
                [LocalName, Main])),
  NewState = State#{
    buckets => NewBuckets,
    nodes_collected => NewNodesCollected,
    pending_requests => NewPending,
    timeout_refs => NewTimeouts
  },

  % Check if there are closer nodes
  case checkCloserNodes(FilteredNodes, VisitedNodes, TargetID) of
    true ->
      % Continue with lookup of Alpha closer nodes not yet visited
      log:debug("~p (~p): CONTINUE lookup - still some closer nodes", 
        [LocalName, Main]),
      UpdatedState = node_lookup(TargetID, NewState, Alpha),
      case LookupMode of
        republish -> subprocess_loop(UpdatedState);
        refresh -> subprocess_loop(UpdatedState);
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
              % Perform one last query for K closest nodes not yet visited
              log:debug("~p (~p): LAST lookup for K nodes not visited", 
                [LocalName, Main]),
              UpdatedState = node_lookup(
                              TargetID, 
                              NewState#{is_last_query => true}, 
                              K),
              case LookupMode of
                republish -> subprocess_loop(UpdatedState);
                refresh -> subprocess_loop(UpdatedState);
                _ -> node_loop(UpdatedState)
              end;
            true ->
              finalize_lookup(NewState)
          end;
        _ ->
          % There are still pending requests
          log:debug("~p (~p): CONTINUE - still some pending requests", 
            [LocalName, Main]),
          % utils:print_nodes(NewPending, 
          %     io_lib:format("~p (~p) - NewPending: ", [LocalName, Main])),
          case LookupMode of
            republish -> subprocess_loop(NewState);
            refresh -> subprocess_loop(NewState);
            _ -> node_loop(NewState)
          end
      end
  end.


%% Initiates or continues a node lookup process for a given target ID.
node_lookup(TargetID, State, Alpha) ->
  Buckets = maps:get(buckets, State),
  LocalName = maps:get(local_name, State, unknown),
  LocalId = maps:get(local_id, State),
  VisitedNodes = maps:get(visited_nodes, State),
  NodesCollected = maps:get(nodes_collected, State),
  LookupMode = maps:get(lookup_mode, State),
  Hops = maps:get(num_hops, State, 0),
  Main = maps:get(self, State),
  Self = self(),

  {ClosestNodes, NewNodesCollected} = select_closest_nodes(TargetID, Alpha, 
    Buckets, NodesCollected, VisitedNodes, Main, LocalName, LocalId),
  utils:print_nodes_with_distance(ClosestNodes, 
                  TargetID,
                  io_lib:format("~p (~p) - node_lookup - ClosestNodes: ", 
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
      utils:print_nodes_with_distance(NewVisitedNodes, 
                TargetID,
                io_lib:format("~p (~p) - node_lookup - VisitedNodes: ", 
                        [LocalName, Main])),
      NewHops = Hops + 1,
      State#{
        visited_nodes => NewVisitedNodes,
        timeout_refs => TimeoutRefs,
        pending_requests => Pending,
        nodes_collected => NewNodesCollected,
        num_hops => NewHops
      }
  end,
  NewState.


%% Selects the closest nodes to a target ID from buckets or collected nodes.
select_closest_nodes(TargetID, Alpha, Buckets, NodesCollected, VisitedNodes, 
  Self, LocalName, LocalId) ->
  case NodesCollected of
    [] ->
      % First lookup of Alpha closest nodes from buckets
      log:debug("~p (~p) - node_lookup: get_closest_from_buckets", 
        [LocalName, Self]),
      ClosestFromBuckets = get_closest_from_buckets(Buckets, TargetID, 
        Alpha, undefined),
      ClosestNodes = [Node || {_, NodeId, _} = Node <- ClosestFromBuckets, 
       not lists:member(Node, VisitedNodes), NodeId =/= LocalId],
      {ClosestNodes, ClosestNodes};
    _ ->
      % Next lookups of Alpha closest nodes not yet visited
      NodesNotVisited = [Node || {_, NodeId, _} = Node <- NodesCollected, 
                 not lists:member(Node, VisitedNodes), 
                NodeId =/= LocalId],
      ClosestNodes = lists:sublist(NodesNotVisited, Alpha),
      {ClosestNodes, NodesCollected}
  end.


%% Sends queries to nodes and manages timeouts for pending requests.
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

  % For each node, send a query, start a timeout timer and add to pending
  {TimeoutRefs, Pending} =
    lists:foldl(
      fun({_Name, _NodeId, NodePid} = Node, {RefsAcc, PendingAcc}) ->
        Ref = erlang:start_timer(TimeoutInterval, Self, NodePid),
        log:debug("Timer ~p started for node ~p", [Ref, NodePid]),
        % Send the message
        case Mode of
          find_node ->
            NodePid ! {'FIND_NODE', TargetID, {LocalName, LocalId, MainPid}, Self};
          find_value ->
            NodePid ! {'FIND_VALUE', TargetID, {LocalName, LocalId, MainPid}, Self}
        end,
        {maps:put(NodePid, Ref, RefsAcc), [Node | PendingAcc]}
      end,
      {TimeoutRefs0, Pending0},
      Nodes
    ),
  {TimeoutRefs, Pending}.


%% Finalizes the lookup process based on the lookup mode and current state.
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
  log:debug("~p (~p): STOP LOOKUP - ZERO pending requests", 
    [LocalName, Main]),
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
      subprocess_loop(ResetState);
    refresh ->
      log:debug("~p (~p): sending refresh ACK to main process (~p)", 
        [LocalName, Main, Main]),
      Main ! {refresh_response, ok, Buckets},
      ResetState = reset_lookup_state(State),
      subprocess_loop(ResetState)
  end.


%% Handles timeout events for pending node requests during lookups.
handle_timeout(NodePid, State) ->
  TimeoutRefs = maps:get(timeout_refs, State),
  % LocalName = maps:get(local_name, State),
  % Main = maps:get(self, State),
  PendingRequests = maps:get(pending_requests, State),

  % Se non è più pending, ignora
  NodeInPending = lists:any(fun({_, _, Pid}) -> Pid =:= NodePid end, 
    PendingRequests),
  case NodeInPending of
      false ->
          log:debug("Timeout received for ~p but not in pending, ignore", 
            [NodePid]),
          % If lookup still going and no more pending requests -> STOP
          case maps:get(lookup_status, State) of
            in_progress ->
              case PendingRequests of
                [] ->
                  self() ! trigger_final_response,
                  State;
                _ -> 
                  log:debug("TIMEOUT: continue 2"),
                  State
              end;
            _ -> 
              log:debug("TIMEOUT: continue 3"),
              self() ! trigger_final_response,
              State
          end;
      true ->
          log:error("Timeout received for pending node: ~p (Pending: ~p)", 
          [NodePid, maps:get(pending_requests, State)]),

          % Remove node from pending requests
          NewPending = lists:filter(fun({_, _, Pid}) -> Pid =/= NodePid end, 
            PendingRequests),

          % Cancel timer
          NewTimeouts = case maps:find(NodePid, TimeoutRefs) of
            {ok, TimeoutRef} ->
                erlang:cancel_timer(TimeoutRef),
                maps:remove(NodePid, TimeoutRefs);
            _ ->
                TimeoutRefs
          end,

          NewState = State#{
            pending_requests => NewPending,
            timeout_refs => NewTimeouts
          },

          % If lookup still going and no more pending requests -> STOP
          case maps:get(lookup_status, NewState) of
            in_progress ->
              case NewPending of
                [] ->
                  self() ! trigger_final_response,
                  NewState;
                _ -> 
                  log:debug("TIMEOUT: continue 4"),
                  NewState
              end;
            _ -> 
              log:debug("TIMEOUT: continue 5"),
              NewState
          end
  end.


%% Manages the subprocess loop for parallel lookup operations
subprocess_loop(State) ->
  LocalName = maps:get(local_name, State),
  LookupStatus = maps:get(lookup_status, State),
  LookupMode = maps:get(lookup_mode, State),
  Main = maps:get(self, State),

  case LookupStatus of
    % If lookup has terminated, kill this lookup process
    done -> exit(normal);
    _ ->
      receive
        {'FIND_NODE_RESPONSE', {nodes, Nodes}, TargetID, FromNode} ->
          {FromName, _FromId, FromPid} = FromNode,
          log:debug("~p (~p) - ~p: FIND_NODE_RESPONSE from ~p (~p)", 
            [LocalName, Main, LookupMode, FromName, FromPid]),
          handle_lookup_response(Nodes, TargetID, FromNode, State);

        % Message received when a pending request timeout happens
        {timeout, _Ref, NodePid} ->
          log:debug("~p (~p) - ~p: lookup TIMEOUT for the node ~p", 
            [LocalName, Main, LookupMode, NodePid]),
          NewState = handle_timeout(NodePid, State),
          subprocess_loop(NewState);

        trigger_final_response -> 
          log:debug("~p (~p) - ~p: lookup finalization", 
            [LocalName, Main, LookupMode]),
          finalize_lookup(State)
      end
  end.


%% Handles the republishing of values that are due for refresh.
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
    fun({Key, Value, Expiry, Owner}, AccStorage) ->
      maps:put(Key, {Value, Expiry, NewRepublishTime, Owner}, 
        AccStorage)
    end,
    Storage,
    ValuesToRepublish
  ),
  utils:print_storage(LocalName, Self, UpdatedStorage),

  % Spawn separate lookup process for each value to republish
  lists:foreach(
    fun({Key, Value, Expiry, Owner}) ->
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
        NewState = node_lookup(Key, LookupState, Alpha),
        subprocess_loop(NewState)
      end)
    end,
    ValuesToRepublish
  ),
  node_loop(State#{storage => UpdatedStorage}).


%% Handles the refreshing of routing table buckets.
handle_refresh(State) ->
  LocalId = maps:get(local_id, State),
  Buckets = maps:get(buckets, State),
  LocalName = maps:get(local_name, State),
  IdByteLength = maps:get(id_byte_length, State, 20),
  Alpha = maps:get(alpha_param, State),
  Self = self(),
  MaxIndex = length(Buckets),

  Indexes = lists:seq(0, MaxIndex - 1),

  % Spawn a separate lookup process for each bucket to refresh
  lists:foreach(fun(Index) ->
    spawn(fun() -> 
        % Generate random ID for selected bucket
        log:important("~p (~p): REFRESHING bucket ~p", 
          [LocalName, Self, Index]),
        TargetId = utils:generate_random_id_in_bucket(LocalId, Index, 
          IdByteLength),

        % Initiate lookup loop
        LookupState = State#{
          lookup_status => in_progress,
          lookup_mode => refresh,
          main_process => Self
        },
        NewState = node_lookup(TargetId, LookupState, Alpha),
        subprocess_loop(NewState)
    end)
  end, Indexes),
  node_loop(State).


%% Stores a value at the closest nodes found during a lookup.
store_value_at_nodes(Nodes, Entry, State) ->
  LocalId = maps:get(local_id, State),
  IdByteLength = maps:get(id_byte_length, State, 20),
  ExpirationInterval = maps:get(expiration_interval, State),
  LocalName = maps:get(local_name, State),
  LookupMode = maps:get(lookup_mode, State),
  MainPid = maps:get(self, State),
  Self = self(),
  Now = erlang:system_time(millisecond),

  AckNodes = case LookupMode of
    store ->
      % Data to store: Key, Value, Expiration time, Owner PID
      Value = Entry,
      Expiry = Now + ExpirationInterval,
      Key = utils:calculate_key(Value, IdByteLength),
      Data = {Key, {Value, Expiry, {LocalName, LocalId, Self}}},

      % Send STORE message to closest nodes and wait for Ack
      StoreNodes = send_store_queries(Nodes, Data, LocalName, LocalId, Self),

      % Return nodes that aknowledged the store
      AckNodes1 = collect_store_acks(length(StoreNodes), [], StoreNodes, 500),
      log:debug("~p (~p): collected ~p store acks", [LocalName, Self, 
        length(AckNodes1)]),
      AckNodes1;

    republish ->
      StoreNodes = send_store_queries(Nodes, Entry, LocalName, LocalId, 
        MainPid),

      % Return nodes that aknowledged the store
      AckNodes1 = collect_store_acks(length(StoreNodes), [], StoreNodes, 500),
      log:debug("~p (~p): collected ~p store acks", [LocalName, Self, 
        length(AckNodes1)]),
      AckNodes1
  end,
  AckNodes.


%% Sends STORE queries to a list of nodes.
send_store_queries(Nodes, Data, LocalName, LocalId, MainPid) ->
  Self = self(),
  log:debug("~p (~p): sending STORE to ~p nodes", [LocalName, MainPid, 
    length(Nodes)]),
  [begin
    NodePid ! {'STORE', Data, {LocalName, LocalId, MainPid}, Self},
    {Name, NodeId, NodePid}
  end || {Name, NodeId, NodePid} <- Nodes].


%% Collects store acknowledgments from nodes with a timeout.
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


%% Compares two lists of nodes to determine if the first list contains 
%% closer nodes to a target ID.
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


%% Sends a ping request to a node and waits for a response.
ping_request(ReceiverPid, LocalId, LocalName, TimeoutInterval) ->
  Self = self(),
  log:debug("~p (~p): send PING to ~p", [LocalName, Self, ReceiverPid]),
  ReceiverPid ! {'PING', {LocalName, LocalId, self()}},
  receive 
    {'PONG', {FromName, _FromId, FromPid}} -> 
      log:debug("~p (~p): received PONG from ~p (~p)", [LocalName, Self, 
        FromName, FromPid]),
      alive
  after TimeoutInterval ->
    log:debug("~p (~p): node ~p is DEAD", [LocalName, Self, ReceiverPid]),
    dead
  end.


%% Updates the routing table with new node information.
update_routing_table([], _, Buckets, _, _, _, _) -> Buckets;
update_routing_table([Node | Rest], LocalId, Buckets, K, LocalName, 
    IdBitLength, TimeoutInterval) ->
  {NodeName, NodeId, NodePid} = Node,
  case NodeId =:= LocalId of
    true ->
      % If node is this node, skip
      update_routing_table(Rest, LocalId, Buckets, K, LocalName, 
        IdBitLength, TimeoutInterval);
    false ->
      BucketIndex = utils:find_bucket_index(LocalId, NodeId, 
        IdBitLength),
      log:debug("~p (~p): try to insert ~p (~p) into bucket ~p", 
        [LocalName, self(), NodeName, NodePid, BucketIndex + 1]),
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
                  {_, _, LRUPid} = LRUNode,
                  Result = ping_request(LRUPid, LocalId, LocalName, 
                    TimeoutInterval),
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
          update_routing_table(Rest, LocalId, NewBuckets, K, 
            LocalName, IdBitLength, TimeoutInterval);
      false ->
        update_routing_table(Rest, LocalId, Buckets, K, LocalName, 
          IdBitLength, TimeoutInterval)
    end
  end.


%% Replaces NewBucket in BucketIndex position in the list of buckets.
replace_bucket(Buckets, BucketIndex, NewBucket) ->
  % Split the routing table into parts before and after the bucket index
  {Prefix, [_OldBucket|Suffix]} = lists:split(BucketIndex, Buckets),
  
  % Combine the prefix, new bucket, and suffix to form the new buckets list
  Prefix ++ [NewBucket] ++ Suffix.


%% Retrieves the closest nodes to a target ID from the routing buckets.
get_closest_from_buckets(Buckets, TargetID, Alpha, FromNode) ->
  % Flatten all nodes in all buckets
  AllNodes = lists:flatten(Buckets),

  % Remove sender node
  FilteredNodes = case FromNode of
    undefined -> AllNodes;
    {_, FromId, _} -> 
      [Node || {_, Id, _} = Node <- AllNodes, Id =/= FromId]
  end,

  % Sort by distance to TargetID
  Sorted = lists:sort(
    fun({_, ID1, _}, {_, ID2, _}) ->
      utils:xor_distance(ID1, TargetID) < utils:xor_distance(ID2, TargetID)
    end,
    FilteredNodes
  ),
  
  % Take the first Alpha nodes
  lists:sublist(Sorted, Alpha).


%% Finds values in storage that need to be republished.
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


%% Calculates the next time when storage entries need to be checked for 
%% republishing.
calculate_next_check_time(Storage, RepublishInterval) ->
  CurrentTime = erlang:system_time(millisecond),
  maps:fold(
    fun(_, {_, _, NextRepublishTime, _}, Earliest) ->
      min(NextRepublishTime, Earliest)
    end,
    CurrentTime + RepublishInterval, 
    Storage
  ).


%% Retrieves an entry from the local storage.
get_entry_from_storage(Key, Storage, _LocalName) ->
  case maps:find(Key, Storage) of
    {ok, Entry} ->
      {Value, Expiry, _Republish, Owner} = Entry,
      {entry, {Value, Expiry, Owner}};
    error ->
      {entry, not_found}
  end.


%% Deletes an entry from the local storage.
delete_entry_from_storage(Key, Storage) ->
  maps:remove(Key, Storage).


%% Stores a value locally with appropriate timestamps.
store_locally(Key, Entry, Storage, RepublishInterval) ->
  CurrentTime = erlang:system_time(millisecond),
  {Value, Expiry, Owner} = Entry,

  % Set/Reset the republish time for the received value and store the entry
  NewRepublishTime = CurrentTime + RepublishInterval,
  UpdatedStorage = maps:put(Key, {Value, Expiry, NewRepublishTime, Owner}, 
    Storage),
  
  UpdatedStorage.


%% Chooses a bootstrap node from available nodes, excluding itself.
choose_bootstrap(OwnName) ->
  AllNodes = ets:match_object(bootstrap_nodes, {'_', '_', '_'}),
  OtherNodes = [{Name, ID, Pid} || {Name, ID, Pid} <- AllNodes, 
    Name =/= OwnName],
  case OtherNodes of
    [] -> 
      undefined;  % First bootstrap node
    _ -> 
      {Name, ID, Pid} = lists:nth(rand:uniform(length(OtherNodes)), 
        OtherNodes),
      {Name, ID, Pid}  
  end.