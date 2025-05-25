-module(bootstrap_node).
-export([start_link/2]).


start_link(Name, ShellPid) ->
	Pid = spawn_link(fun() -> init(Name, ShellPid) end),
	log:info("Spawned bootstrap node: ~p~n", [{Name, Pid}]),
	{ok, Pid}.

init(Name, ShellPid) ->
	% Get ID length from global options
	[{global_options, NetworkOpts}] = ets:lookup(network_options, 
											global_options),
	IdByteLength = maps:get(id_byte_length, NetworkOpts, 20),

	ID = utils:generate_node_id(IdByteLength),

	% Save the bootstrap info in an ETS table
	ets:insert(bootstrap_nodes, {Name, ID, self()}),
	log:info("Saved bootstrap node: ~p~n", [Name]),

	% Choose a bootstrap node for this bootstrap node
	BootstrapNode = node:choose_bootstrap(Name),
	log:info("Chosen bootstrap node: ~p~n", [BootstrapNode]),
	node:init_node(ID, BootstrapNode, Name, ShellPid),
	ok.
