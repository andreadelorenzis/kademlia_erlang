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

	% Choose a bootstrap node for this bootstrap node
	BootstrapNode = node:choose_bootstrap(Name),
	log:info("Chosen bootstrap node: ~p~n", [BootstrapNode]),
	node:init_node(ID, BootstrapNode, Name, ShellPid),
	ok.
