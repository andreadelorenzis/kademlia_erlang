-module(log).
-export([
    start/0, 
    log/3, 
    log/2, 
    set_level/1,
    set_level_global/1,
    debug/1,
    debug/2,
    info/1,
    info/2,
    warn/1,
    warn/2,
    error/1,
    error/2,
    important/1,
    important/2
]).

-define(DEFAULT_LEVEL, info).

-define(LOG_LEVEL_KEY, '__log_level__').

start() ->
    put(?LOG_LEVEL_KEY, ?DEFAULT_LEVEL),
    io:format("Log system initialized with default level: ~p~n", [?DEFAULT_LEVEL]).

set_level(Level) ->
    case level(Level) of
        {ok, _} ->
            put(?LOG_LEVEL_KEY, Level),
            io:format("Log level is now: ~p~n", [get(?LOG_LEVEL_KEY)]);
        {error, unknown_level} ->
            io:format("Invalid log level: ~p (keeping current level: ~p)~n", 
                     [Level, get(?LOG_LEVEL_KEY)])
    end.

set_level_global(Level) ->
    % Imposta per tutti i processi
    case level(Level) of
        {ok, _} ->
            persistent_term:put(?LOG_LEVEL_KEY, Level),
            ok;
        _ ->
            {error, invalid_level}
    end.

debug(Format) ->
    log(debug, Format).

debug(Format, Args) ->
    log(debug, Format, Args).

info(Format) ->
    log(info, Format).

info(Format, Args) ->
    log(info, Format, Args).

warn(Format) ->
    log(warn, Format).

warn(Format, Args) ->
    log(warn, Format, Args).

error(Format) ->
    log(error, Format).

error(Format, Args) ->
    log(error, Format, Args).

important(Format) ->
    log(important, Format).

important(Format, Args) ->
    log(important, Format, Args).


log(Level, Format) ->
    case should_log(Level) of
        true ->
            Timestamp = log_timestamp(),
            Msg = lists:flatten(io_lib:format(Format, [])),
            io:format("~s [~s] ~s~n", [Timestamp, string:to_upper(atom_to_list(Level)), Msg]);
        false ->
            ok
    end.

log(Level, Format, Args) ->
    case should_log(Level) of
        true ->
            Timestamp = log_timestamp(),
            Msg = lists:flatten(io_lib:format(Format, Args)),
            io:format("~s [~s] ~s~n", [Timestamp, string:to_upper(atom_to_list(Level)), Msg]);
        false ->
            ok
    end.

should_log(Level) ->
    Current =  persistent_term:get(?LOG_LEVEL_KEY, ?DEFAULT_LEVEL),
    case {level(Level), level(Current)} of
        {{ok, L1}, {ok, L2}} -> L1 >= L2;
        _ -> false 
    end.


log_timestamp() ->
    {{Y, M, D}, {H, Min, S}} = calendar:local_time(),
    lists:flatten(io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B", [Y, M, D, H, Min, S])).
    
level(debug) -> {ok, 0};
level(info) -> {ok, 1};
level(warn) -> {ok, 2};
level(error) -> {ok, 3};
level(important) -> {ok, 4};
level(_) -> {error, unknown_level}.