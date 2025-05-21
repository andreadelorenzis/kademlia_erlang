-module(log).
-export([
    start/0, 
    log/3, log/2, 
    set_level/1,
    set_level_global/1,
    debug/1, debug/2,
    info/1, info/2,
    warn/1, warn/2,
    error/1, error/2,
    important/1, important/2,
    raw/2, raw/3,
    clean_console/0
]).

-define(DEFAULT_LEVEL, info).
-define(LOG_LEVEL_KEY, '__log_level__').
-define(LOG_FILE_KEY, '__log_file__').

start() ->
    put(?LOG_LEVEL_KEY, ?DEFAULT_LEVEL),
    case file:open("log.txt", [write, utf8]) of
        {ok, Fd} ->
            persistent_term:put(?LOG_FILE_KEY, Fd),
            io:format("Log system initialized with file logging.~n");
        {error, Reason} ->
            io:format("Failed to open log file: ~p~n", [Reason])
    end.

set_level(Level) ->
    case level(Level) of
        {ok, _} ->
            put(?LOG_LEVEL_KEY, Level),
            io:format("Log level set with: ~p~n", [get(?LOG_LEVEL_KEY)]);
        {error, unknown_level} ->
            io:format("Invalid log level: ~p (keeping current level: ~p)~n", 
                     [Level, get(?LOG_LEVEL_KEY)])
    end.

set_level_global(Level) ->
    case level(Level) of
        {ok, _} ->
            persistent_term:put(?LOG_LEVEL_KEY, Level),
            io:format("Log level set with: ~p~n", [Level]),
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
            Line = io_lib:format("~s [~s] ~s~n", [Timestamp, string:to_upper(atom_to_list(Level)), Msg]),
            write_log(lists:flatten(Line));
        false ->
            ok
    end.

log(Level, Format, Args) ->
    case should_log(Level) of
        true ->
            Timestamp = log_timestamp(),
            Msg = lists:flatten(io_lib:format(Format, Args)),
            Line = io_lib:format("~s [~s] ~s~n", [Timestamp, string:to_upper(atom_to_list(Level)), Msg]),
            write_log(lists:flatten(Line));
        false ->
            ok
    end.

write_log(Line) ->
    io:put_chars(Line),
    case persistent_term:get(?LOG_FILE_KEY) of
        undefined -> io:format("~nfailed to log~n"); % stdout fallback
        Fd -> file:write(Fd, Line)
    end.

clean_console() ->
    % Clear screen and move cursor to top-left
    io:format("\e[2J\e[H").


raw(Level, Format) ->
    raw(Level, Format, []).

raw(Level, Format, Args) ->
    case should_log(Level) of
        true ->
            Msg = lists:flatten(io_lib:format(Format, Args)),
            write_raw_log(Msg);
        false ->
            ok
    end.

write_raw_log(Line) ->
    LineStr = lists:flatten(Line) ++ "\n",
    io:put_chars(LineStr),
    case persistent_term:get(?LOG_FILE_KEY) of
        undefined -> ok;
        Fd -> file:write(Fd, LineStr)
    end.


should_log(Level) ->
    Current =  persistent_term:get(?LOG_LEVEL_KEY, ?DEFAULT_LEVEL),
    case {level(Level), level(Current)} of
        {{ok, L1}, {ok, L2}} -> L1 >= L2;
        _ -> false 
    end.


log_timestamp() ->
    {{Y, M, D}, {H, Min, S}} = calendar:local_time(),
    {_Mega, _Sec, Micro} = erlang:timestamp(),
    Milli = Micro div 1000,
    lists:flatten(io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B.~3..0B", 
        [Y, M, D, H, Min, S, Milli])).
    
level(debug) -> {ok, 0};
level(info) -> {ok, 1};
level(warn) -> {ok, 2};
level(error) -> {ok, 3};
level(important) -> {ok, 4};
level(_) -> {error, unknown_level}.