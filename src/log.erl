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


%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Starts the logger with a default log level.
start() ->
    put(?LOG_LEVEL_KEY, ?DEFAULT_LEVEL),
    case file:open("log.txt", [write, utf8]) of
        {ok, Fd} ->
            persistent_term:put(?LOG_FILE_KEY, Fd),
            io:format("Log system initialized with file logging.~n");
        {error, Reason} ->
            io:format("Failed to open log file: ~p~n", [Reason])
    end.


%% Sets the log level for the local process.
set_level(Level) ->
    case level(Level) of
        {ok, _} ->
            put(?LOG_LEVEL_KEY, Level),
            io:format("Log level set with: ~p~n", [get(?LOG_LEVEL_KEY)]);
        {error, unknown_level} ->
            io:format("Invalid log level: ~p (keeping current level: ~p)~n", 
                     [Level, get(?LOG_LEVEL_KEY)])
    end.


%% Sets the log level for all the spawned processes. 
set_level_global(Level) ->
    case level(Level) of
        {ok, _} ->
            persistent_term:put(?LOG_LEVEL_KEY, Level),
            io:format("Log level set with: ~p~n", [Level]),
            ok;
        _ ->
            {error, invalid_level}
    end.


%% Logs formatted string without other log info (e.g. time)
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


%% Clears console screen and move cursor to top-left.
clean_console() ->
    io:format("\e[2J\e[H").


%% Logs a string with debug level.
debug(Format) ->
    log(debug, Format).
debug(Format, Args) ->
    log(debug, Format, Args).


%% Logs a string with info level.
info(Format) ->
    log(info, Format).
info(Format, Args) ->
    log(info, Format, Args).


%% Logs a string with warn level.
warn(Format) ->
    log(warn, Format).
warn(Format, Args) ->
    log(warn, Format, Args).


%% Logs a string with error level.
error(Format) ->
    log(error, Format).
error(Format, Args) ->
    log(error, Format, Args).


%% Logs a string with important level.
important(Format) ->
    log(important, Format).
important(Format, Args) ->
    log(important, Format, Args).


%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Log formatted string withouth arguments.
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

%% Log formatted string with arguments.
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


%% Logs a formatted string into a file.
write_log(Line) ->
    io:put_chars(Line),
    case persistent_term:get(?LOG_FILE_KEY) of
        undefined -> io:format("~nfailed to log~n"); % stdout fallback
        Fd -> file:write(Fd, Line)
    end.


%% Logs a formatted string into a file without other log info (e.g. time)
write_raw_log(Line) ->
    LineStr = lists:flatten(Line) ++ "\n",
    io:put_chars(LineStr),
    case persistent_term:get(?LOG_FILE_KEY) of
        undefined -> ok;
        Fd -> file:write(Fd, LineStr)
    end.


%% Checks if the passed level has at least the same priority of the level
%% currently set.
should_log(Level) ->
    Current =  persistent_term:get(?LOG_LEVEL_KEY, ?DEFAULT_LEVEL),
    case {level(Level), level(Current)} of
        {{ok, L1}, {ok, L2}} -> L1 >= L2;
        _ -> false 
    end.


%% Logs a timestamp for every non-raw log (Year, Month, Day, Hour, Minute, 
%% Second, Millisecond).
log_timestamp() ->
    {{Y, M, D}, {H, Min, S}} = calendar:local_time(),
    {_Mega, _Sec, Micro} = erlang:timestamp(),
    Milli = Micro div 1000,
    lists:flatten(io_lib:format("~4..0B-~2..0B-~2..0B ~2..0B:~2..0B:~2..0B.~3..0B", 
        [Y, M, D, H, Min, S, Milli])).
    

%% Level priorities.
level(debug) -> {ok, 0};
level(info) -> {ok, 1};
level(warn) -> {ok, 2};
level(error) -> {ok, 3};
level(important) -> {ok, 4};
level(_) -> {error, unknown_level}.