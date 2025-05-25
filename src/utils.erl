-module(utils).
-export([generate_node_id/1, order_unique/2, find_bucket_index/3, 
         xor_distance/2, calculate_key/2, generate_random_id_in_bucket/3, 
         is_bootstrap/1, print_buckets/4,
         print_buckets/3, print_storage/3, print_nodes/2, print_storage/4, 
         print_nodes/3, xor_distance_integer/2, print_nodes_with_distance/3, 
         print_buckets_with_distance/5, print_buckets_with_distance/6]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Calculates the XOR distance between two binary IDs of equal length.
xor_distance(A, B) when byte_size(A) =:= byte_size(B) ->
    list_to_binary(
        lists:map(fun({X, Y}) -> X bxor Y end, 
                  lists:zip(binary_to_list(A), binary_to_list(B))));
xor_distance(A, B) ->
    error({invalid_binary_lengths, byte_size(A), byte_size(B)}).


%% Calculates the XOR distance between two binary IDs and returns it as an 
%% integer.
xor_distance_integer(A, B) ->
    BinaryXor = xor_distance(A, B),
    binary:decode_unsigned(BinaryXor).


%% Generates a random node ID with the specified number of bytes.
generate_node_id(NumBytes) when is_integer(NumBytes), NumBytes > 0 ->
    crypto:strong_rand_bytes(NumBytes).


%% Calculates a SHA-256 hash key from a value, truncated to the specified 
%% number of bytes.
calculate_key(Value, NumBytes) when is_integer(NumBytes), NumBytes > 0 ->
    % Convert key to binary
    ValueBin = if
        is_binary(Value) -> Value;
        is_list(Value) -> list_to_binary(Value);
        true -> term_to_binary(Value)
    end,
    
    % Generate hash SHA-256 of the value
    FullHash = crypto:hash(sha256, ValueBin),
    
    % Take only desired number of bytes
    BytesToUse = min(NumBytes, byte_size(FullHash)),
    <<HashPart:BytesToUse/binary, _/binary>> = FullHash,
    HashPart.


%% Generates a random ID that would fall into a specific bucket relative to 
%% a local ID.
generate_random_id_in_bucket(LocalId, BucketIndex, IdByteLength) ->
    % Create an ID that is formed by concatenating a prefix equal to 
    % LocalID (up to BucketIndex), the inverted BucketIndex bit, and the 
    % rest from RandomID

    % Generate random binary ID of the specified length
    RandomId = crypto:strong_rand_bytes(IdByteLength), 
    % Calculate the position of the byte in LocalID
    BytePos = BucketIndex div 8,
    % Calculate the bit offset in the byte from the right, 
    BitPos = 7 - (BucketIndex rem 8), 
    % Converts the binaries to lists of bytes 
    LocalBytes = binary_to_list(LocalId),
    RandomBytes = binary_to_list(RandomId),
    % Prefix equal to LocalID (before BucketIndex)
    NewIdPrefixBytes = lists:sublist(LocalBytes, BytePos),
    % Byte in BytePos (the bucket index starts from zero, so it adds 1)
    LocalByte = lists:nth(BytePos + 1, LocalBytes),
    RandomByte = lists:nth(BytePos + 1, RandomBytes),
    % Mask for bits that need to be equal to local ID
    Mask = (255 bsl (BitPos + 1)),
    % Mask for bit that needs to be different
    BitMask = (1 bsl BitPos),
    % New byte is equal to:
    %   bits up to bit pos equal to local byte + 
    %   inverted bit + 
    %   bits after bitPos equal to random byte
    NewByte = (LocalByte band Mask) bxor (BitMask) bor (RandomByte band (255 - Mask - BitMask)) band 255,
    % Remaining bytes are random
    NewIdSuffixBytes = lists:nthtail(BytePos + 1, RandomBytes),
    % Concatenates the parts
    GeneratedID = list_to_binary(NewIdPrefixBytes ++ [NewByte] ++ NewIdSuffixBytes),
    GeneratedID.


%% Orders a list of nodes by XOR distance to a target ID and removes 
%% duplicates.
order_unique(Nodes, TargetID) ->
	UniqueNodes = remove_duplicates(Nodes),
	lists:sort(
		fun({_, ID1, _Pid1}, {_, ID2, _Pid2}) -> 
			xor_distance(ID1, TargetID) < xor_distance(ID2, TargetID) 
		end,
		UniqueNodes
	).


%% Finds the bucket index (distance) between a local ID and remote ID.
find_bucket_index(LocalId, RemoteId, IdBitLength) ->
    % Calculate XOR of the two IDs
    Distance = xor_distance(LocalId, RemoteId),
    % Find MSB
    find_msb_set(Distance, IdBitLength). 


%% Checks if a node name indicates it's a bootstrap node.
is_bootstrap(Name) ->
    case atom_to_list(Name) of
        "bootstrap_sup" -> false;
        "bootstrap" ++ _ -> true;
        _ -> false
    end.


%% Prints the contents of routing buckets in a readable format.
print_buckets(LocalName, Pid, Buckets) ->
    print_buckets(LocalName, Pid, Buckets, debug).
print_buckets(LocalName, Pid, Buckets, LogLevel) ->
    Header = lists:flatten(io_lib:format("~n~n------ BUCKETS OF ~p (~p)------~n", [LocalName, Pid])),
    Footer = io_lib:format("~n------ END BUCKETS ------~n", []),
    Lines = print_buckets_acc(LocalName, Buckets, 1, []),
    log_msg(LogLevel, "~p (~p): ~s~s~s", [LocalName, Pid, Header, Lines, Footer]).


%% Prints routing buckets with XOR distance information relative to local ID.
print_buckets_with_distance(LocalId, LocalName, Pid, Buckets, LogLevel) ->
    print_buckets_with_distance(LocalId, LocalName, Pid, Buckets, undefined, LogLevel).

%% Prints routing buckets with XOR distance information relative to local 
%% and a target ID.
print_buckets_with_distance(LocalId, LocalName, Pid, Buckets, TargetId, LogLevel) ->
    Header = lists:flatten(io_lib:format("~n~n------ BUCKETS OF ~p (~p)------~n", [LocalName, Pid])),
    Footer = "\n------ END BUCKETS ------\n",
    Lines = print_buckets_with_distance_acc(LocalId, Buckets, 1, [], TargetId),
    Body = string:join(lists:reverse(Lines), "\n"),
    log_msg(LogLevel, "~p (~p): ~s~s~s", [LocalName, Pid, Header, Body, Footer]).


%% Prints the contents of a node's key-value storage.
print_storage(LocalName, Pid, Storage) ->
    print_storage(LocalName, Pid, Storage, debug).

print_storage(LocalName, Pid, Storage, LogLevel) -> 
    Pairs = maps:to_list(Storage),
    Lines = [format_line(Key, Entry) || {Key, Entry} <- Pairs],
    Header = lists:flatten(io_lib:format("~n~n------ STORAGE OF ~p (~p)------~n", [LocalName, Pid])),
    Footer = io_lib:format("~n------ END STORAGE ------~n~n", []),
    Text = string:join(Lines, "\n"),
    log_msg(LogLevel, "~p (~p): ~s~s~s", [LocalName, Pid, Header, lists:flatten(Text), Footer]).


%% Prints a list of nodes in a readable format.
print_nodes(Nodes, Text) ->
    print_nodes(Nodes, Text, debug).

print_nodes(Nodes, Text, LogLevel) ->
    NodesMapped = lists:map(fun({NodeName, _, NodePid}) -> {NodeName, NodePid} end, Nodes),
    log_msg(LogLevel, "~n~s~p~n", [Text, NodesMapped]).


%% Prints nodes with their XOR distance to a target ID.
print_nodes_with_distance(Nodes, TargetId, Text) ->
    NodesWithDist = lists:map(
        fun({NodeName, NodeId, NodePid}) ->
            Dist = xor_distance_integer(NodeId, TargetId),
            {NodeName, NodePid, Dist}
        end,
        Nodes
    ),
    Sorted = lists:sort(fun({_, _, D1}, {_, _, D2}) -> D1 =< D2 end, NodesWithDist),
    Lines = [io_lib:format("--- ~p ~p (distance: ~p)", [Name, Pid, Dist]) || {Name, Pid, Dist} <- Sorted],
    Header = lists:flatten(io_lib:format("~n~n~s~n", [Text])),
    Body = string:join([lists:flatten(Line) || Line <- Lines], "\n"),
    Footer = "\n------ END NODES ------\n",
    log_msg(debug, "~s~s~s", [Header, Body, Footer]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Removes duplicate nodes from a list.
remove_duplicates(Nodes) ->
	lists:foldl(fun(Node, Acc) ->
		case lists:member(Node, Acc) of 
            true -> Acc; % remove node
            _ -> [Node | Acc] % keep node
		end
	end, [], Nodes).


%% Helper function that finds the most significant bit set in a XOR distance.
find_msb_set(Distance, IdBitLength) when is_binary(Distance) ->
    IntDistance = binary:decode_unsigned(Distance),
    find_msb_set(IntDistance, 0, IdBitLength).

% Base case: zero distance -> no difference between the two IDs
find_msb_set(0, _, _) -> 0;
% Recursive function that iterates on the bits
find_msb_set(Distance, BitPos, IdBitLength) ->
    BitMask = 1 bsl ((IdBitLength-1) - BitPos),
    case Distance band BitMask of
        0 -> 
            % Current bit is zero -> continue to next bit
            find_msb_set(Distance, BitPos + 1, IdBitLength);
        _ -> 
            % Current bit is one -> found MSB
            BitPos
    end.


format_line(_Key, Entry) ->
    {Value, Expiry, RepublishTime, {_, _, OwnerPid}} = Entry,
    Line = io_lib:format("--- ~p (value), ~p (expiry), ~p (republish), ~p (owner)", 
        [Value, Expiry, RepublishTime, OwnerPid]),
    lists:flatten(Line).


print_buckets_acc(_, [], _Index, Acc) ->
    string:join(lists:reverse(Acc), "\n");
print_buckets_acc(LocalName, [H | T], Index, Acc) ->
    case H of
        [] -> print_buckets_acc(LocalName, T, Index + 1, Acc);
        _ -> 
            Nodes = [{Name, Pid} || {Name, _, Pid} <- H],
            Line = io_lib:format("--- ~p: ~p", [Index, Nodes]),
            print_buckets_acc(LocalName, T, Index + 1, [lists:flatten(Line) | Acc])
    end.


print_buckets_with_distance_acc(_, [], _, Acc, _) ->
    Acc;
print_buckets_with_distance_acc(LocalId, [H | T], Index, Acc, TargetId) ->
    case H of
        [] -> print_buckets_with_distance_acc(LocalId, T, Index + 1, Acc, TargetId);
        _ ->
            Nodes = [
                case TargetId of
                    undefined ->
                        {Name, Pid, xor_distance_integer(NodeId, LocalId)};
                    _ ->
                        {Name, Pid, xor_distance_integer(NodeId, LocalId), xor_distance_integer(NodeId, TargetId)}
                end
                || {Name, NodeId, Pid} <- H
            ],
            Line = io_lib:format("--- ~p: ~p", [Index, Nodes]),
            print_buckets_with_distance_acc(LocalId, T, Index + 1, [lists:flatten(Line) | Acc], TargetId)
    end.


%% Helper function to log messages with different severity levels.
log_msg(debug, Fmt, Args) -> log:debug(Fmt, Args);
log_msg(info, Fmt, Args)  -> log:info(Fmt, Args);
log_msg(warn, Fmt, Args)  -> log:warn(Fmt, Args);
log_msg(error, Fmt, Args) -> log:error(Fmt, Args);
log_msg(important, Fmt, Args) -> log:important(Fmt, Args);
log_msg(_, Fmt, Args)     -> log:debug(Fmt, Args).