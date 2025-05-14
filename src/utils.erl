-module(utils).
-export([generate_node_id/1, order_unique/2, find_bucket_index/3, xor_distance/2, 
         calculate_key/2, generate_random_id_in_bucket/3, is_bootstrap/1, print_buckets/4,
         print_buckets/3, print_storage/3, print_nodes/2, print_storage/4, print_nodes/3]).


% Calculate XOR between binary IDs
xor_distance(A, B) ->
    list_to_binary(lists:map(fun({X, Y}) -> X bxor Y end, 
                   lists:zip(binary_to_list(A), binary_to_list(B)))).



% Generate a random ID with specified number of bytes
generate_node_id(NumBytes) when is_integer(NumBytes), NumBytes > 0 ->
    crypto:strong_rand_bytes(NumBytes).

% Generate an hash of a key with a specified number of bytes
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


% Generate a random ID in the specified bucket
generate_random_id_in_bucket(LocalId, BucketIndex, IdByteLength) ->

    % Generate random binary ID of the specified length
    RandomId = crypto:strong_rand_bytes(IdByteLength), 
    
    % Create an ID that is formed by concatenating a prefix equal to LocalID (up to BucketIndex), 
    % the inverted BucketIndex bit, and the rest from RandomID

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
    NewByte = (LocalByte band Mask) bxor (BitMask) bor (RandomByte band (255 - Mask - BitMask)),
    
    % Remaining bytes are random
    NewIdSuffixBytes = lists:nthtail(BytePos + 1, RandomBytes),
    
    % Concatenates the parts
    GeneratedID = list_to_binary(NewIdPrefixBytes ++ [NewByte] ++ NewIdSuffixBytes),
    GeneratedID.


% Order the nodes based on XOR distance and remove duplicates
order_unique(Nodes, TargetID) ->
	UniqueNodes = remove_duplicates(Nodes),
	lists:sort(
		fun({_, ID1, _Pid1}, {_, ID2, _Pid2}) -> 
			xor_distance(ID1, TargetID) < xor_distance(ID2, TargetID) 
		end,
		UniqueNodes
	).

% Find the most significant bit of difference between the two IDs
find_bucket_index(LocalId, RemoteId, IdBitLength) ->
    % Calculate XOR of the two IDs
    Distance = xor_distance(LocalId, RemoteId),
    
    % Find MSB
    find_msb_set(Distance, IdBitLength). 


is_bootstrap(Name) ->
    case atom_to_list(Name) of
        "bootstrap_sup" -> false;
        "bootstrap" ++ _ -> true;
        _ -> false
    end.


%%
%% Pretty prints the contents of the buckets list
%%
print_buckets(LocalName, Pid, Buckets) ->
    print_buckets(LocalName, Pid, Buckets, debug).

print_buckets(LocalName, Pid, Buckets, LogLevel) ->
    Header = lists:flatten(io_lib:format("~n~n------ BUCKETS OF ~p (~p)------~n", [LocalName, Pid])),
    Footer = io_lib:format("~n------ END BUCKETS ------~n", []),
    Lines = print_buckets_acc(LocalName, Buckets, 1, []),
    log_msg(LogLevel, "~p (~p): ~s~s~s", [LocalName, Pid, Header, Lines, Footer]).

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


%%
%% Pretty prints the contents of the storage
%%
print_storage(LocalName, Pid, Storage) ->
    print_storage(LocalName, Pid, Storage, debug).

print_storage(LocalName, Pid, Storage, LogLevel) -> 
    Pairs = maps:to_list(Storage),
    Lines = [format_line(Key, Entry) || {Key, Entry} <- Pairs],
    Header = lists:flatten(io_lib:format("~n~n------ STORAGE OF ~p (~p)------~n", [LocalName, Pid])),
    Footer = io_lib:format("~n------ END STORAGE ------~n~n", []),
    Text = string:join(Lines, "\n"),
    log_msg(LogLevel, "~p (~p): ~s~s~s", [LocalName, Pid, Header, lists:flatten(Text), Footer]).

format_line(_Key, Entry) ->
    {Value, Expiry, RepublishTime, {_OwnerID, OwnerPid}} = Entry,
    Line = io_lib:format("--- ~p (value), ~p (expiry), ~p (republish), ~p (owner)", 
        [Value, Expiry, RepublishTime, OwnerPid]),
    lists:flatten(Line).


%%
%% Pretty prints the provided list of nodes of the form {NodeName, NodeId, NodePid}
%%
print_nodes(Nodes, Prefix) ->
    print_nodes(Nodes, Prefix, debug).

print_nodes(Nodes, StringPrefix, LogLevel) ->
    NodesMapped = lists:map(fun({NodeName, _, NodePid}) -> {NodeName, NodePid} end, Nodes),
    log_msg(LogLevel, "~n~s~p~n", [StringPrefix, NodesMapped]).


%% Dynamic log level
log_msg(debug, Fmt, Args) -> log:debug(Fmt, Args);
log_msg(info, Fmt, Args)  -> log:info(Fmt, Args);
log_msg(warn, Fmt, Args)  -> log:warn(Fmt, Args);
log_msg(error, Fmt, Args) -> log:error(Fmt, Args);
log_msg(important, Fmt, Args) -> log:important(Fmt, Args);
log_msg(_, Fmt, Args)     -> log:debug(Fmt, Args).  % fallback


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


remove_duplicates(Nodes) ->
	lists:foldl(fun(Node, Acc) ->
		case lists:member(Node, Acc) of 
            true -> Acc; % remove node
            _ -> [Node | Acc] % keep node
		end
	end, [], Nodes).


% Convert distance xor to integer
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