-module(utils).
-export([generate_node_id_160bit/0, order_unique/2, find_bucket_index/2, xor_distance/2, 
         calculate_key/1, generate_random_id_in_bucket/2, is_bootstrap/1]).


% Calculate XOR between two 160 bit IDs
xor_distance(A, B) when byte_size(A) =:= 20, byte_size(B) =:= 20 ->
    list_to_binary(lists:map(fun({X, Y}) -> X bxor Y end, 
                   lists:zip(binary_to_list(A), binary_to_list(B)))).



% Generate 20 random bytes (160 bit)
generate_node_id_160bit() ->
    crypto:strong_rand_bytes(20).

% Use SHA-1 on the value to create a 160 bit key
calculate_key(Value) ->
    BinaryValue = to_binary(Value),
    crypto:hash(sha, BinaryValue).

to_binary(Value) when is_binary(Value) -> 
    Value;
to_binary(Value) when is_list(Value) -> 
    case io_lib:printable_list(Value) of  % String (list of characters)
        true -> list_to_binary(Value);
        false -> crash
    end;
to_binary(Value) when is_atom(Value) -> 
    atom_to_binary(Value, utf8);
to_binary(Value) when is_integer(Value) -> 
    integer_to_binary(Value);
to_binary(Value) when is_float(Value) -> 
    float_to_binary(Value, [{decimals, 10}, compact]).


% Generate a random ID in the specified bucket
generate_random_id_in_bucket(LocalId, BucketIndex) ->

    % Generate random 160 bit ID (20 bytes)
    RandomId = crypto:strong_rand_bytes(20), 
    
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
		fun({ID1, _Pid1}, {ID2, _Pid2}) -> 
			xor_distance(ID1, TargetID) < xor_distance(ID2, TargetID) 
		end,
		UniqueNodes
	).

% Find the most significant bit of difference between the two IDs
find_bucket_index(LocalId, RemoteId) ->
    % Calculate XOR of the two IDs
    Distance = xor_distance(LocalId, RemoteId),
    
    % Find MSB
    find_msb_set(Distance). 


is_bootstrap(Name) ->
    case atom_to_list(Name) of
        "bootstrap_sup" -> false;
        "bootstrap" ++ _ -> true;
        _ -> false
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE FUNCTIONS %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


remove_duplicates(Nodes) ->
	lists:foldl(fun(Node, Acc) ->
		case lists:member(Node, Acc) of 
            true -> Acc; % remove node
            _ -> [Node | Acc] % keep node
		end
	end, [], Nodes).


% Convert distance xor to integer
find_msb_set(Distance) when is_binary(Distance) ->
    IntDistance = binary:decode_unsigned(Distance),
    find_msb_set(IntDistance, 0).

% Base case: zero distance -> no difference between the two IDs
find_msb_set(0, _) -> 0;
% Recursive function that iterates on the bits
find_msb_set(Distance, BitPos) ->
    BitMask = 1 bsl (159 - BitPos),
    case Distance band BitMask of
        0 -> 
            % Current bit is zero -> continue to next bit
            find_msb_set(Distance, BitPos + 1);
        _ -> 
            % Current bit is one -> found MSB
            BitPos
    end.