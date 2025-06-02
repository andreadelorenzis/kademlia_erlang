# Kademlia-Erlang
A simple implementation of the Kademlia DHT (Distributed Hash Table) protocol in Erlang.
## Features
- Basic Kademlia protocol: STORE, FIND_NODE, FIND_VALUE, PING
- Configurable parameters: `k`, `alpha`, ID size, intervals.
- Asynchronous and concurrent architecture, leveraging Erlang processes
- Periodic refresh, republish, expiration mechanisms
- Logging and metrics collection
## Getting started
### 1) Clone the project
```bash
git clone https://github.com/andreadelorenzis/kademlia_erlang.git
cd kademlia_erlang
```
### 2) Compile the sources
To compile all the sources execute this command from the shell in the root directory:
```bash
erl
make:all([{outdir, './ebin'}]).
```
### 3) Start the shell
Start the Erlang shell while pointing it to the `/ebin` folder with:
```erlang
erl -pa ebin
```
## Usage
### Create a network
```erlang
test:prepare_network(BootstrapCount, NodeCount).
```
### Basic operations
```erlang
test:ping(SenderPid, ReceiverPid).                 % => alive | dead
test:store(NodePid, Value).                        % => {store_response, AckNodes}
test:find_node(NodePid, TargetId).                 % => {find_node_response, KClosest}
test:find_value(NodePid, Key).                     % => {find_value_response, Entry, Hops} | not_found
test:refresh_node(Node).                           % Refresh routing table
```
### Debugging & introspection
```erlang
test:print_buckets(NodePid).
test:print_buckets_with_distance(Node).
test:print_buckets_with_distance(Node, TargetId).
test:print_storage(NodePid).
```
### Benchmarks
```erlang
test:run_lookup_measurements().
test:run_measurements_with_failure().
test:run_join_measurements().
```
### Configuration
Default options used in `test:prepare_network/2`:
```erlang
#{
  k_param => 10,
  alpha_param => 2,
  id_byte_length => 2,
  republish_interval => 30000,
  expiration_interval => 86400000,
  refresh_interval => 3600000,
  timeout_interval => 2000,
  log_level => important,
  refresh => true
}
```
