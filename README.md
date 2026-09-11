# Rustis

A distributed, high-performance key-value store inspired by Redis, built with Rust. Rustis implements a master-slave architecture with consistent hashing for scalable data distribution across multiple nodes.

## Why Rustis?

Rustis provides a scalable, fault-tolerant in-memory key-value store with the following advantages:

- **Distributed Architecture**: Master-slave topology with consistent hashing for even data distribution
- **High Performance**: Built with Rust for memory safety and zero-cost abstractions
- **Flexible Eviction Policies**: Support for FIFO, LIFO, and LRU cache eviction strategies
- **TTL Support**: Time-to-live functionality for automatic key expiration
- **Redundancy & Failover**: Built-in redundancy mechanisms for high availability
- **Virtual Nodes**: Uses virtual nodes for better load balancing in consistent hashing
- **RESTful API**: Simple HTTP/JSON interface for easy integration

## Features

- **Consistent Hashing**: Distributes keys across multiple slave nodes using virtual nodes
- **Multiple Eviction Policies**: FIFO, LIFO, and LRU for cache management
- **TTL (Time-To-Live)**: Automatic expiration of keys after specified duration
- **Master-Slave Architecture**: Central orchestrator routes requests to appropriate slave nodes
- **Redundancy Support**: Configurable redundancy for fault tolerance
- **Health Checks**: Built-in health monitoring endpoints
- **Bulk Operations**: Support for batch insert operations
- **IPv4 & IPv6 Support**: Works with both IP address formats

## Dependencies

```toml
[dependencies]
xxhash-rust = { version = "0.8", features = ["xxh3"] }
rand = "0.9"
axum = "0.8"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
reqwest = { version = "0.12", features = ["json"] }
uuid = { version = "1", features = ["v4", "serde"] }
```

## Building

```bash
# Clone the repository
git clone <repository-url>
cd Rustis

# Build the project
cargo build --release

# Run tests (if available)
cargo test
```

## Usage

### Starting a Master Node

The master node acts as the orchestrator, routing client requests to appropriate slave nodes based on consistent hashing.

```bash
# Start master on default port 8080
cargo run -- --role=master

# Start master on custom port
cargo run -- --role=master --port=9000

# Enable debug mode
cargo run -- --role=master --debug
```

### Starting a Slave Node

Slave nodes store the actual key-value data and serve client requests routed by the master.

```bash
# Start slave with default settings
cargo run -- --role=slave

# Start slave with custom port and cache capacity
cargo run -- --role=slave --port=8081 --cap=1000

# Start slave with eviction policy
cargo run -- --role=slave --cap=1000 --evic_policy=lru

# Start slave with debug mode
cargo run -- --role=slave --debug
```

### Command Line Arguments

| Argument | Short | Description | Default |
|----------|-------|-------------|---------|
| `--role` | `-r` | Node role (master/slave) | slave |
| `--port` | `-p` | Port number | 8080 |
| `--cap` | `-c` | Cache capacity (number of KV pairs) | unlimited |
| `--evic_policy` | `-ep` | Eviction policy (fifo/lifo/lru) | none |
| `--n_vnodes` | `-n` | Number of virtual nodes | 4 |
| `--debug` | `-d` | Enable debug mode | false |
| `--help` | `-h` | Show help message | - |

## API Endpoints

### Master Endpoints

All master endpoints route requests to the appropriate slave node based on consistent hashing.

#### Add Key-Value Pair
```bash
POST /item/{key}/{value}?ttl={optional_seconds}
```

#### Get Value by Key
```bash
GET /item/{key}
```

#### Update Key
```bash
PUT /item/{key}?value={new_value}&ttl={optional_seconds}
```

#### Delete Key
```bash
DELETE /item/{key}
```

#### Check if Key Exists
```bash
GET /key/{key}
```

#### Check if Value Exists
```bash
GET /value/{value}
```

#### Get All Keys
```bash
GET /key
```

#### Get All Values
```bash
GET /value
```

#### Get All Key-Value Pairs
```bash
GET /item
```

#### Bulk Insert
```bash
POST /item
Content-Type: application/json

[
  ["key1", "value1", 60],
  ["key2", "value2", null],
  ["key3", "value3", 120]
]
```

### Slave Endpoints

Slave nodes expose the same endpoints for direct access, plus additional internal endpoints:

#### Health Check
```bash
GET /health
```

#### Master Communication (Internal)
```bash
POST /comm
GET /comm
```

#### Transfer/Failover (Internal)
```bash
POST /transfer
```

## Examples

### Example 1: Basic Key-Value Operations

```bash
# Start master
cargo run -- --role=master --port=8080

# Start slave 1
cargo run -- --role=slave --port=8081 --cap=100 --evic_policy=lru

# Start slave 2
cargo run -- --role=slave --port=8082 --cap=100 --evic_policy=fifo

# Add a key-value pair (via master)
curl -X POST http://localhost:8080/item/name/John

# Get a value (via master)
curl http://localhost:8080/item/name

# Update a value
curl -X PUT http://localhost:8080/item/name?value=Jane

# Delete a key
curl -X DELETE http://localhost:8080/item/name

# Check if key exists
curl http://localhost:8080/key/name
```

### Example 2: Using TTL

```bash
# Add key with 60 second TTL
curl -X POST "http://localhost:8080/item/session/abc123?ttl=60"

# Key will automatically expire after 60 seconds
```

### Example 3: Bulk Operations

```bash
# Insert multiple key-value pairs at once
curl -X POST http://localhost:8080/item \
  -H "Content-Type: application/json" \
  -d '[
    ["user1", "Alice", null],
    ["user2", "Bob", 300],
    ["user3", "Charlie", 600]
  ]'
```

### Example 4: Direct Slave Access

```bash
# Access slave directly (bypasses master)
curl http://localhost:8081/item/name

# Check slave health
curl http://localhost:8081/health
```

## Architecture

### Master Node (Orchestrator)
- Maintains a consistent hash ring with virtual nodes
- Routes client requests to appropriate slave nodes
- Manages node topology and failover
- Coordinates between slave nodes

### Slave Node (Cache)
- Stores key-value pairs in memory
- Implements eviction policies (FIFO, LIFO, LRU)
- Handles TTL expiration
- Provides redundancy and failover support
- Communicates with master for topology updates

### Consistent Hashing
- Uses virtual nodes for better load distribution
- Minimizes data movement when nodes are added/removed
- Hash ring ensures even distribution across slaves

## Eviction Policies

- **FIFO (First In, First Out)**: Evicts the oldest inserted key
- **LIFO (Last In, First Out)**: Evicts the most recently inserted key
- **LRU (Least Recently Used)**: Evicts the least recently accessed key

## License

This project is licensed under GPL-3.0-only. See the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Future Enhancements

- [ ] Persistence to disk
- [ ] Authentication and authorization
- [ ] TLS/SSL support
- [ ] Cluster management UI
- [ ] Metrics and monitoring
- [ ] Transaction support
- [ ] Pub/Sub messaging
