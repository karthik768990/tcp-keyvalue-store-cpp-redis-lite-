# 🚀 C++ Key-Value Store (Redis-Inspired TCP Project)

This project is a **custom-built, minimal Redis-like key-value database system** implemented in modern C++. It includes a robust TCP server and a feature-rich client with custom protocol support, connection management, and local command history.

Designed to mimic the behavior of Redis using raw sockets and binary protocol encoding, it's a great learning resource for understanding:

- Sockets (TCP/IP)
- Custom binary protocol design
- Event-driven client-server communication
- Memory-safe and low-level systems programming in C++
- Efficient maintainance of the idle connections
- Data structures
- Manual memory management
   
---

## ✨ Features

### 🖥 Server
- Accepts multiple client connections
- Parses custom binary protocol
- Supports TTL-based expiration
- Automatically removes idle connections
- Handles ZSET (sorted set) operations

### 🧑‍💻 Client
- Command-line interface
- Automatic reconnection handling
- 10-second socket timeout with robust error handling
- Maintains local history of last 10 commands
- 'hist' command to display recent commands with timestamps
- Graceful exit on entering 'quit'

### ⚙️ Protocol
- Fully custom binary message framing
- Efficient type-tagged responses (nil, string, integer, float, array, error)
-Efficient responses given by the server in the binary format which is universally accepted that are 1 as success and 0 as failure 
---

## 📦 Supported Commands
______________________________________________________________________________
| Command                      | Description                                  |
|------------------------------|----------------------------------------------|
| 'SET key value'              | Set a key to a given value                   |
| 'GET key'                    | Retrieve the value of a key                  |
| 'DEL key'                    | Delete a key                                 |
| 'EXISTS key'                 | Check if a key exists                        |
| 'INCR key'                   | Increment the integer value of a key         |
| 'DECR key'                   | Decrement the integer value of a key         |
| 'EXPIRE key seconds'         | Set a TTL (time-to-live) on a key            |
| 'TTL key'                    | Show remaining TTL for a key                 |
| 'ZADD key score member'      | Add a member with score to a sorted set      |
| 'ZREM key member'            | Remove a member from a sorted set            |
| 'ZRANGE key start stop'      | Get a range of elements from a sorted set    |
| 'ZCARD key'                  | Get the number of members in the sorted set  |
| 'ZSCORE key member'          | Get the score of a specific member           |
| 'HIST' *(client-side only)*  | Show the last 10 commands with timestamps    |
| 'QUIT'                       | Exit the client gracefully                   |
| 'PEXPIRE <key> milli sec'    | Set key to expire in N milliseconds          |
|'TTL key'                     | Get the remaining time to live in seconds    |
|'PTTL key'                    | Get the remaining time to live in milli sec  |
| 'KEYS'                       | Returns all the keys                         |
|______________________________|______________________________________________|


> ✅ All commands except 'HIST' are sent to the server using a binary protocol.

---

## 🔧 Build Instructions

### ✅ Requirements

- A Linux-based system or WSL
- 'g++' with C++17 or newer
- Basic familiarity with terminal usage

### 🔨 Compile

'''bash
g++ -std=c++17 -o server server.cpp
g++ -std=c++17 -o client client.cpp
## Usage 
- Clone the repository from the terminal of ubuntu based kernels using
 git clone https://github.com/karthik768990/tcp-keyvalue-store-ccp-redis-lite.git
-Then navigate to the folder using
 cd tcp-keyvalue-store-ccp-redis-lite
-Compile both the server and the client codes using the commands mentions above and then run the cliend and the server separately in two separate terminals using
./server
./client
-Use the terminals input as the input of the commands from the client side and go with it and use the server.
### FeedBack
-If there is any query or improvements feel free to contach with the mail 
karthiktamarapalli5437@gmail.com

