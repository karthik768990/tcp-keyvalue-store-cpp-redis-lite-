#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <errno.h>
#include <deque>

#define MAX_MSG 65536

void die(const char *msg) {
    perror(msg);
    exit(1);
}

void set_socket_timeout(int sock, int seconds) {
    timeval tv;
    tv.tv_sec = seconds;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

int connect_to_server() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }

    return sock;
}

bool write_full(int sock, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t*)buf;
    while (len > 0) {
        ssize_t n = write(sock, p, len);
        if (n <= 0) {
            if (errno == ECONNRESET || errno == EPIPE || errno == EAGAIN || errno == EWOULDBLOCK) {
                std::cerr << "[write_full] Disconnected or timed out.\n";
                return false;
            }
            perror("write");
            return false;
        }
        p += n;
        len -= n;
    }
    return true;
}

bool read_full(int sock, void *buf, size_t len) {
    uint8_t *p = (uint8_t*)buf;
    while (len > 0) {
        ssize_t n = read(sock, p, len);
        if (n <= 0) {
            if (errno == ECONNRESET || errno == EPIPE || errno == EAGAIN || errno == EWOULDBLOCK) {
                std::cerr << "[read_full] Disconnected or timed out.\n";
                return false;
            }
            perror("read");
            return false;
        }
        p += n;
        len -= n;
    }
    return true;
}

bool send_request(int sock, const std::vector<std::string> &args) {
    uint8_t req[MAX_MSG];
    size_t pos = 0;

    uint32_t n = args.size();
    memcpy(&req[pos], &n, 4); pos += 4;

    for (const auto &arg : args) {
        uint32_t len = arg.size();
        memcpy(&req[pos], &len, 4); pos += 4;
        memcpy(&req[pos], arg.data(), len); pos += len;
    }

    uint32_t msg_len = pos;
    return write_full(sock, &msg_len, 4) && write_full(sock, req, msg_len);
}

bool receive_response(int sock) {
    uint32_t len;
    if (!read_full(sock, &len, 4)) return false;
    if (len > MAX_MSG) {
        std::cerr << "Response too long\n";
        return false;
    }
    uint8_t buf[MAX_MSG];
    if (!read_full(sock, buf, len)) return false;

    size_t i = 0;
    while (i < len) {
        uint8_t type = buf[i++];
        if (type == 0) {
            std::cout << "(nil)\n";
        } else if (type == 1) {
            uint32_t code, msg_len;
            memcpy(&code, &buf[i], 4); i += 4;
            memcpy(&msg_len, &buf[i], 4); i += 4;
            std::cout << "Error [" << code << "]: " << std::string((char*)&buf[i], msg_len) << "\n";
            i += msg_len;
        } else if (type == 2) {
            uint32_t slen;
            memcpy(&slen, &buf[i], 4); i += 4;
            std::cout << "\"" << std::string((char*)&buf[i], slen) << "\"\n";
            i += slen;
        } else if (type == 3) {
            int64_t val;
            memcpy(&val, &buf[i], 8); i += 8;
            std::cout << val << "\n";
        } else if (type == 4) {
            double val;
            memcpy(&val, &buf[i], 8); i += 8;
            std::cout << val << "\n";
        } else if (type == 5) {
            uint32_t n;
            memcpy(&n, &buf[i], 4); i += 4;
            std::cout << "[Array of " << (n / 2) << " elements]\n";
        } else {
            std::cerr << "Unknown response type: " << (int)type << "\n";
            break;
        }
    }
    return true;
}

int main() {
    int sock = -1;
    std::string line;
    std::deque<std::string> command_history;

    while (true) {
        if (sock < 0) {
            sock = connect_to_server();
            if (sock < 0) {
                std::cerr << "[Retrying connection in 2s...]\n";
                sleep(2);
                continue;
            }
            set_socket_timeout(sock, 10);  // timeout: 10 seconds
            std::cout << "[Connected to server]\n";
        }

        std::cout << "client> ";
        if (!std::getline(std::cin, line)) break;

        if (line.empty()) continue;
        std::vector<std::string> args;
        size_t start = 0;
        while (start < line.size()) {
            size_t end = line.find_first_of(" \t\r\n", start);
            if (end == std::string::npos) end = line.size();
            if (end > start) args.emplace_back(line.substr(start, end - start));
            start = line.find_first_not_of(" \t\r\n", end);
        }

        if (args.empty()) continue;
        if (args[0] == "quit") break;

        if (args[0] == "hist") {
            std::cout << "Last " << command_history.size() << " commands:\n";
            for (const auto &cmd : command_history)
                std::cout << "  " << cmd << "\n";
            continue;
        }

        std::string command_line;
        for (size_t i = 0; i < args.size(); ++i) {
            if (i > 0) command_line += " ";
            command_line += args[i];
        }

        if (!command_line.empty()) {
            command_history.push_back(command_line);
            if (command_history.size() > 10)
                command_history.pop_front();
        }

        if (!send_request(sock, args) || !receive_response(sock)) {
            std::cerr << "[Lost connection. Reconnecting...]\n";
            close(sock);
            sock = -1;
        }
    }

    if (sock >= 0) close(sock);
    std::cout << "Client exiting.\n";
    return 0;
}
