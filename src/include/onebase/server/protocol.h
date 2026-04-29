#pragma once

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#ifdef SendMessage
#undef SendMessage
#endif
#ifdef ERROR
#undef ERROR
#endif
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace onebase {

#ifdef _WIN32
using socket_t = SOCKET;
#else
using socket_t = int;
#endif

// Default server port
static constexpr uint16_t DEFAULT_SERVER_PORT = 23101;

// Wire protocol message types
enum class MessageType : uint8_t {
  QUERY = 'Q',            // Client -> Server: SQL query string
  RESULT = 'R',           // Server -> Client: result set (tabular text)
  ERROR = 'E',            // Server -> Client: error message
  COMMAND_COMPLETE = 'C', // Server -> Client: DML success message
  TERMINATE = 'X',        // Client -> Server: disconnect
};

// ---- Low-level I/O helpers ------------------------------------------------

// Read exactly `len` bytes into `buf`. Returns false on EOF / error.
inline auto ReadExact(socket_t fd, void *buf, size_t len) -> bool {
  auto *p = static_cast<uint8_t *>(buf);
  size_t remaining = len;
  while (remaining > 0) {
#ifdef _WIN32
    int n = ::recv(fd, reinterpret_cast<char *>(p), static_cast<int>(remaining), 0);
#else
    ssize_t n = ::read(fd, p, remaining);
#endif
    if (n <= 0) {
      return false;
    }
    p += n;
    remaining -= static_cast<size_t>(n);
  }
  return true;
}

// Write exactly `len` bytes from `buf`. Returns false on error.
inline auto WriteExact(socket_t fd, const void *buf, size_t len) -> bool {
  auto *p = static_cast<const uint8_t *>(buf);
  size_t remaining = len;
  while (remaining > 0) {
#ifdef _WIN32
    int n = ::send(fd, reinterpret_cast<const char *>(p), static_cast<int>(remaining), 0);
#else
    ssize_t n = ::write(fd, p, remaining);
#endif
    if (n <= 0) {
      return false;
    }
    p += n;
    remaining -= static_cast<size_t>(n);
  }
  return true;
}

// ---- Message framing ------------------------------------------------------
//
// Each message on the wire:
//   [4 bytes] payload length  (network byte order, includes type byte)
//   [1 byte ] message type
//   [N bytes] data            (payload length - 1)

inline auto SendMessage(socket_t fd, MessageType type, const std::string &data) -> bool {
  uint32_t payload_len = static_cast<uint32_t>(data.size() + 1); // +1 for type byte
  uint32_t net_len = htonl(payload_len);
  if (!WriteExact(fd, &net_len, 4)) {
    return false;
  }
  auto type_byte = static_cast<uint8_t>(type);
  if (!WriteExact(fd, &type_byte, 1)) {
    return false;
  }
  if (!data.empty() && !WriteExact(fd, data.data(), data.size())) {
    return false;
  }
  return true;
}

inline auto RecvMessage(socket_t fd, MessageType *type, std::string *data) -> bool {
  uint32_t net_len = 0;
  if (!ReadExact(fd, &net_len, 4)) {
    return false;
  }
  uint32_t payload_len = ntohl(net_len);
  if (payload_len == 0) {
    return false;
  }
  uint8_t type_byte = 0;
  if (!ReadExact(fd, &type_byte, 1)) {
    return false;
  }
  *type = static_cast<MessageType>(type_byte);
  uint32_t data_len = payload_len - 1;
  data->resize(data_len);
  if (data_len > 0 && !ReadExact(fd, data->data(), data_len)) {
    return false;
  }
  return true;
}

// ---- Result set formatting ------------------------------------------------
//
// Text format used inside RESULT messages:
//   Line 1:     col1|col2|col3          (pipe-separated column names)
//   Lines 2..N: val1|val2|val3          (pipe-separated stringified values)

inline auto FormatResultHeader(const std::vector<std::string> &col_names) -> std::string {
  std::string header;
  for (size_t i = 0; i < col_names.size(); i++) {
    if (i > 0) {
      header += '|';
    }
    header += col_names[i];
  }
  return header;
}

inline auto FormatResultRow(const std::vector<std::string> &values) -> std::string {
  std::string row;
  for (size_t i = 0; i < values.size(); i++) {
    if (i > 0) {
      row += '|';
    }
    row += values[i];
  }
  return row;
}

inline auto ParsePipeSeparated(const std::string &line) -> std::vector<std::string> {
  std::vector<std::string> parts;
  std::string current;
  for (char c : line) {
    if (c == '|') {
      parts.push_back(current);
      current.clear();
    } else {
      current += c;
    }
  }
  parts.push_back(current);
  return parts;
}

}  // namespace onebase
