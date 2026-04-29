#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
#include <signal.h>

#include <atomic>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "onebase/binder/binder.h"
#include "onebase/common/logger.h"
#include "onebase/optimizer/optimizer.h"
#include "onebase/server/onebase_instance.h"
#include "onebase/server/protocol.h"
#include "onebase/storage/table/tuple.h"

static std::atomic<bool> g_running{true};

#ifdef _WIN32
using socket_t = SOCKET;
constexpr socket_t kInvalidSocket = INVALID_SOCKET;
static bool InitSockets() {
  WSADATA wsa{};
  return WSAStartup(MAKEWORD(2, 2), &wsa) == 0;
}
static void CleanupSockets() { WSACleanup(); }
static void CloseSocket(socket_t fd) { closesocket(fd); }
#else
using socket_t = int;
constexpr socket_t kInvalidSocket = -1;
static bool InitSockets() { return true; }
static void CleanupSockets() {}
static void CloseSocket(socket_t fd) { ::close(fd); }
#endif

static socket_t g_listen_fd = kInvalidSocket;

static void SignalHandler(int /*sig*/) {
  g_running.store(false);
  // Unblock accept() by closing the listen socket
  if (g_listen_fd != kInvalidSocket) {
    CloseSocket(g_listen_fd);
    g_listen_fd = kInvalidSocket;
  }
}

// Execute a SQL query against the instance and return a formatted response.
static void HandleQuery(onebase::OneBaseInstance &instance,
                        const std::string &sql, int client_fd) {
  using namespace onebase;
  try {
    Binder binder(instance.GetCatalog());
    auto plan = binder.BindQuery(sql);

    Optimizer optimizer(instance.GetCatalog());
    plan = optimizer.Optimize(plan);

    std::vector<Tuple> result_set;
    instance.GetExecutionEngine()->Execute(plan, &result_set);

    // Build result text
    const auto &schema = plan->GetOutputSchema();
    uint32_t col_count = schema.GetColumnCount();

    // Header line
    std::vector<std::string> col_names;
    col_names.reserve(col_count);
    for (uint32_t i = 0; i < col_count; i++) {
      col_names.push_back(schema.GetColumn(i).GetName());
    }
    std::string body = FormatResultHeader(col_names) + "\n";

    // Data rows
    for (const auto &tuple : result_set) {
      std::vector<std::string> vals;
      vals.reserve(col_count);
      for (uint32_t i = 0; i < col_count; i++) {
        vals.push_back(tuple.GetValue(i).ToString());
      }
      body += FormatResultRow(vals) + "\n";
    }

    // Append row count summary
    body += "(" + std::to_string(result_set.size()) + " row"
            + (result_set.size() != 1 ? "s" : "") + ")";

    SendMessage(client_fd, MessageType::RESULT, body);
  } catch (const std::exception &e) {
    SendMessage(client_fd, MessageType::ERROR, e.what());
  }
}

// Handle a single client connection.
static void HandleClient(onebase::OneBaseInstance &instance,
                         socket_t client_fd, const std::string &client_addr) {
  using namespace onebase;
  LOG_INFO("Client connected: {}", client_addr);

  while (g_running.load()) {
    MessageType type{};
    std::string data;
    if (!RecvMessage(client_fd, &type, &data)) {
      break;  // connection closed or error
    }

    switch (type) {
      case MessageType::QUERY:
        HandleQuery(instance, data, client_fd);
        break;
      case MessageType::TERMINATE:
        goto done;
      default:
        SendMessage(client_fd, MessageType::ERROR, "Unknown message type");
        break;
    }
  }

done:
  LOG_INFO("Client disconnected: {}", client_addr);
  CloseSocket(client_fd);
}

static void PrintUsage(const char *prog) {
  std::cerr << "Usage: " << prog << " [options]\n"
            << "Options:\n"
            << "  -d <db_file>   Database file (default: onebase.db)\n"
            << "  -p <port>      Listen port (default: "
            << onebase::DEFAULT_SERVER_PORT << ")\n"
            << "  -h             Show this help\n";
}

auto main(int argc, char *argv[]) -> int {
  std::string db_file = "onebase.db";
  uint16_t port = onebase::DEFAULT_SERVER_PORT;

#ifdef _WIN32
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-d" && i + 1 < argc) {
      db_file = argv[++i];
    } else if (arg == "-p" && i + 1 < argc) {
      port = static_cast<uint16_t>(std::stoi(argv[++i]));
    } else if (arg == "-h") {
      PrintUsage(argv[0]);
      return 0;
    } else {
      PrintUsage(argv[0]);
      return 1;
    }
  }
#else
  int opt;
  while ((opt = getopt(argc, argv, "d:p:h")) != -1) {
    switch (opt) {
      case 'd':
        db_file = optarg;
        break;
      case 'p':
        port = static_cast<uint16_t>(std::stoi(optarg));
        break;
      case 'h':
      default:
        PrintUsage(argv[0]);
        return (opt == 'h') ? 0 : 1;
    }
  }
#endif

  // Install signal handlers for graceful shutdown
  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);
#ifndef _WIN32
  signal(SIGPIPE, SIG_IGN);  // ignore broken pipe
#endif

  if (!InitSockets()) {
    std::cerr << "Failed to initialize sockets." << std::endl;
    return 1;
  }

  std::cout << "OneBase Server starting..." << std::endl;
  std::cout << "Database file: " << db_file << std::endl;

  onebase::OneBaseInstance instance(db_file);

  std::cout << "Buffer pool size: "
            << instance.GetBufferPoolManager()->GetPoolSize() << " pages" << std::endl;

  // Create TCP listen socket
  g_listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (g_listen_fd == kInvalidSocket) {
    perror("socket");
    CleanupSockets();
    return 1;
  }

  int reuse = 1;
  setsockopt(g_listen_fd, SOL_SOCKET, SO_REUSEADDR,
#ifdef _WIN32
             reinterpret_cast<const char *>(&reuse),
#else
             &reuse,
#endif
             sizeof(reuse));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);

  if (::bind(g_listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
    perror("bind");
    CloseSocket(g_listen_fd);
    CleanupSockets();
    return 1;
  }

  if (::listen(g_listen_fd, 16) < 0) {
    perror("listen");
    CloseSocket(g_listen_fd);
    CleanupSockets();
    return 1;
  }

  std::cout << "Listening on port " << port << std::endl;
  std::cout << "Press Ctrl+C to stop." << std::endl;

  // Accept loop -- one thread per client
  std::vector<std::thread> workers;
  while (g_running.load()) {
    sockaddr_in client_addr{};
#ifdef _WIN32
    int client_len = sizeof(client_addr);
#else
    socklen_t client_len = sizeof(client_addr);
#endif
    socket_t client_fd = ::accept(g_listen_fd,
                                  reinterpret_cast<sockaddr *>(&client_addr),
                                  &client_len);
    if (client_fd == kInvalidSocket) {
      if (!g_running.load()) {
        break;  // shutdown requested
      }
      perror("accept");
      continue;
    }

    char ip_buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, ip_buf, sizeof(ip_buf));
    std::string client_str = std::string(ip_buf) + ":"
                             + std::to_string(ntohs(client_addr.sin_port));

    workers.emplace_back(HandleClient, std::ref(instance), client_fd, client_str);
  }

  // Wait for all client threads to finish
  for (auto &t : workers) {
    if (t.joinable()) {
      t.join();
    }
  }

  if (g_listen_fd != kInvalidSocket) {
    CloseSocket(g_listen_fd);
  }

  CleanupSockets();

  std::cout << "\nOneBase Server stopped." << std::endl;
  return 0;
}
