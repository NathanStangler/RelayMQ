#include "relaymq/server.hpp"
#include "relaymq/client.hpp"
#include "relaymq/net.hpp"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

TEST(AuthTest, AllowUnauthenticated) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    ASSERT_TRUE(server.start(config));

    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", server.port()));

    std::mutex cv_mutex;
    std::condition_variable cv;
    bool received = false;
    std::string received_payload;

    client.subscribe("service.auth.open", [&](const relaymq::Message& msg) {
        std::unique_lock<std::mutex> lock(cv_mutex);
        received = true;
        received_payload = msg.payload;
        lock.unlock();
        cv.notify_one();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    client.publish("service.auth.open", "start");

    std::unique_lock<std::mutex> lock(cv_mutex);
    bool ok = cv.wait_for(lock, std::chrono::seconds(2), [&]() {
        return received;
    });

    EXPECT_TRUE(ok);
    EXPECT_EQ(received_payload, "start");

    client.disconnect();
    server.stop();
}

TEST(AuthTest, TokenAuthSuccess) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    config.auth_token = "valid_secret_token";
    ASSERT_TRUE(server.start(config));

    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", server.port(), "valid_secret_token"));

    std::mutex cv_mutex;
    std::condition_variable cv;
    bool received = false;
    std::string received_payload;

    client.subscribe("service.auth.secured", [&](const relaymq::Message& msg) {
        std::unique_lock<std::mutex> lock(cv_mutex);
        received = true;
        received_payload = msg.payload;
        lock.unlock();
        cv.notify_one();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    client.publish("service.auth.secured", "secure");

    std::unique_lock<std::mutex> lock(cv_mutex);
    bool ok = cv.wait_for(lock, std::chrono::seconds(2), [&]() {
        return received;
    });

    EXPECT_TRUE(ok);
    EXPECT_EQ(received_payload, "secure");

    client.disconnect();
    server.stop();
}

TEST(AuthTest, TokenAuthInvalidRejected) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    config.auth_token = "correct_token";
    ASSERT_TRUE(server.start(config));

    relaymq::Client client;
    bool connected = client.connect("127.0.0.1", server.port(), "wrong_token");
    EXPECT_FALSE(connected);
    EXPECT_FALSE(client.is_connected());

    server.stop();
}

TEST(AuthTest, UnauthenticatedCommandRejected) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    config.auth_token = "required_token";
    ASSERT_TRUE(server.start(config));

    relaymq::Client client;
    bool connected = client.connect("127.0.0.1", server.port(), "");
    EXPECT_TRUE(connected);

    std::mutex cv_mutex;
    std::condition_variable cv;
    std::atomic<bool> disconnected{false};

    client.set_disconnect_callback([&]() {
        disconnected = true;
        std::unique_lock<std::mutex> lock(cv_mutex);
        cv.notify_one();
    });

    client.publish("unauthorized.topic", "data");

    std::unique_lock<std::mutex> lock(cv_mutex);
    bool ok = cv.wait_for(lock, std::chrono::seconds(2), [&]() {
        return disconnected.load();
    });

    EXPECT_TRUE(ok);
    EXPECT_FALSE(client.is_connected());

    client.disconnect();
    server.stop();
}

TEST(AuthTest, ClusterPeeringAuthSuccess) {
    relaymq::Server server_1;
    relaymq::ServerConfig c1;
    c1.port = 0;
    c1.enable_clustering = true;
    c1.cluster_port = 0;
    c1.cluster_name = "auth_cluster";
    c1.node_id = "node_1";
    c1.cluster_token = "cluster_secret";
    ASSERT_TRUE(server_1.start(c1));

    relaymq::Server server_2;
    relaymq::ServerConfig c2;
    c2.port = 0;
    c2.enable_clustering = true;
    c2.cluster_port = 0;
    c2.cluster_name = "auth_cluster";
    c2.node_id = "node_2";
    c2.cluster_token = "cluster_secret";
    ASSERT_TRUE(server_2.start(c2));

    ASSERT_TRUE(server_1.connect_to_peer("127.0.0.1", server_2.cluster_port()));

    for (int i = 0; i < 50; i++) {
        if (server_1.connected_peers() == 1 && server_2.connected_peers() == 1) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_EQ(server_1.connected_peers(), 1);
    EXPECT_EQ(server_2.connected_peers(), 1);

    server_1.stop();
    server_2.stop();
}

TEST(AuthTest, ClusterPeeringAuthMismatchRejected) {
    relaymq::Server server_1;
    relaymq::ServerConfig c1;
    c1.port = 0;
    c1.enable_clustering = true;
    c1.cluster_port = 0;
    c1.cluster_name = "auth_cluster";
    c1.node_id = "node_1";
    c1.cluster_token = "secret_a";
    ASSERT_TRUE(server_1.start(c1));

    relaymq::Server server_2;
    relaymq::ServerConfig c2;
    c2.port = 0;
    c2.enable_clustering = true;
    c2.cluster_port = 0;
    c2.cluster_name = "auth_cluster";
    c2.node_id = "node_2";
    c2.cluster_token = "secret_b";
    ASSERT_TRUE(server_2.start(c2));

    ASSERT_TRUE(server_1.connect_to_peer("127.0.0.1", server_2.cluster_port()));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_EQ(server_2.connected_peers(), 0);

    server_1.stop();
    server_2.stop();
}

TEST(AuthTest, AuthTimeoutDisconnect) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    config.auth_token = "secret_token";
    config.auth_timeout_ms = 100;
    ASSERT_TRUE(server.start(config));

    int raw_fd = relaymq::net::connect_tcp("127.0.0.1", server.port(), false);
    ASSERT_GE(raw_fd, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    char buf[64];
    ssize_t bytes = recv(raw_fd, buf, sizeof(buf), MSG_DONTWAIT);
    EXPECT_LE(bytes, 0);

    relaymq::net::close_socket(raw_fd);
    server.stop();
}
