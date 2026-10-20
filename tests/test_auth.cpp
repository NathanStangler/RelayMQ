#include "relaymq/server.hpp"
#include "test_socket.hpp"
#include <gtest/gtest.h>
#include <chrono>
#include <thread>

TEST(AuthTest, AllowUnauthenticated) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    ASSERT_TRUE(server.start(config));

    relaymq::test::TestSocket sub;
    ASSERT_TRUE(sub.connect(server.port()));
    ASSERT_TRUE(sub.send("SUB service.auth.open 1\r\n"));
    EXPECT_EQ(sub.read_line(), "OK\r\n");

    relaymq::test::TestSocket pub;
    ASSERT_TRUE(pub.connect(server.port()));
    ASSERT_TRUE(pub.send("PUB service.auth.open 5\r\nstart\r\n"));

    EXPECT_EQ(sub.read_line(), "MSG service.auth.open 1 5\r\n");
    EXPECT_EQ(sub.read_line(), "start\r\n");

    sub.close();
    pub.close();
    server.stop();
}

TEST(AuthTest, TokenAuthSuccess) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    config.auth_token = "valid_secret_token";
    ASSERT_TRUE(server.start(config));

    relaymq::test::TestSocket sub;
    ASSERT_TRUE(sub.connect(server.port()));
    ASSERT_TRUE(sub.send("AUTH valid_secret_token\r\n"));
    EXPECT_EQ(sub.read_line(), "OK\r\n");
    ASSERT_TRUE(sub.send("SUB service.auth.secured 1\r\n"));
    EXPECT_EQ(sub.read_line(), "OK\r\n");

    relaymq::test::TestSocket pub;
    ASSERT_TRUE(pub.connect(server.port()));
    ASSERT_TRUE(pub.send("AUTH valid_secret_token\r\n"));
    EXPECT_EQ(pub.read_line(), "OK\r\n");
    ASSERT_TRUE(pub.send("PUB service.auth.secured 6\r\nsecure\r\n"));

    EXPECT_EQ(sub.read_line(), "MSG service.auth.secured 1 6\r\n");
    EXPECT_EQ(sub.read_line(), "secure\r\n");

    sub.close();
    pub.close();
    server.stop();
}

TEST(AuthTest, TokenAuthInvalidRejected) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    config.auth_token = "correct_token";
    ASSERT_TRUE(server.start(config));

    relaymq::test::TestSocket conn;
    ASSERT_TRUE(conn.connect(server.port()));
    ASSERT_TRUE(conn.send("AUTH wrong_token\r\n"));

    std::string err = conn.read_line();
    EXPECT_TRUE(err.starts_with("ERR "));
    EXPECT_TRUE(conn.read_line(100).empty());

    conn.close();
    server.stop();
}

TEST(AuthTest, UnauthenticatedCommandRejected) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    config.auth_token = "required_token";
    ASSERT_TRUE(server.start(config));

    relaymq::test::TestSocket conn;
    ASSERT_TRUE(conn.connect(server.port()));
    ASSERT_TRUE(conn.send("PUB unauthorized.topic 4\r\ndata\r\n"));

    std::string err = conn.read_line();
    EXPECT_TRUE(err.starts_with("ERR "));
    EXPECT_TRUE(conn.read_line(100).empty());

    conn.close();
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

    relaymq::test::TestSocket conn;
    ASSERT_TRUE(conn.connect(server.port()));

    std::this_thread::sleep_for(std::chrono::milliseconds(700));

    EXPECT_TRUE(conn.read_line(100).empty());

    conn.close();
    server.stop();
}
