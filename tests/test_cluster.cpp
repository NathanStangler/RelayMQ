#include "relaymq/server.hpp"
#include "relaymq/client.hpp"
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <algorithm>

class ClusterTest : public testing::Test {
protected:
    void SetUp() override {
        relaymq::ServerConfig config1;
        config1.port = 0;
        config1.enable_clustering = true;
        config1.cluster_port = 0;
        config1.cluster_name = "cluster_1";
        config1.node_id = "node_1";

        ASSERT_TRUE(server_1_.start(config1));
        ASSERT_GT(server_1_.port(), 0);
        ASSERT_GT(server_1_.cluster_port(), 0);

        relaymq::ServerConfig config2;
        config2.port = 0;
        config2.enable_clustering = true;
        config2.cluster_port = 0;
        config2.cluster_name = "cluster_1";
        config2.node_id = "node_2";

        ASSERT_TRUE(server_2_.start(config2));
        ASSERT_GT(server_2_.port(), 0);
        ASSERT_GT(server_2_.cluster_port(), 0);
    }

    void TearDown() override {
        server_1_.stop();
        server_2_.stop();
    }

    relaymq::Server server_1_;
    relaymq::Server server_2_;
};

TEST_F(ClusterTest, PeerHandshake) {
    ASSERT_EQ(server_1_.connected_peers(), 0);
    ASSERT_EQ(server_2_.connected_peers(), 0);

    ASSERT_TRUE(server_1_.connect_to_peer("127.0.0.1", server_2_.cluster_port()));

    bool handshake_done = false;
    for (int i = 0; i < 50; i++) {
        if (server_1_.connected_peers() == 1 && server_2_.connected_peers() == 1) {
            handshake_done = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_TRUE(handshake_done);

    std::vector<std::string> peers_1 = server_1_.peer_ids();
    ASSERT_EQ(peers_1.size(), 1);
    EXPECT_EQ(peers_1[0], "node_2");

    std::vector<std::string> peers_2 = server_2_.peer_ids();
    ASSERT_EQ(peers_2.size(), 1);
    EXPECT_EQ(peers_2[0], "node_1");
}

TEST_F(ClusterTest, PeerPingPong) {
    ASSERT_TRUE(server_1_.connect_to_peer("127.0.0.1", server_2_.cluster_port()));

    bool connected = false;
    for (int i = 0; i < 50; i++) {
        if (server_1_.connected_peers() == 1 && server_2_.connected_peers() == 1) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(connected);

    EXPECT_TRUE(server_1_.send_to_peer("node_2", relaymq::ProtocolParser::format_rping()));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    EXPECT_EQ(server_1_.connected_peers(), 1);
    EXPECT_EQ(server_2_.connected_peers(), 1);
}

TEST_F(ClusterTest, ClusterMismatch) {
    relaymq::ServerConfig config3;
    config3.port = 0;
    config3.enable_clustering = true;
    config3.cluster_port = 0;
    config3.cluster_name = "cluster_2";
    config3.node_id = "node_3";

    relaymq::Server server_3;
    ASSERT_TRUE(server_3.start(config3));

    ASSERT_TRUE(server_3.connect_to_peer("127.0.0.1", server_1_.cluster_port()));

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_EQ(server_1_.connected_peers(), 0);
    EXPECT_EQ(server_3.connected_peers(), 0);

    server_3.stop();
}

TEST_F(ClusterTest, LocalSubscriptionGossip) {
    ASSERT_TRUE(server_1_.connect_to_peer("127.0.0.1", server_2_.cluster_port()));

    bool connected = false;
    for (int i = 0; i < 50; i++) {
        if (server_1_.connected_peers() == 1 && server_2_.connected_peers() == 1) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(connected);

    relaymq::Client client_1;
    ASSERT_TRUE(client_1.connect("127.0.0.1", server_1_.port()));

    std::string sid_1 = client_1.subscribe("service.data.update", [](const relaymq::Message&) {});
    ASSERT_FALSE(sid_1.empty());
    ASSERT_GE(client_1.ping(), 0);

    EXPECT_EQ(server_1_.local_subject_count("service.data.update"), 1);

    bool synced = false;
    for (int i = 0; i < 50; i++) {
        if (server_2_.total_remote_subscriptions() == 1) {
            synced = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(synced);

    std::vector<std::string> matches = server_2_.match_peers("service.data.update");
    ASSERT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0], "node_1");

    std::vector<std::string> node1_subs = server_2_.peer_subscriptions("node_1");
    ASSERT_EQ(node1_subs.size(), 1);
    EXPECT_EQ(node1_subs[0], "service.data.update");

    relaymq::Client client_2;
    ASSERT_TRUE(client_2.connect("127.0.0.1", server_1_.port()));
    std::string sid_2 = client_2.subscribe("service.data.update", [](const relaymq::Message&) {});
    ASSERT_FALSE(sid_2.empty());
    ASSERT_GE(client_2.ping(), 0);

    EXPECT_EQ(server_1_.local_subject_count("service.data.update"), 2);
    EXPECT_EQ(server_2_.total_remote_subscriptions(), 1);

    EXPECT_TRUE(client_1.unsubscribe(sid_1));
    ASSERT_GE(client_1.ping(), 0);
    EXPECT_EQ(server_1_.local_subject_count("service.data.update"), 1);
    EXPECT_EQ(server_2_.total_remote_subscriptions(), 1);

    EXPECT_TRUE(client_2.unsubscribe(sid_2));
    ASSERT_GE(client_2.ping(), 0);
    bool cleaned = false;
    for (int i = 0; i < 50; i++) {
        if (server_2_.total_remote_subscriptions() == 0) {
            cleaned = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(cleaned);
    EXPECT_EQ(server_1_.local_subject_count("service.data.update"), 0);
    EXPECT_EQ(server_2_.match_peers("service.data.update").size(), 0);

    client_1.disconnect();
    client_2.disconnect();
}

TEST_F(ClusterTest, WildcardSubscriptionGossip) {
    ASSERT_TRUE(server_1_.connect_to_peer("127.0.0.1", server_2_.cluster_port()));

    bool connected = false;
    for (int i = 0; i < 50; i++) {
        if (server_1_.connected_peers() == 1 && server_2_.connected_peers() == 1) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(connected);

    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", server_1_.port()));

    client.subscribe("service.events.*", [](const relaymq::Message&) {});
    client.subscribe("service.audit.>", [](const relaymq::Message&) {});
    ASSERT_GE(client.ping(), 0);

    bool synced = false;
    for (int i = 0; i < 50; i++) {
        if (server_2_.total_remote_subscriptions() == 2) {
            synced = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(synced);

    std::vector<std::string> m1 = server_2_.match_peers("service.events.login");
    ASSERT_EQ(m1.size(), 1);
    EXPECT_EQ(m1[0], "node_1");

    std::vector<std::string> m2 = server_2_.match_peers("service.audit.user.create.v2");
    ASSERT_EQ(m2.size(), 1);
    EXPECT_EQ(m2[0], "node_1");

    std::vector<std::string> m3 = server_2_.match_peers("service.other.topic");
    EXPECT_EQ(m3.size(), 0);

    client.disconnect();
}

TEST_F(ClusterTest, InitialSubscriptionSync) {
    relaymq::ServerConfig config3;
    config3.port = 0;
    config3.enable_clustering = true;
    config3.cluster_port = 0;
    config3.cluster_name = "cluster_1";
    config3.node_id = "node_3";

    relaymq::Server server_3;
    ASSERT_TRUE(server_3.start(config3));

    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", server_3.port()));
    client.subscribe("service.orders.create", [](const relaymq::Message&) {});
    client.subscribe("service.payments.process", [](const relaymq::Message&) {});
    ASSERT_GE(client.ping(), 0);

    EXPECT_EQ(server_3.active_subjects().size(), 2);

    ASSERT_TRUE(server_3.connect_to_peer("127.0.0.1", server_1_.cluster_port()));

    bool synced = false;
    for (int i = 0; i < 50; i++) {
        if (server_1_.total_remote_subscriptions() == 2) {
            synced = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(synced);

    std::vector<std::string> m1 = server_1_.match_peers("service.orders.create");
    ASSERT_EQ(m1.size(), 1);
    EXPECT_EQ(m1[0], "node_3");

    std::vector<std::string> m2 = server_1_.match_peers("service.payments.process");
    ASSERT_EQ(m2.size(), 1);
    EXPECT_EQ(m2[0], "node_3");

    client.disconnect();
    server_3.stop();
}

TEST_F(ClusterTest, ClientDisconnectUnsubGossip) {
    ASSERT_TRUE(server_1_.connect_to_peer("127.0.0.1", server_2_.cluster_port()));

    bool connected = false;
    for (int i = 0; i < 50; i++) {
        if (server_1_.connected_peers() == 1 && server_2_.connected_peers() == 1) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(connected);

    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", server_1_.port()));
    client.subscribe("service.session.state", [](const relaymq::Message&) {});
    ASSERT_GE(client.ping(), 0);

    bool synced = false;
    for (int i = 0; i < 50; i++) {
        if (server_2_.total_remote_subscriptions() == 1) {
            synced = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(synced);
    EXPECT_EQ(server_1_.local_subject_count("service.session.state"), 1);

    client.disconnect();

    bool cleaned = false;
    for (int i = 0; i < 50; i++) {
        if (server_2_.total_remote_subscriptions() == 0) {
            cleaned = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(cleaned);
    EXPECT_EQ(server_1_.local_subject_count("service.session.state"), 0);
}

TEST_F(ClusterTest, PeerDisconnectSubscriptionCleanup) {
    ASSERT_TRUE(server_1_.connect_to_peer("127.0.0.1", server_2_.cluster_port()));

    bool connected = false;
    for (int i = 0; i < 50; i++) {
        if (server_1_.connected_peers() == 1 && server_2_.connected_peers() == 1) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(connected);

    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", server_2_.port()));
    client.subscribe("service.cache.invalidate", [](const relaymq::Message&) {});
    ASSERT_GE(client.ping(), 0);

    bool synced = false;
    for (int i = 0; i < 50; i++) {
        if (server_1_.total_remote_subscriptions() == 1) {
            synced = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(synced);
    EXPECT_EQ(server_1_.match_peers("service.cache.invalidate").size(), 1);

    server_2_.stop();

    bool purged = false;
    for (int i = 0; i < 50; i++) {
        if (server_1_.total_remote_subscriptions() == 0 && server_1_.connected_peers() == 0) {
            purged = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(purged);
    EXPECT_EQ(server_1_.match_peers("service.cache.invalidate").size(), 0);

    client.disconnect();
}
