#include "relaymq/server.hpp"
#include <gtest/gtest.h>
#include <chrono>
#include <thread>

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
