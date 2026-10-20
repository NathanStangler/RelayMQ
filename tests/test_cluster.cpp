#include "relaymq/server.hpp"
#include "test_socket.hpp"
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <vector>
#include <string>

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

        relaymq::ServerConfig config2;
        config2.port = 0;
        config2.enable_clustering = true;
        config2.cluster_port = 0;
        config2.cluster_name = "cluster_1";
        config2.node_id = "node_2";
        ASSERT_TRUE(server_2_.start(config2));
    }

    void TearDown() override {
        server_1_.stop();
        server_2_.stop();
    }

    bool connect_nodes() {
        if (!server_1_.connect_to_peer("127.0.0.1", server_2_.cluster_port())) {
            return false;
        }
        return wait_peers(server_1_, 1) && wait_peers(server_2_, 1);
    }

    static bool wait_peers(relaymq::Server& server, size_t expected, int timeout_ms = 1000) {
        int steps = timeout_ms / 10;
        for (int i = 0; i < steps; i++) {
            if (server.connected_peers() == expected) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return server.connected_peers() == expected;
    }

    static bool wait_remote_subs(relaymq::Server& server, size_t expected, int timeout_ms = 1000) {
        int steps = timeout_ms / 10;
        for (int i = 0; i < steps; i++) {
            if (server.total_remote_subscriptions() == expected) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return server.total_remote_subscriptions() == expected;
    }

    relaymq::Server server_1_;
    relaymq::Server server_2_;
};

TEST_F(ClusterTest, PeerHandshake) {
    ASSERT_EQ(server_1_.connected_peers(), 0);
    ASSERT_EQ(server_2_.connected_peers(), 0);

    ASSERT_TRUE(connect_nodes());

    std::vector<std::string> peers_1 = server_1_.peer_ids();
    ASSERT_EQ(peers_1.size(), 1);
    EXPECT_EQ(peers_1[0], "node_2");

    std::vector<std::string> peers_2 = server_2_.peer_ids();
    ASSERT_EQ(peers_2.size(), 1);
    EXPECT_EQ(peers_2[0], "node_1");
}

TEST_F(ClusterTest, PeerPingPong) {
    ASSERT_TRUE(connect_nodes());

    EXPECT_TRUE(server_1_.send_to_peer("node_2", "RPING\r\n"));
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
    ASSERT_TRUE(connect_nodes());

    relaymq::test::TestSocket client_1;
    ASSERT_TRUE(client_1.connect(server_1_.port()));
    ASSERT_TRUE(client_1.send("SUB service.data.update 101\r\n"));
    EXPECT_EQ(client_1.read_line(), "OK\r\n");

    EXPECT_EQ(server_1_.local_subject_count("service.data.update"), 1);
    ASSERT_TRUE(wait_remote_subs(server_2_, 1));

    std::vector<std::string> matches = server_2_.match_peers("service.data.update");
    ASSERT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0], "node_1");

    std::vector<std::string> node1_subs = server_2_.peer_subscriptions("node_1");
    ASSERT_EQ(node1_subs.size(), 1);
    EXPECT_EQ(node1_subs[0], "service.data.update");

    relaymq::test::TestSocket client_2;
    ASSERT_TRUE(client_2.connect(server_1_.port()));
    ASSERT_TRUE(client_2.send("SUB service.data.update 102\r\n"));
    EXPECT_EQ(client_2.read_line(), "OK\r\n");

    EXPECT_EQ(server_1_.local_subject_count("service.data.update"), 2);
    EXPECT_EQ(server_2_.total_remote_subscriptions(), 1);

    ASSERT_TRUE(client_1.send("UNSUB 101\r\n"));
    EXPECT_EQ(client_1.read_line(), "OK\r\n");
    EXPECT_EQ(server_1_.local_subject_count("service.data.update"), 1);
    EXPECT_EQ(server_2_.total_remote_subscriptions(), 1);

    ASSERT_TRUE(client_2.send("UNSUB 102\r\n"));
    EXPECT_EQ(client_2.read_line(), "OK\r\n");
    ASSERT_TRUE(wait_remote_subs(server_2_, 0));
    EXPECT_EQ(server_1_.local_subject_count("service.data.update"), 0);
    EXPECT_EQ(server_2_.match_peers("service.data.update").size(), 0);

    client_1.close();
    client_2.close();
}

TEST_F(ClusterTest, WildcardSubscriptionGossip) {
    ASSERT_TRUE(connect_nodes());

    relaymq::test::TestSocket client;
    ASSERT_TRUE(client.connect(server_1_.port()));
    ASSERT_TRUE(client.send("SUB service.events.* 1\r\n"));
    EXPECT_EQ(client.read_line(), "OK\r\n");
    ASSERT_TRUE(client.send("SUB service.audit.> 2\r\n"));
    EXPECT_EQ(client.read_line(), "OK\r\n");

    ASSERT_TRUE(wait_remote_subs(server_2_, 2));

    std::vector<std::string> m1 = server_2_.match_peers("service.events.login");
    ASSERT_EQ(m1.size(), 1);
    EXPECT_EQ(m1[0], "node_1");

    std::vector<std::string> m2 = server_2_.match_peers("service.audit.user.create.v2");
    ASSERT_EQ(m2.size(), 1);
    EXPECT_EQ(m2[0], "node_1");

    std::vector<std::string> m3 = server_2_.match_peers("service.other.topic");
    EXPECT_EQ(m3.size(), 0);

    client.close();
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

    relaymq::test::TestSocket client;
    ASSERT_TRUE(client.connect(server_3.port()));
    ASSERT_TRUE(client.send("SUB service.orders.create 1\r\n"));
    EXPECT_EQ(client.read_line(), "OK\r\n");
    ASSERT_TRUE(client.send("SUB service.payments.process 2\r\n"));
    EXPECT_EQ(client.read_line(), "OK\r\n");

    EXPECT_EQ(server_3.active_subjects().size(), 2);

    ASSERT_TRUE(server_3.connect_to_peer("127.0.0.1", server_1_.cluster_port()));
    ASSERT_TRUE(wait_remote_subs(server_1_, 2));

    std::vector<std::string> m1 = server_1_.match_peers("service.orders.create");
    ASSERT_EQ(m1.size(), 1);
    EXPECT_EQ(m1[0], "node_3");

    std::vector<std::string> m2 = server_1_.match_peers("service.payments.process");
    ASSERT_EQ(m2.size(), 1);
    EXPECT_EQ(m2[0], "node_3");

    client.close();
    server_3.stop();
}

TEST_F(ClusterTest, ClientDisconnectUnsubGossip) {
    ASSERT_TRUE(connect_nodes());

    relaymq::test::TestSocket client;
    ASSERT_TRUE(client.connect(server_1_.port()));
    ASSERT_TRUE(client.send("SUB service.session.state 1\r\n"));
    EXPECT_EQ(client.read_line(), "OK\r\n");

    ASSERT_TRUE(wait_remote_subs(server_2_, 1));
    EXPECT_EQ(server_1_.local_subject_count("service.session.state"), 1);

    client.close();

    ASSERT_TRUE(wait_remote_subs(server_2_, 0));
    EXPECT_EQ(server_1_.local_subject_count("service.session.state"), 0);
}

TEST_F(ClusterTest, PeerDisconnectSubscriptionCleanup) {
    ASSERT_TRUE(connect_nodes());

    relaymq::test::TestSocket client;
    ASSERT_TRUE(client.connect(server_2_.port()));
    ASSERT_TRUE(client.send("SUB service.cache.invalidate 1\r\n"));
    EXPECT_EQ(client.read_line(), "OK\r\n");

    ASSERT_TRUE(wait_remote_subs(server_1_, 1));
    EXPECT_EQ(server_1_.match_peers("service.cache.invalidate").size(), 1);

    server_2_.stop();

    ASSERT_TRUE(wait_peers(server_1_, 0));
    ASSERT_TRUE(wait_remote_subs(server_1_, 0));
    EXPECT_EQ(server_1_.match_peers("service.cache.invalidate").size(), 0);

    client.close();
}

TEST_F(ClusterTest, CrossClusterPublishSubscribe) {
    ASSERT_TRUE(connect_nodes());

    relaymq::test::TestSocket subscriber;
    ASSERT_TRUE(subscriber.connect(server_2_.port()));
    ASSERT_TRUE(subscriber.send("SUB service.orders.create 1\r\n"));
    EXPECT_EQ(subscriber.read_line(), "OK\r\n");

    ASSERT_TRUE(wait_remote_subs(server_1_, 1));

    relaymq::test::TestSocket publisher;
    ASSERT_TRUE(publisher.connect(server_1_.port()));
    ASSERT_TRUE(publisher.send("PUB service.orders.create 12\r\norder_id=101\r\n"));

    EXPECT_EQ(subscriber.read_line(), "MSG service.orders.create 1 12\r\n");
    EXPECT_EQ(subscriber.read_line(), "order_id=101\r\n");

    subscriber.close();
    publisher.close();
}

TEST_F(ClusterTest, CrossClusterPublishSubscribeWildcard) {
    ASSERT_TRUE(connect_nodes());

    relaymq::test::TestSocket sub1;
    ASSERT_TRUE(sub1.connect(server_1_.port()));
    ASSERT_TRUE(sub1.send("SUB service.events.* 1\r\n"));
    EXPECT_EQ(sub1.read_line(), "OK\r\n");

    relaymq::test::TestSocket sub2;
    ASSERT_TRUE(sub2.connect(server_2_.port()));
    ASSERT_TRUE(sub2.send("SUB service.audit.> 2\r\n"));
    EXPECT_EQ(sub2.read_line(), "OK\r\n");

    ASSERT_TRUE(wait_remote_subs(server_2_, 1));
    ASSERT_TRUE(wait_remote_subs(server_1_, 1));

    relaymq::test::TestSocket publisher;
    ASSERT_TRUE(publisher.connect(server_2_.port()));

    ASSERT_TRUE(publisher.send("PUB service.events.login 6\r\nuser_1\r\n"));
    EXPECT_EQ(sub1.read_line(), "MSG service.events.login 1 6\r\n");
    EXPECT_EQ(sub1.read_line(), "user_1\r\n");

    ASSERT_TRUE(publisher.send("PUB service.audit.user.create.v2 9\r\nadmin_log\r\n"));
    EXPECT_EQ(sub2.read_line(), "MSG service.audit.user.create.v2 2 9\r\n");
    EXPECT_EQ(sub2.read_line(), "admin_log\r\n");

    sub1.close();
    sub2.close();
    publisher.close();
}

TEST_F(ClusterTest, LoopPrevention) {
    relaymq::ServerConfig config3;
    config3.port = 0;
    config3.enable_clustering = true;
    config3.cluster_port = 0;
    config3.cluster_name = "cluster_1";
    config3.node_id = "node_3";

    relaymq::Server server_3;
    ASSERT_TRUE(server_3.start(config3));

    ASSERT_TRUE(server_1_.connect_to_peer("127.0.0.1", server_2_.cluster_port()));
    ASSERT_TRUE(server_2_.connect_to_peer("127.0.0.1", server_3.cluster_port()));
    ASSERT_TRUE(server_1_.connect_to_peer("127.0.0.1", server_3.cluster_port()));

    ASSERT_TRUE(wait_peers(server_1_, 2));
    ASSERT_TRUE(wait_peers(server_2_, 2));
    ASSERT_TRUE(wait_peers(server_3, 2));

    relaymq::test::TestSocket client_1;
    ASSERT_TRUE(client_1.connect(server_1_.port()));
    ASSERT_TRUE(client_1.send("SUB service.broadcast.test 1\r\n"));
    EXPECT_EQ(client_1.read_line(), "OK\r\n");

    relaymq::test::TestSocket client_2;
    ASSERT_TRUE(client_2.connect(server_2_.port()));
    ASSERT_TRUE(client_2.send("SUB service.broadcast.test 2\r\n"));
    EXPECT_EQ(client_2.read_line(), "OK\r\n");

    relaymq::test::TestSocket client_3;
    ASSERT_TRUE(client_3.connect(server_3.port()));
    ASSERT_TRUE(client_3.send("SUB service.broadcast.test 3\r\n"));
    EXPECT_EQ(client_3.read_line(), "OK\r\n");

    ASSERT_TRUE(wait_remote_subs(server_1_, 2));
    ASSERT_TRUE(wait_remote_subs(server_2_, 2));
    ASSERT_TRUE(wait_remote_subs(server_3, 2));

    ASSERT_TRUE(client_1.send("PUB service.broadcast.test 4\r\nping\r\n"));

    EXPECT_EQ(client_1.read_line(), "MSG service.broadcast.test 1 4\r\n");
    EXPECT_EQ(client_1.read_line(), "ping\r\n");

    EXPECT_EQ(client_2.read_line(), "MSG service.broadcast.test 2 4\r\n");
    EXPECT_EQ(client_2.read_line(), "ping\r\n");

    EXPECT_EQ(client_3.read_line(), "MSG service.broadcast.test 3 4\r\n");
    EXPECT_EQ(client_3.read_line(), "ping\r\n");

    EXPECT_TRUE(client_1.read_line(50).empty());
    EXPECT_TRUE(client_2.read_line(50).empty());
    EXPECT_TRUE(client_3.read_line(50).empty());

    client_1.close();
    client_2.close();
    client_3.close();
    server_3.stop();
}

TEST_F(ClusterTest, CrossClusterRequestReply) {
    ASSERT_TRUE(connect_nodes());

    relaymq::test::TestSocket worker;
    ASSERT_TRUE(worker.connect(server_2_.port()));
    ASSERT_TRUE(worker.send("SUB service.math.double 1\r\n"));
    EXPECT_EQ(worker.read_line(), "OK\r\n");

    ASSERT_TRUE(wait_remote_subs(server_1_, 1));

    relaymq::test::TestSocket requester;
    ASSERT_TRUE(requester.connect(server_1_.port()));
    ASSERT_TRUE(requester.send("SUB subject_1 2\r\n"));
    EXPECT_EQ(requester.read_line(), "OK\r\n");

    ASSERT_TRUE(wait_remote_subs(server_2_, 1));

    ASSERT_TRUE(requester.send("PUB service.math.double subject_1 2\r\n20\r\n"));

    EXPECT_EQ(worker.read_line(), "MSG service.math.double 1 subject_1 2\r\n");
    EXPECT_EQ(worker.read_line(), "20\r\n");

    ASSERT_TRUE(worker.send("PUB subject_1 2\r\n40\r\n"));

    EXPECT_EQ(requester.read_line(), "MSG subject_1 2 2\r\n");
    EXPECT_EQ(requester.read_line(), "40\r\n");

    worker.close();
    requester.close();
}

TEST_F(ClusterTest, HeartbeatTimeoutDisconnect) {
    relaymq::ServerConfig config3;
    config3.port = 0;
    config3.enable_clustering = true;
    config3.cluster_port = 0;
    config3.cluster_name = "cluster_1";
    config3.node_id = "node_3";
    config3.heartbeat_interval_ms = 30;
    config3.max_missed_heartbeats = 2;

    relaymq::Server server_3;
    ASSERT_TRUE(server_3.start(config3));

    relaymq::test::TestSocket peer;
    ASSERT_TRUE(peer.connect(server_3.cluster_port()));
    ASSERT_TRUE(peer.send("ROUTE_CONNECT cluster_1 node_4\r\n"));

    ASSERT_TRUE(wait_peers(server_3, 1));
    ASSERT_TRUE(wait_peers(server_3, 0, 1000));

    peer.close();
    server_3.stop();
}

TEST_F(ClusterTest, NodeRestartRecovery) {
    ASSERT_TRUE(connect_nodes());

    relaymq::test::TestSocket client_1;
    ASSERT_TRUE(client_1.connect(server_1_.port()));
    ASSERT_TRUE(client_1.send("SUB service.feed.orders 1\r\n"));
    EXPECT_EQ(client_1.read_line(), "OK\r\n");

    relaymq::test::TestSocket client_2;
    ASSERT_TRUE(client_2.connect(server_2_.port()));
    ASSERT_TRUE(client_2.send("SUB service.feed.executions 2\r\n"));
    EXPECT_EQ(client_2.read_line(), "OK\r\n");

    ASSERT_TRUE(wait_remote_subs(server_1_, 1));
    ASSERT_TRUE(wait_remote_subs(server_2_, 1));

    client_2.close();
    server_2_.stop();

    ASSERT_TRUE(wait_peers(server_1_, 0));
    ASSERT_TRUE(wait_remote_subs(server_1_, 0));

    EXPECT_EQ(server_1_.local_subject_count("service.feed.orders"), 1);
    ASSERT_TRUE(client_1.send("PUB service.feed.orders 18\r\nlocal_test_payload\r\n"));
    EXPECT_EQ(client_1.read_line(), "MSG service.feed.orders 1 18\r\n");
    EXPECT_EQ(client_1.read_line(), "local_test_payload\r\n");

    relaymq::ServerConfig config3;
    config3.port = 0;
    config3.enable_clustering = true;
    config3.cluster_port = 0;
    config3.cluster_name = "cluster_1";
    config3.node_id = "node_2";

    relaymq::Server server_2_restarted;
    ASSERT_TRUE(server_2_restarted.start(config3));

    ASSERT_TRUE(server_1_.connect_to_peer("127.0.0.1", server_2_restarted.cluster_port()));

    ASSERT_TRUE(wait_peers(server_1_, 1));
    ASSERT_TRUE(wait_peers(server_2_restarted, 1));
    ASSERT_TRUE(wait_remote_subs(server_2_restarted, 1));
    EXPECT_EQ(server_2_restarted.match_peers("service.feed.orders").size(), 1);

    relaymq::test::TestSocket client_2_new;
    ASSERT_TRUE(client_2_new.connect(server_2_restarted.port()));

    ASSERT_TRUE(client_1.send("SUB service.data.restored 3\r\n"));
    EXPECT_EQ(client_1.read_line(), "OK\r\n");
    ASSERT_TRUE(wait_remote_subs(server_2_restarted, 2));

    ASSERT_TRUE(client_2_new.send("PUB service.data.restored 15\r\nresumed_payload\r\n"));

    EXPECT_EQ(client_1.read_line(), "MSG service.data.restored 3 15\r\n");
    EXPECT_EQ(client_1.read_line(), "resumed_payload\r\n");

    client_1.close();
    client_2_new.close();
    server_2_restarted.stop();
}

TEST_F(ClusterTest, LocalClientsUnaffectedByPeerPartition) {
    ASSERT_TRUE(connect_nodes());

    relaymq::test::TestSocket sub;
    ASSERT_TRUE(sub.connect(server_1_.port()));
    ASSERT_TRUE(sub.send("SUB service.local.only 1\r\n"));
    EXPECT_EQ(sub.read_line(), "OK\r\n");

    relaymq::test::TestSocket pub;
    ASSERT_TRUE(pub.connect(server_1_.port()));

    ASSERT_TRUE(pub.send("PUB service.local.only 4\r\nmsg1\r\n"));
    EXPECT_EQ(sub.read_line(), "MSG service.local.only 1 4\r\n");
    EXPECT_EQ(sub.read_line(), "msg1\r\n");

    server_2_.stop();

    ASSERT_TRUE(wait_peers(server_1_, 0));

    ASSERT_TRUE(pub.send("PUB service.local.only 4\r\nmsg2\r\n"));
    EXPECT_EQ(sub.read_line(), "MSG service.local.only 1 4\r\n");
    EXPECT_EQ(sub.read_line(), "msg2\r\n");

    ASSERT_TRUE(pub.send("PUB service.local.only 4\r\nmsg3\r\n"));
    EXPECT_EQ(sub.read_line(), "MSG service.local.only 1 4\r\n");
    EXPECT_EQ(sub.read_line(), "msg3\r\n");

    sub.close();
    pub.close();
}
