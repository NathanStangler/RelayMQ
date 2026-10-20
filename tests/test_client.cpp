#include "relaymq/server.hpp"
#include "relaymq/client.hpp"
#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <future>

class ClientTest : public testing::Test {
protected:
    void SetUp() override {
        relaymq::ServerConfig config;
        config.port = 0;
        ASSERT_TRUE(server_.start(config));
        port_ = server_.port();
        ASSERT_GT(port_, 0);
    }

    void TearDown() override {
        server_.stop();
    }

    relaymq::Server server_;
    uint16_t port_ = 0;
};

TEST_F(ClientTest, ConnectDisconnect) {
    relaymq::Client client;
    EXPECT_FALSE(client.is_connected());

    ASSERT_TRUE(client.connect("127.0.0.1", port_));
    EXPECT_TRUE(client.is_connected());

    client.disconnect();
    EXPECT_FALSE(client.is_connected());
}

TEST_F(ClientTest, PingLatency) {
    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", port_));

    int64_t latency = client.ping();
    EXPECT_GE(latency, 0);

    client.disconnect();
}

TEST_F(ClientTest, PublishSubscribe) {
    relaymq::Client sub_client;
    ASSERT_TRUE(sub_client.connect("127.0.0.1", port_));

    std::promise<relaymq::Message> promise;
    std::future<relaymq::Message> future = promise.get_future();

    std::string sid = sub_client.subscribe("service.test.pubsub", [&](const relaymq::Message& msg) {
        promise.set_value(msg);
    });
    ASSERT_FALSE(sid.empty());

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    relaymq::Client pub_client;
    ASSERT_TRUE(pub_client.connect("127.0.0.1", port_));
    ASSERT_TRUE(pub_client.publish("service.test.pubsub", "test"));

    ASSERT_EQ(future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    relaymq::Message received = future.get();

    EXPECT_EQ(received.subject, "service.test.pubsub");
    EXPECT_EQ(received.sid, sid);
    EXPECT_EQ(received.payload, "test");

    sub_client.disconnect();
    pub_client.disconnect();
}

TEST_F(ClientTest, Unsubscribe) {
    relaymq::Client sub_client;
    ASSERT_TRUE(sub_client.connect("127.0.0.1", port_));

    std::atomic<int> received_count(0);
    std::string sid = sub_client.subscribe("service.test.unsub", [&](const relaymq::Message&) {
        received_count++;
    });
    ASSERT_FALSE(sid.empty());

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    relaymq::Client pub_client;
    ASSERT_TRUE(pub_client.connect("127.0.0.1", port_));

    ASSERT_TRUE(pub_client.publish("service.test.unsub", "msg_1"));
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(received_count.load(), 1);

    ASSERT_TRUE(sub_client.unsubscribe(sid));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    ASSERT_TRUE(pub_client.publish("service.test.unsub", "msg_2"));
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(received_count.load(), 1);

    sub_client.disconnect();
    pub_client.disconnect();
}

TEST_F(ClientTest, RequestReplySuccess) {
    relaymq::Client responder;
    ASSERT_TRUE(responder.connect("127.0.0.1", port_));

    responder.subscribe("service.compute.double", [&responder](const relaymq::Message& msg) {
        if (!msg.reply_to.empty()) {
            int value = std::stoi(msg.payload);
            responder.publish(msg.reply_to, std::to_string(value * 2));
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    relaymq::Client requester;
    ASSERT_TRUE(requester.connect("127.0.0.1", port_));

    std::optional<relaymq::Message> reply = requester.request("service.compute.double", "20", 1000);
    ASSERT_TRUE(reply.has_value());
    EXPECT_EQ(reply->payload, "40");

    responder.disconnect();
    requester.disconnect();
}

TEST_F(ClientTest, RequestReplyTimeout) {
    relaymq::Client requester;
    ASSERT_TRUE(requester.connect("127.0.0.1", port_));

    std::optional<relaymq::Message> reply = requester.request("service.unknown", "ping", 100);
    EXPECT_FALSE(reply.has_value());

    requester.disconnect();
}
