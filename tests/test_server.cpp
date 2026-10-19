#include "relaymq/server.hpp"
#include "relaymq/client.hpp"
#include <gtest/gtest.h>
#include <future>
#include <chrono>

class ServerTest : public testing::Test {
protected:
    void SetUp() override {
        ASSERT_TRUE(server_.start(0));
        port_ = server_.port();
        ASSERT_GT(port_, 0);
    }

    void TearDown() override {
        server_.stop();
    }

    relaymq::Server server_;
    uint16_t port_ = 0;
};

TEST_F(ServerTest, PingPong) {
    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", port_));
    EXPECT_TRUE(client.is_connected());

    int64_t latency = client.ping();
    EXPECT_GE(latency, 0);

    client.disconnect();
    EXPECT_FALSE(client.is_connected());
}

TEST_F(ServerTest, PublishSubscribe) {
    relaymq::Client subscriber;
    ASSERT_TRUE(subscriber.connect("127.0.0.1", port_));

    std::promise<relaymq::Message> message_promise;
    std::future<relaymq::Message> message_future = message_promise.get_future();

    std::string sid = subscriber.subscribe("service.data.update", [&](const relaymq::Message& msg) {
        message_promise.set_value(msg);
    });
    ASSERT_FALSE(sid.empty());

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    relaymq::Client publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", port_));
    ASSERT_TRUE(publisher.publish("service.data.update", "value=50"));

    ASSERT_EQ(message_future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    relaymq::Message received = message_future.get();

    EXPECT_EQ(received.subject, "service.data.update");
    EXPECT_EQ(received.sid, sid);
    EXPECT_EQ(received.payload, "value=50");

    subscriber.disconnect();
    publisher.disconnect();
}

TEST_F(ServerTest, WildcardRouting) {
    relaymq::Client star_sub;
    ASSERT_TRUE(star_sub.connect("127.0.0.1", port_));

    relaymq::Client tail_sub;
    ASSERT_TRUE(tail_sub.connect("127.0.0.1", port_));

    std::atomic<int> star_count(0);
    std::atomic<int> tail_count(0);

    star_sub.subscribe("service.*.event", [&](const relaymq::Message&) {
        star_count++;
    });

    tail_sub.subscribe("service.internal.>", [&](const relaymq::Message&) {
        tail_count++;
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    relaymq::Client publisher;
    ASSERT_TRUE(publisher.connect("127.0.0.1", port_));

    publisher.publish("service.internal.event", "payload_1");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_EQ(star_count.load(), 1);
    EXPECT_EQ(tail_count.load(), 1);

    publisher.publish("service.external.event", "payload_2");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_EQ(star_count.load(), 2);
    EXPECT_EQ(tail_count.load(), 1);

    publisher.publish("service.internal.database.error", "payload_3");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_EQ(star_count.load(), 2);
    EXPECT_EQ(tail_count.load(), 2);

    star_sub.disconnect();
    tail_sub.disconnect();
    publisher.disconnect();
}
