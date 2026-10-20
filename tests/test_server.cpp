#include "relaymq/server.hpp"
#include "test_socket.hpp"
#include <gtest/gtest.h>

class ServerTest : public testing::Test {
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

TEST_F(ServerTest, PingPong) {
    relaymq::test::TestSocket client;
    ASSERT_TRUE(client.connect(port_));
    EXPECT_TRUE(client.is_connected());

    ASSERT_TRUE(client.send("PING\r\n"));
    EXPECT_EQ(client.read_line(), "PONG\r\n");

    client.close();
    EXPECT_FALSE(client.is_connected());
}

TEST_F(ServerTest, PublishSubscribe) {
    relaymq::test::TestSocket subscriber;
    ASSERT_TRUE(subscriber.connect(port_));
    ASSERT_TRUE(subscriber.send("SUB service.data.update 100\r\n"));
    EXPECT_EQ(subscriber.read_line(), "OK\r\n");

    relaymq::test::TestSocket publisher;
    ASSERT_TRUE(publisher.connect(port_));
    ASSERT_TRUE(publisher.send("PUB service.data.update 8\r\nvalue=50\r\n"));

    EXPECT_EQ(subscriber.read_line(), "MSG service.data.update 100 8\r\n");
    EXPECT_EQ(subscriber.read_line(), "value=50\r\n");

    subscriber.close();
    publisher.close();
}

TEST_F(ServerTest, WildcardRouting) {
    relaymq::test::TestSocket star_sub;
    ASSERT_TRUE(star_sub.connect(port_));
    ASSERT_TRUE(star_sub.send("SUB service.*.event star_1\r\n"));
    EXPECT_EQ(star_sub.read_line(), "OK\r\n");

    relaymq::test::TestSocket tail_sub;
    ASSERT_TRUE(tail_sub.connect(port_));
    ASSERT_TRUE(tail_sub.send("SUB service.internal.> tail_1\r\n"));
    EXPECT_EQ(tail_sub.read_line(), "OK\r\n");

    relaymq::test::TestSocket publisher;
    ASSERT_TRUE(publisher.connect(port_));

    ASSERT_TRUE(publisher.send("PUB service.internal.event 9\r\npayload_1\r\n"));

    EXPECT_EQ(star_sub.read_line(), "MSG service.internal.event star_1 9\r\n");
    EXPECT_EQ(star_sub.read_line(), "payload_1\r\n");

    EXPECT_EQ(tail_sub.read_line(), "MSG service.internal.event tail_1 9\r\n");
    EXPECT_EQ(tail_sub.read_line(), "payload_1\r\n");

    ASSERT_TRUE(publisher.send("PUB service.external.event 9\r\npayload_2\r\n"));

    EXPECT_EQ(star_sub.read_line(), "MSG service.external.event star_1 9\r\n");
    EXPECT_EQ(star_sub.read_line(), "payload_2\r\n");
    EXPECT_TRUE(tail_sub.read_line(50).empty());

    ASSERT_TRUE(publisher.send("PUB service.internal.database.error 9\r\npayload_3\r\n"));

    EXPECT_EQ(tail_sub.read_line(), "MSG service.internal.database.error tail_1 9\r\n");
    EXPECT_EQ(tail_sub.read_line(), "payload_3\r\n");
    EXPECT_TRUE(star_sub.read_line(50).empty());

    star_sub.close();
    tail_sub.close();
    publisher.close();
}
