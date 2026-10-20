#include "relaymq/outbound_writer.hpp"
#include "relaymq/connection.hpp"
#include "relaymq/net.hpp"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <thread>

TEST(OutboundWriterTest, StartAndStop) {
    relaymq::OutboundWriter writer;
    writer.start();
    writer.stop();
}

TEST(OutboundWriterTest, WriteConnection) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    relaymq::net::set_nonblocking(sv[0]);
    relaymq::net::set_nonblocking(sv[1]);

    std::shared_ptr<relaymq::Connection> conn = std::make_shared<relaymq::Connection>(sv[0], "test_client", true);

    relaymq::OutboundWriter writer;
    writer.start();

    EXPECT_TRUE(writer.enqueue(conn, "PING\r\n"));

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    char buf[64];
    ssize_t bytes = recv(sv[1], buf, sizeof(buf) - 1, 0);
    ASSERT_GT(bytes, 0);
    buf[bytes] = '\0';
    EXPECT_STREQ(buf, "PING\r\n");

    writer.stop();
    conn->close_connection();
    close(sv[1]);
}
