#include "relaymq/protocol.hpp"
#include <gtest/gtest.h>

TEST(ProtocolParserTest, ControlCommands) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    parser.append("PING\r\nPONG\r\nOK\r\nERR invalid subject\r\n");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::PING);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::PONG);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::OK);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::ERR);
    EXPECT_EQ(cmd.error_message, "invalid subject");

    EXPECT_FALSE(parser.next_command(cmd));
}

TEST(ProtocolParserTest, SubscribeAndUnsubscribe) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    parser.append("SUB service.message.value 101\r\nUNSUB 101\r\n");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::SUB);
    EXPECT_EQ(cmd.subject, "service.message.value");
    EXPECT_EQ(cmd.sid, "101");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::UNSUB);
    EXPECT_EQ(cmd.sid, "101");

    EXPECT_FALSE(parser.next_command(cmd));
}

TEST(ProtocolParserTest, PublishPayload) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    std::string wire = relaymq::ProtocolParser::format_pub("service.message.value", "amount=50");
    parser.append(wire);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::PUB);
    EXPECT_EQ(cmd.subject, "service.message.value");
    EXPECT_EQ(cmd.payload, "amount=50");
    EXPECT_TRUE(cmd.reply_to.empty());
}

TEST(ProtocolParserTest, PublishWithReplyTo) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    std::string pub = relaymq::ProtocolParser::format_pub("service.message.value", "amount=50", "box.user_1");
    parser.append(pub);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::PUB);
    EXPECT_EQ(cmd.subject, "service.message.value");
    EXPECT_EQ(cmd.reply_to, "box.user_1");
    EXPECT_EQ(cmd.payload, "amount=50");
}

TEST(ProtocolParserTest, StreamFragmentation) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    parser.append("PUB service.message.new 1");
    EXPECT_FALSE(parser.next_command(cmd));

    parser.append("4\r\n{\"value\":\"");
    EXPECT_FALSE(parser.next_command(cmd));

    parser.append("50\"}\r\n");
    ASSERT_TRUE(parser.next_command(cmd));

    EXPECT_EQ(cmd.type, relaymq::CommandType::PUB);
    EXPECT_EQ(cmd.subject, "service.message.new");
    EXPECT_EQ(cmd.payload, "{\"value\":\"50\"}");

    EXPECT_EQ(parser.buffered_bytes(), 0);
}

TEST(ProtocolParserTest, Serializers) {
    EXPECT_EQ(relaymq::ProtocolParser::format_ping(), "PING\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_pong(), "PONG\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_ok(), "OK\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_err("unknown topic"), "ERR unknown topic\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_sub("service.counts.*", "1"), "SUB service.counts.* 1\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_unsub("1"), "UNSUB 1\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_pub("service.message", "abc"), "PUB service.message 3\r\nabc\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_msg("service.message", "1", "abc"), "MSG service.message 1 3\r\nabc\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_route_connect("cluster_1", "node_a"), "ROUTE_CONNECT cluster_1 node_a\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_rsub("service.data.*"), "RSUB service.data.*\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_runsub("service.data.*"), "RUNSUB service.data.*\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_rmsg("service.data.update", "payload123"), "RMSG service.data.update 10\r\npayload123\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_rmsg("service.data.update", "payload123", "_REPLY.42"), "RMSG service.data.update _REPLY.42 10\r\npayload123\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_rping(), "RPING\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_rpong(), "RPONG\r\n");
}

TEST(ProtocolParserTest, RouteCommands) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    parser.append("ROUTE_CONNECT cluster_1 node_1\r\nRPING\r\nRPONG\r\n");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::ROUTE_CONNECT);
    EXPECT_EQ(cmd.cluster_name, "cluster_1");
    EXPECT_EQ(cmd.node_id, "node_1");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::RPING);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::RPONG);

    EXPECT_FALSE(parser.next_command(cmd));
}

TEST(ProtocolParserTest, RouteSubscriptions) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    parser.append("RSUB service.orders.*\r\nRUNSUB service.orders.*\r\n");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::RSUB);
    EXPECT_EQ(cmd.subject, "service.orders.*");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::RUNSUB);
    EXPECT_EQ(cmd.subject, "service.orders.*");

    EXPECT_FALSE(parser.next_command(cmd));
}

TEST(ProtocolParserTest, RouteMessages) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    std::string m1 = relaymq::ProtocolParser::format_rmsg("service.events.update", "status=ok");
    parser.append(m1);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::RMSG);
    EXPECT_EQ(cmd.subject, "service.events.update");
    EXPECT_EQ(cmd.payload, "status=ok");
    EXPECT_TRUE(cmd.reply_to.empty());

    std::string m2 = relaymq::ProtocolParser::format_rmsg("service.events.query", "query_id=10", "_REPLY.99");
    parser.append(m2);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::RMSG);
    EXPECT_EQ(cmd.subject, "service.events.query");
    EXPECT_EQ(cmd.payload, "query_id=10");
    EXPECT_EQ(cmd.reply_to, "_REPLY.99");

    EXPECT_FALSE(parser.next_command(cmd));
}

TEST(ProtocolParserTest, RouteStreamFragmentation) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    parser.append("RMSG service.data.stream 8\r\n123");
    EXPECT_FALSE(parser.next_command(cmd));

    parser.append("45678\r\n");
    ASSERT_TRUE(parser.next_command(cmd));

    EXPECT_EQ(cmd.type, relaymq::CommandType::RMSG);
    EXPECT_EQ(cmd.subject, "service.data.stream");
    EXPECT_EQ(cmd.payload, "12345678");
    EXPECT_EQ(parser.buffered_bytes(), 0);
}
