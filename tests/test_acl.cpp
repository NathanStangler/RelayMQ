#include "relaymq/server.hpp"
#include "test_socket.hpp"
#include "relaymq/acl.hpp"

#include <gtest/gtest.h>

TEST(AclTest, UnrestrictedAccess) {
    relaymq::SubjectAcl acl(true);
    EXPECT_TRUE(acl.is_unrestricted());
    EXPECT_TRUE(acl.can_publish("service.billing.invoices"));
    EXPECT_TRUE(acl.can_subscribe("service.notifications.email"));
}

TEST(AclTest, PatternAccess) {
    relaymq::SubjectAcl acl(false);
    EXPECT_FALSE(acl.is_unrestricted());

    EXPECT_FALSE(acl.allow_publish(""));
    EXPECT_FALSE(acl.allow_publish("service..metrics"));
    EXPECT_FALSE(acl.allow_publish("service.>.invalid"));
    EXPECT_EQ(acl.publish_rule_count(), 0);

    EXPECT_TRUE(acl.allow_publish("service.jobs.start"));
    EXPECT_TRUE(acl.allow_subscribe("service.events.>"));
    EXPECT_EQ(acl.publish_rule_count(), 1);
    EXPECT_EQ(acl.subscribe_rule_count(), 1);

    EXPECT_TRUE(acl.can_publish("service.jobs.start"));
    EXPECT_TRUE(acl.can_subscribe("service.events.created"));

    acl.clear();
    EXPECT_EQ(acl.publish_rule_count(), 0);
    EXPECT_EQ(acl.subscribe_rule_count(), 0);
    EXPECT_FALSE(acl.can_publish("service.jobs.start"));
    EXPECT_FALSE(acl.can_subscribe("service.events.created"));
}

TEST(AclTest, ScopedPublishWildcards) {
    relaymq::SubjectAcl acl(false);
    EXPECT_FALSE(acl.is_unrestricted());

    EXPECT_TRUE(acl.allow_publish("service.jobs.start"));
    EXPECT_TRUE(acl.allow_publish("service.*.metrics"));
    EXPECT_TRUE(acl.allow_publish("service.telemetry.>"));

    EXPECT_TRUE(acl.can_publish("service.jobs.start"));
    EXPECT_FALSE(acl.can_publish("service.jobs.cancel"));

    EXPECT_TRUE(acl.can_publish("service.auth.metrics"));
    EXPECT_TRUE(acl.can_publish("service.billing.metrics"));
    EXPECT_FALSE(acl.can_publish("service.auth.events"));

    EXPECT_TRUE(acl.can_publish("service.telemetry.cpu"));
    EXPECT_TRUE(acl.can_publish("service.telemetry.cluster.node1.mem"));
    EXPECT_FALSE(acl.can_publish("other.telemetry.cpu"));

    EXPECT_FALSE(acl.can_subscribe("service.jobs.start"));
}

TEST(AclTest, ScopedSubscribeWildcards) {
    relaymq::SubjectAcl acl(false);
    EXPECT_TRUE(acl.allow_subscribe("service.events.>"));
    EXPECT_TRUE(acl.allow_subscribe("events.>"));

    EXPECT_TRUE(acl.can_subscribe("service.events.created"));
    EXPECT_TRUE(acl.can_subscribe("service.events.>"));
    EXPECT_TRUE(acl.can_subscribe("events.1234"));

    EXPECT_FALSE(acl.can_subscribe("service.admin.config"));
    EXPECT_FALSE(acl.can_subscribe("service.internal.shutdown"));
}

TEST(AclTest, ScopedPublishSubscribe) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    config.auth_token = "admin_token";

    relaymq::TokenRule pub_rule;
    pub_rule.token = "worker_token";
    pub_rule.allow_publish = {"service.events.>"};
    config.token_rules.push_back(pub_rule);

    relaymq::TokenRule sub_rule;
    sub_rule.token = "consumer_token";
    sub_rule.allow_subscribe = {"service.events.>"};
    config.token_rules.push_back(sub_rule);

    ASSERT_TRUE(server.start(config));

    relaymq::test::TestSocket sub;
    ASSERT_TRUE(sub.connect(server.port()));
    ASSERT_TRUE(sub.send("AUTH consumer_token\r\n"));
    EXPECT_EQ(sub.read_line(), "OK\r\n");
    ASSERT_TRUE(sub.send("SUB service.events.created 1\r\n"));
    EXPECT_EQ(sub.read_line(), "OK\r\n");

    ASSERT_TRUE(sub.send("PUB service.events.created 20\r\nunauthorized_payload\r\n"));

    std::string err = sub.read_line();
    EXPECT_TRUE(err.starts_with("ERR "));
    EXPECT_NE(err.find("Permission Violation"), std::string::npos);

    relaymq::test::TestSocket pub;
    ASSERT_TRUE(pub.connect(server.port()));
    ASSERT_TRUE(pub.send("AUTH worker_token\r\n"));
    EXPECT_EQ(pub.read_line(), "OK\r\n");
    ASSERT_TRUE(pub.send("PUB service.events.created 9\r\njob_id=20\r\n"));

    EXPECT_EQ(sub.read_line(), "MSG service.events.created 1 9\r\n");
    EXPECT_EQ(sub.read_line(), "job_id=20\r\n");

    sub.close();
    pub.close();
    server.stop();
}

TEST(AclTest, UnrestrictedAdmin) {
    relaymq::Server server;
    relaymq::ServerConfig config;
    config.port = 0;
    config.auth_token = "admin_token";

    relaymq::TokenRule restricted_rule;
    restricted_rule.token = "restricted";
    restricted_rule.allow_subscribe = {"service.public.>"};
    config.token_rules.push_back(restricted_rule);

    ASSERT_TRUE(server.start(config));

    relaymq::test::TestSocket admin;
    ASSERT_TRUE(admin.connect(server.port()));
    ASSERT_TRUE(admin.send("AUTH admin_token\r\n"));
    EXPECT_EQ(admin.read_line(), "OK\r\n");
    ASSERT_TRUE(admin.send("SUB service.internal.audit 1\r\n"));
    EXPECT_EQ(admin.read_line(), "OK\r\n");

    ASSERT_TRUE(admin.send("PUB service.internal.audit 13\r\nadmin_payload\r\n"));

    EXPECT_EQ(admin.read_line(), "MSG service.internal.audit 1 13\r\n");
    EXPECT_EQ(admin.read_line(), "admin_payload\r\n");

    admin.close();
    server.stop();
}
