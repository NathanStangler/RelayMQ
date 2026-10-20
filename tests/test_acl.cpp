#include "relaymq/server.hpp"
#include "relaymq/client.hpp"
#include "relaymq/acl.hpp"

#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

TEST(AclTest, UnrestrictedAccess) {
    relaymq::SubjectAcl acl(true);
    EXPECT_TRUE(acl.is_unrestricted());
    EXPECT_TRUE(acl.can_publish("service.billing.invoices"));
    EXPECT_TRUE(acl.can_subscribe("service.notifications.email"));
}

TEST(AclTest, ScopedPublishWildcards) {
    relaymq::SubjectAcl acl(false);
    EXPECT_FALSE(acl.is_unrestricted());

    acl.allow_publish("service.jobs.start");
    acl.allow_publish("service.*.metrics");
    acl.allow_publish("service.telemetry.>");

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
    acl.allow_subscribe("service.events.>");
    acl.allow_subscribe("events.>");

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

    relaymq::Client sub_client;
    ASSERT_TRUE(sub_client.connect("127.0.0.1", server.port(), "consumer_token"));

    std::mutex cv_mutex;
    std::condition_variable cv;
    bool received = false;
    std::string received_payload;

    std::string sid = sub_client.subscribe("service.events.created", [&](const relaymq::Message& msg) {
        std::unique_lock<std::mutex> lock(cv_mutex);
        received = true;
        received_payload = msg.payload;
        lock.unlock();
        cv.notify_one();
    });
    EXPECT_FALSE(sid.empty());

    bool pub_denied = sub_client.publish("service.events.created", "payload");
    EXPECT_TRUE(pub_denied);

    relaymq::Client pub_client;
    ASSERT_TRUE(pub_client.connect("127.0.0.1", server.port(), "worker_token"));

    std::string bad_sid = pub_client.subscribe("service.admin.secrets", [](const relaymq::Message&) {});
    (void)bad_sid;

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_TRUE(pub_client.publish("service.events.created", "job_id=20"));

    std::unique_lock<std::mutex> lock(cv_mutex);
    bool ok = cv.wait_for(lock, std::chrono::seconds(2), [&]() {
        return received;
    });

    EXPECT_TRUE(ok);
    EXPECT_EQ(received_payload, "job_id=20");

    EXPECT_TRUE(sub_client.is_connected());
    EXPECT_TRUE(pub_client.is_connected());

    sub_client.disconnect();
    pub_client.disconnect();
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

    relaymq::Client admin_client;
    ASSERT_TRUE(admin_client.connect("127.0.0.1", server.port(), "admin_token"));

    std::mutex cv_mutex;
    std::condition_variable cv;
    bool received = false;

    admin_client.subscribe("service.internal.audit", [&](const relaymq::Message&) {
        std::unique_lock<std::mutex> lock(cv_mutex);
        received = true;
        lock.unlock();
        cv.notify_one();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    admin_client.publish("service.internal.audit", "admin_payload");

    std::unique_lock<std::mutex> lock(cv_mutex);
    bool ok = cv.wait_for(lock, std::chrono::seconds(2), [&]() {
        return received;
    });

    EXPECT_TRUE(ok);

    admin_client.disconnect();
    server.stop();
}
