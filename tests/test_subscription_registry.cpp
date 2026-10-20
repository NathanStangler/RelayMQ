#include "relaymq/subscription_registry.hpp"
#include <gtest/gtest.h>

TEST(SubscriptionRegistryTest, SubjectValidation) {
    EXPECT_TRUE(relaymq::SubscriptionRegistry::is_valid_subject("service"));
    EXPECT_TRUE(relaymq::SubscriptionRegistry::is_valid_subject("service.user.create"));
    EXPECT_TRUE(relaymq::SubscriptionRegistry::is_valid_subject("service.*.create"));
    EXPECT_TRUE(relaymq::SubscriptionRegistry::is_valid_subject("service.user.>"));
    EXPECT_TRUE(relaymq::SubscriptionRegistry::is_valid_subject(">"));
    EXPECT_TRUE(relaymq::SubscriptionRegistry::is_valid_subject("*"));

    EXPECT_FALSE(relaymq::SubscriptionRegistry::is_valid_subject(""));
    EXPECT_FALSE(relaymq::SubscriptionRegistry::is_valid_subject("service..create"));
    EXPECT_FALSE(relaymq::SubscriptionRegistry::is_valid_subject(".service"));
    EXPECT_FALSE(relaymq::SubscriptionRegistry::is_valid_subject("service."));
    EXPECT_FALSE(relaymq::SubscriptionRegistry::is_valid_subject("service.>.create"));
    EXPECT_FALSE(relaymq::SubscriptionRegistry::is_valid_subject("service.user*"));
    EXPECT_FALSE(relaymq::SubscriptionRegistry::is_valid_subject("service.>user"));
}

TEST(SubscriptionRegistryTest, ExactMatching) {
    relaymq::SubscriptionRegistry registry;

    registry.add_subscription("service.auth.login", "client_1", "sub_1");
    registry.add_subscription("service.auth.logout", "client_2", "sub_2");
    registry.add_subscription("service.data.backup", "client_3", "sub_3");

    EXPECT_EQ(registry.total_subscriptions(), 3);

    std::vector<relaymq::Subscription> matches_login = registry.match("service.auth.login");
    ASSERT_EQ(matches_login.size(), 1);
    EXPECT_EQ(matches_login[0].client_id, "client_1");
    EXPECT_EQ(matches_login[0].sid, "sub_1");

    std::vector<relaymq::Subscription> matches_logout = registry.match("service.auth.logout");
    ASSERT_EQ(matches_logout.size(), 1);
    EXPECT_EQ(matches_logout[0].client_id, "client_2");
    EXPECT_EQ(matches_logout[0].sid, "sub_2");

    std::vector<relaymq::Subscription> matches_restore = registry.match("service.data.restore");
    EXPECT_EQ(matches_restore.size(), 0);
}

TEST(SubscriptionRegistryTest, SingleTokenWildcard) {
    relaymq::SubscriptionRegistry registry;

    registry.add_subscription("service.*.status", "client_1", "1");
    registry.add_subscription("service.backend.*", "client_2", "2");

    std::vector<relaymq::Subscription> m1 = registry.match("service.backend.status");
    EXPECT_EQ(m1.size(), 2);

    std::vector<relaymq::Subscription> m2 = registry.match("service.frontend.status");
    ASSERT_EQ(m2.size(), 1);
    EXPECT_EQ(m2[0].client_id, "client_1");

    std::vector<relaymq::Subscription> m3 = registry.match("service.backend.metrics");
    ASSERT_EQ(m3.size(), 1);
    EXPECT_EQ(m3[0].client_id, "client_2");

    std::vector<relaymq::Subscription> m4 = registry.match("service.backend.internal.status");
    EXPECT_EQ(m4.size(), 0);
}

TEST(SubscriptionRegistryTest, TailWildcard) {
    relaymq::SubscriptionRegistry registry;

    registry.add_subscription("service.internal.>", "client_1", "1");

    std::vector<relaymq::Subscription> m1 = registry.match("service.internal.auth");
    ASSERT_EQ(m1.size(), 1);
    EXPECT_EQ(m1[0].client_id, "client_1");

    std::vector<relaymq::Subscription> m2 = registry.match("service.internal.auth.token");
    ASSERT_EQ(m2.size(), 1);
    EXPECT_EQ(m2[0].client_id, "client_1");

    std::vector<relaymq::Subscription> m3 = registry.match("service.internal.auth.token.refresh.job");
    ASSERT_EQ(m3.size(), 1);
    EXPECT_EQ(m3[0].client_id, "client_1");

    std::vector<relaymq::Subscription> m4 = registry.match("service.external.api");
    EXPECT_EQ(m4.size(), 0);
}

TEST(SubscriptionRegistryTest, CombinedWildcards) {
    relaymq::SubscriptionRegistry registry;

    registry.add_subscription("service.worker.job", "client_exact", "1");
    registry.add_subscription("service.*.job", "client_star", "2");
    registry.add_subscription("service.>", "client_tail", "3");

    std::vector<relaymq::Subscription> matches = registry.match("service.worker.job");
    EXPECT_EQ(matches.size(), 3);

    std::vector<relaymq::Subscription> matches_cron = registry.match("service.cron.job");
    EXPECT_EQ(matches_cron.size(), 2);

    std::vector<relaymq::Subscription> matches_restart = registry.match("service.cron.job.restart");
    ASSERT_EQ(matches_restart.size(), 1);
    EXPECT_EQ(matches_restart[0].client_id, "client_tail");
}

TEST(SubscriptionRegistryTest, RemoveSubscription) {
    relaymq::SubscriptionRegistry registry;

    registry.add_subscription("service.notification.email", "client_1", "sub_1");
    registry.add_subscription("service.notification.email", "client_2", "sub_2");
    EXPECT_EQ(registry.total_subscriptions(), 2);

    bool removed = registry.remove_subscription("client_1", "sub_1");
    EXPECT_TRUE(removed);
    EXPECT_EQ(registry.total_subscriptions(), 1);

    std::vector<relaymq::Subscription> matches = registry.match("service.notification.email");
    ASSERT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0].client_id, "client_2");

    EXPECT_FALSE(registry.remove_subscription("client_1", "sub_1"));
}

TEST(SubscriptionRegistryTest, RemoveAllForClient) {
    relaymq::SubscriptionRegistry registry;

    registry.add_subscription("service.notification.email", "client_1", "sub_1");
    registry.add_subscription("service.billing.>", "client_1", "sub_2");
    registry.add_subscription("service.notification.*", "client_2", "sub_3");
    EXPECT_EQ(registry.total_subscriptions(), 3);

    registry.remove_all_for_client("client_1");
    EXPECT_EQ(registry.total_subscriptions(), 1);

    EXPECT_EQ(registry.match("service.billing.invoice").size(), 0);

    std::vector<relaymq::Subscription> matches = registry.match("service.notification.email");
    ASSERT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0].client_id, "client_2");
}

TEST(SubscriptionRegistryTest, RemoveSubscriptionWithSubject) {
    relaymq::SubscriptionRegistry registry;

    registry.add_subscription("service.order.created", "client_1", "sub_1");
    registry.add_subscription("service.order.*", "client_1", "sub_2");
    registry.add_subscription("service.order.>", "client_1", "sub_3");

    std::string subject;
    EXPECT_TRUE(registry.remove_subscription("client_1", "sub_1", &subject));
    EXPECT_EQ(subject, "service.order.created");

    EXPECT_TRUE(registry.remove_subscription("client_1", "sub_3", &subject));
    EXPECT_EQ(subject, "service.order.>");

    EXPECT_TRUE(registry.remove_subscription("client_1", "sub_2", &subject));
    EXPECT_EQ(subject, "service.order.*");

    EXPECT_FALSE(registry.remove_subscription("client_1", "sub_1", &subject));
    EXPECT_EQ(registry.total_subscriptions(), 0);
}

TEST(SubscriptionRegistryTest, RemoveAllReturnsSubjects) {
    relaymq::SubscriptionRegistry registry;

    registry.add_subscription("service.a", "client_1", "1");
    registry.add_subscription("service.b.*", "client_1", "2");
    registry.add_subscription("service.c.>", "client_1", "3");

    std::vector<std::string> removed = registry.remove_all_for_client("client_1");
    EXPECT_EQ(removed.size(), 3);
    EXPECT_EQ(registry.total_subscriptions(), 0);

    registry.clear();
    EXPECT_EQ(registry.total_subscriptions(), 0);
}
