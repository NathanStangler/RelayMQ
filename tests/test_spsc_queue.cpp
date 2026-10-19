#include "relaymq/spsc_queue.hpp"
#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <string>

TEST(SpscQueueTest, PushPop) {
    relaymq::SpscQueue<int, 4> queue;

    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.size(), 0);
    EXPECT_EQ(queue.capacity(), 4);

    EXPECT_TRUE(queue.push(10));
    EXPECT_TRUE(queue.push(20));
    EXPECT_FALSE(queue.empty());
    EXPECT_EQ(queue.size(), 2);

    int value = 0;
    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 10);
    EXPECT_EQ(queue.size(), 1);

    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 20);
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.size(), 0);

    EXPECT_FALSE(queue.pop(value));
}

TEST(SpscQueueTest, CapacityAndFull) {
    relaymq::SpscQueue<int, 4> queue;

    EXPECT_TRUE(queue.push(1));
    EXPECT_TRUE(queue.push(2));
    EXPECT_TRUE(queue.push(3));
    EXPECT_TRUE(queue.push(4));

    EXPECT_FALSE(queue.push(5));
    EXPECT_EQ(queue.size(), 4);

    int value = 0;
    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 1);

    EXPECT_TRUE(queue.push(5));
    EXPECT_FALSE(queue.push(6));

    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 2);
    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 3);
    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 4);
    EXPECT_TRUE(queue.pop(value));
    EXPECT_EQ(value, 5);
    EXPECT_TRUE(queue.empty());
}

TEST(SpscQueueTest, PushPopLoop) {
    relaymq::SpscQueue<std::string, 4> queue;

    for (int i = 0; i < 100; i++) {
        EXPECT_TRUE(queue.push("msg_" + std::to_string(i)));
        std::string value;
        EXPECT_TRUE(queue.pop(value));
        EXPECT_EQ(value, "msg_" + std::to_string(i));
    }
    EXPECT_TRUE(queue.empty());
}

TEST(SpscQueueTest, ProducerConsumer) {
    constexpr size_t total_items = 100000;
    relaymq::SpscQueue<size_t, 1024> queue;

    std::thread producer([&]() {
        for (size_t i = 0; i < total_items; i++) {
            while (!queue.push(i)) {
                std::this_thread::yield();
            }
        }
    });

    std::vector<size_t> received;
    received.reserve(total_items);

    std::thread consumer([&]() {
        size_t count = 0;
        while (count < total_items) {
            size_t value = 0;
            if (queue.pop(value)) {
                received.push_back(value);
                count++;
            } else {
                std::this_thread::yield();
            }
        }
    });

    producer.join();
    consumer.join();

    ASSERT_EQ(received.size(), total_items);
    for (size_t i = 0; i < total_items; i++) {
        EXPECT_EQ(received[i], i);
    }
}
