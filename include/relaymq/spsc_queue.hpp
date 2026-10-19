#pragma once

#include <atomic>
#include <array>
#include <cstddef>
#include <utility>

namespace relaymq {

template <typename T, size_t Capacity>
class SpscQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    static_assert(Capacity >= 2, "Capacity must be at least 2");

public:
    SpscQueue() : head_(0), tail_(0) {}
    ~SpscQueue() = default;

    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;
    SpscQueue(SpscQueue&&) = delete;
    SpscQueue& operator=(SpscQueue&&) = delete;

    bool push(const T& item) {
        const size_t current_head = head_.load(std::memory_order_relaxed);
        const size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head - current_tail == Capacity) {
            return false;
        }

        buffer_[current_head & Mask] = item;
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    bool push(T&& item) {
        const size_t current_head = head_.load(std::memory_order_relaxed);
        const size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head - current_tail == Capacity) {
            return false;
        }

        buffer_[current_head & Mask] = std::move(item);
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        const size_t current_head = head_.load(std::memory_order_acquire);

        if (current_tail == current_head) {
            return false;
        }

        item = std::move(buffer_[current_tail & Mask]);
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    size_t size() const {
        return head_.load(std::memory_order_relaxed) - tail_.load(std::memory_order_relaxed);
    }

    bool empty() const {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }

    constexpr size_t capacity() const {
        return Capacity;
    }

private:
    static constexpr size_t Mask = Capacity - 1;

    alignas(64) std::array<T, Capacity> buffer_{};
    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
};

}
