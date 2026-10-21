#include "relaymq/outbound_writer.hpp"
#include "relaymq/net.hpp"

namespace relaymq {

OutboundWriter::OutboundWriter() = default;

OutboundWriter::~OutboundWriter() {
    stop();
}

void OutboundWriter::start() {
    bool expected = false;
    if (is_running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        writer_thread_ = std::thread(&OutboundWriter::writer_loop, this);
    }
}

void OutboundWriter::stop() {
    bool expected = true;
    if (is_running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        cv_.notify_all();
        if (writer_thread_.joinable()) {
            writer_thread_.join();
        }
    }
}

bool OutboundWriter::enqueue(std::shared_ptr<Connection> conn, std::string data, bool close_after) {
    if (!is_running_.load(std::memory_order_relaxed)) {
        return false;
    }
    OutboundMessage msg{std::move(conn), std::move(data), close_after};
    std::lock_guard<std::mutex> lock(enqueue_mutex_);
    while (is_running_.load(std::memory_order_relaxed) && !queue_.push(std::move(msg))) {
        std::this_thread::yield();
    }
    cv_.notify_one();
    return true;
}

void OutboundWriter::flush_batch(int fd, std::string& batch) {
    if (fd < 0 || batch.empty()) {
        return;
    }
    net::send_all(fd, batch, 1000);
    batch.clear();
}

void OutboundWriter::writer_loop() {
    std::string batch;
    batch.reserve(65536);
    int current_fd = -1;

    while (is_running_.load(std::memory_order_relaxed)) {
        OutboundMessage msg;
        if (queue_.pop(msg)) {
            if (msg.conn && msg.conn->is_open()) {
                int fd = msg.conn->socket_fd();
                if (fd != current_fd || batch.size() + msg.data.size() > 65536) {
                    flush_batch(current_fd, batch);
                    current_fd = fd;
                }
                batch.append(msg.data);
                if (msg.close_after) {
                    flush_batch(current_fd, batch);
                    current_fd = -1;
                    msg.conn->close_connection();
                }
            }

            while (queue_.pop(msg)) {
                if (msg.conn && msg.conn->is_open()) {
                    int fd = msg.conn->socket_fd();
                    if (fd != current_fd || batch.size() + msg.data.size() > 65536) {
                        flush_batch(current_fd, batch);
                        current_fd = fd;
                    }
                    batch.append(msg.data);
                    if (msg.close_after) {
                        flush_batch(current_fd, batch);
                        current_fd = -1;
                        msg.conn->close_connection();
                    }
                }
            }

            flush_batch(current_fd, batch);
            current_fd = -1;
        } else {
            std::unique_lock<std::mutex> lock(cv_mutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(10), [this]() {
                return !queue_.empty() || !is_running_.load(std::memory_order_relaxed);
            });
        }
    }

    OutboundMessage msg;
    while (queue_.pop(msg)) {
        if (msg.conn && msg.conn->is_open()) {
            int fd = msg.conn->socket_fd();
            if (fd != current_fd || batch.size() + msg.data.size() > 65536) {
                flush_batch(current_fd, batch);
                current_fd = fd;
            }
            batch.append(msg.data);
            if (msg.close_after) {
                flush_batch(current_fd, batch);
                current_fd = -1;
                msg.conn->close_connection();
            }
        }
    }

    flush_batch(current_fd, batch);
}

}
