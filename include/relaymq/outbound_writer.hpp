#pragma once

#include "relaymq/connection.hpp"
#include "relaymq/spsc_queue.hpp"
#include <string>
#include <memory>
#include <thread>
#include <atomic>

namespace relaymq {

struct OutboundMessage {
    std::shared_ptr<Connection> conn;
    std::string data;
    bool close_after = false;
};

class OutboundWriter {
public:
    OutboundWriter();
    ~OutboundWriter();

    OutboundWriter(const OutboundWriter&) = delete;
    OutboundWriter& operator=(const OutboundWriter&) = delete;

    void start();
    void stop();
    bool enqueue(std::shared_ptr<Connection> conn, std::string data, bool close_after = false);

private:
    SpscQueue<OutboundMessage, 65536> queue_;
    std::thread writer_thread_;
    std::atomic<bool> is_running_{false};

    void writer_loop();
    void flush_batch(int fd, std::string& batch);
};

}
