#include "coro/coro.hpp"
#include "resp/resp/all.hpp"
#include "server/DB.h"
#include "server/RedisProtoc.h"
auto main() -> int {
    rangedb::DB *db = new rangedb::DB();
    auto make_tcp_echo_server = [db](std::shared_ptr<coro::io_scheduler> scheduler) -> coro::task<void> {
        auto make_on_connection_task = [db](coro::net::tcp::client client) -> coro::task<void> {
            rangedb::Bytebuffer buffer(1024);
            resp::encoder<resp::buffer> enc;
            resp::decoder dec;
            resp::result res;
            size_t s = 5;
            std::vector<resp::buffer> buffers;
            std::string response;
            rangedb::Slice request;
            resp::buffer cmd;
            while (true) {
                // Wait for data to be available to read.
                co_await client.poll(coro::poll_op::read);
                auto [rstatus, rspan] = client.recv(buffer);
                switch (rstatus) {
                case coro::net::recv_status::ok:
                    // Make sure the client socket can be written to.
                    // std::cout << "recv data: " << std::string(rspan.data(), rspan.size()) << std::endl;
                    res = dec.decode(rspan.data(), rspan.size());
                    cmd = (res.value().array())[0].bulkstr();
                    if (cmd == "GET") {
                        resp::buffer get_key = (res.value().array())[1].bulkstr();
                        rangedb::ByteKey key((int8_t *)get_key.data(), get_key.size());
                        request.key_ = key;
                        db->Get(&request);
                        buffers = enc.encode_value(request.data_);
                    } else if ((res.value().array())[0].bulkstr() == "SET") {
                        resp::buffer set_key = (res.value().array())[1].bulkstr();
                        resp::buffer set_value = (res.value().array())[2].bulkstr();
                        rangedb::ByteKey key((int8_t *)set_key.data(), set_key.size());
                        request.key_ = key;
                        db->Put(&request);
                        buffers = enc.encode_value("OK");
                    }
                    if (buffers.size() != 0) {
                        response.clear();
                        for (auto &&buffer : buffers) {
                            response.append(buffer.data(), buffer.size());
                            // std::cout << "response: " << response << std::endl;
                        }
                    } else {
                        response = "+OK\r\n";
                    }

                    co_await client.poll(coro::poll_op::write);
                    client.send(response);
                    break;
                case coro::net::recv_status::would_block:
                    break;
                case coro::net::recv_status::closed:
                default:
                    co_return;
                }
            }
        };

        co_await scheduler->schedule();
        coro::net::tcp::server server{scheduler, coro::net::tcp::server::options{.port = 8888}};

        while (true) {
            // Wait for a new connection.
            auto pstatus = co_await server.poll();
            switch (pstatus) {
            case coro::poll_status::event: {
                auto client = server.accept();
                if (client.socket().is_valid()) {
                    scheduler->schedule(make_on_connection_task(std::move(client)));
                } // else report error or something if the socket was invalid or could not be accepted.
            } break;
            case coro::poll_status::error:
            case coro::poll_status::closed:
            case coro::poll_status::timeout:
            default:
                co_return;
            }
        }

        co_return;
    };

    std::vector<coro::task<void>> workers{};
    for (size_t i = 0; i < 8; ++i) {
        auto scheduler = coro::io_scheduler::make_shared(coro::io_scheduler::options{.execution_strategy = coro::io_scheduler::execution_strategy_t::process_tasks_inline});

        workers.push_back(make_tcp_echo_server(scheduler));
    }

    coro::sync_wait(coro::when_all(std::move(workers)));
}