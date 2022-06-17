
#include <iostream>
#include <tuple>
#include <utility>
#include <vector>

#include <folly/experimental/coro/BlockingWait.h>
#include <folly/experimental/coro/BoundedQueue.h>
#include <folly/experimental/coro/Collect.h>
#include <folly/experimental/coro/detail/Barrier.h>
#include <folly/experimental/coro/detail/BarrierTask.h>
#include <spdlog/spdlog.h>
#include <boost/asio/buffers_iterator.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>

#include "utils.h"

class WebSocket final {
public:
	using WebsocketStream = boost::beast::websocket::stream<boost::beast::tcp_stream>;
	using RequestParser = boost::beast::http::request_parser<boost::beast::http::string_body>;

	WebSocket(IoContextPool& pool)
		: m_pool(pool) {
		m_acceptor_ptr = std::make_shared<boost::asio::ip::tcp::acceptor>(m_pool.getIoContext());
		boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 8848));
		m_acceptor_ptr->open(endpoint.protocol());
		m_acceptor_ptr->set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
		m_acceptor_ptr->bind(endpoint);
		boost::beast::error_code ec;
		m_acceptor_ptr->listen(boost::asio::socket_base::max_listen_connections, ec);
		if (ec) {
			spdlog::error("{}", ec.message());
		}
	}

	folly::coro::Task<std::optional<std::pair<std::shared_ptr<WebsocketStream>, std::shared_ptr<RequestParser>>>> isWebsocket(std::shared_ptr<Executor> ex, boost::asio::ip::tcp::socket sock) {
		auto bad_request = [](auto&& req, boost::beast::string_view why) {
			boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::bad_request, req.version()};
			res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
			res.set(boost::beast::http::field::content_type, "text/html");
			res.keep_alive(req.keep_alive());
			res.body() = std::string(why);
			res.prepare_payload();
			return res;
		};
		boost::beast::flat_buffer buffer_;
		boost::beast::tcp_stream stream_{std::move(sock)};
		auto parser_ = std::make_shared<RequestParser>();
		parser_->body_limit(10000);
		auto [http_ec, http_count] = co_await async_read_http(stream_, buffer_, *parser_);
		if (http_ec == boost::beast::http::error::end_of_stream) {
			boost::beast::error_code ec;
			stream_.socket().shutdown(boost::asio::ip::tcp::socket::shutdown_send, ec);
			co_return std::nullopt;
		}
		if (!boost::beast::websocket::is_upgrade(parser_->get())) {
			spdlog::error("not websocket");
			auto rsp = bad_request(parser_->release(), "");
			folly::coro::detail::Barrier barrier_{1};
			boost::beast::http::async_write(stream_, rsp, [&](boost::beast::error_code ec, std::size_t bytes_transferred) {
				if (ec)
					spdlog::error("async_write: {}", ec.message());
				barrier_.remaining();
			});
			co_await barrier_.arriveAndWait();
			co_return std::nullopt;
		}

		// ------------------------------------------------------------------
		auto path = parser_->get().target();
		spdlog::info("request path: {}");
		// ------------------------------------------------------------------

		auto ws_ptr = std::make_shared<WebsocketStream>(stream_.release_socket());
		ws_ptr->set_option(boost::beast::websocket::stream_base::timeout::suggested(boost::beast::role_type::server));
		ws_ptr->set_option(boost::beast::websocket::stream_base::decorator([](boost::beast::websocket::response_type& res) {
			res.set(boost::beast::http::field::server, std::string(BOOST_BEAST_VERSION_STRING) + " websocket-server-async");
		}));
		co_return std::make_optional(std::make_pair(std::move(ws_ptr), parser_));
	}

	folly::coro::Task<void> session(std::shared_ptr<Executor> ex, std::shared_ptr<WebsocketStream> ws_ptr, std::shared_ptr<RequestParser> parser_) {
		auto ec = co_await async_accept_ws(*ws_ptr, parser_->release());
		if (ec) {
			spdlog::error("[async_accept_ws]: {}", ec.message());
			co_return;
		}
		auto bounded_queue_ptr = std::make_shared<folly::coro::BoundedQueue<std::string>>(5);

		auto consumer = [bounded_queue_ptr, ws_ptr, ex]() -> folly::coro::Task<void> {
			while (true) {
				auto str = co_await bounded_queue_ptr->dequeue();
				if (!ws_ptr->is_open())
					co_return;
				ws_ptr->text(ws_ptr->got_text());
				auto [ec, _] = co_await async_write_ws(*ws_ptr, boost::asio::buffer(str.c_str(), str.size()));
				if (ec) {
					spdlog::error("make_task: {}", ec.message());
					co_return;
				}
			}
			co_return;
		};
		folly::coro::co_invoke(std::move(consumer)).scheduleOn(ex.get()).start();

		auto send_pong = [ex, ws_ptr, bounded_queue_ptr]() -> folly::coro::Task<void> {
			boost::asio::steady_timer steady_timer{ex->m_io_context};
			while (true) {
				if (!ws_ptr->is_open())
					co_return;
				ws_ptr->async_pong("async_pong", [=](boost::beast::error_code ec) {
					if (ec)
						spdlog::error("async_pong ec: {}", ec.message());
				});
				steady_timer.expires_after(std::chrono::seconds(2));
				co_await timeout(steady_timer);
			}
		};
		folly::coro::co_invoke(std::move(send_pong)).scheduleOn(ex.get()).start();

		auto producer = [ex, ws_ptr, bounded_queue_ptr]() -> folly::coro::Task<void> {
			int i = 0;
			boost::asio::steady_timer steady_timer{ex->m_io_context};
			while (true) {
				if (!ws_ptr->is_open())
					co_return;
				co_await bounded_queue_ptr->enqueue(std::to_string(i++));
				steady_timer.expires_after(std::chrono::seconds(2));
				co_await timeout(steady_timer);
			}
		};
		folly::coro::co_invoke(std::move(producer)).scheduleOn(ex.get()).start();

		boost::beast::flat_buffer buffer_;
		while (true) {
			buffer_.consume(buffer_.size());
			std::cout << std::this_thread::get_id() << std::endl;
			// boost::beast::get_lowest_layer(*ws_ptr).expires_after(std::chrono::seconds(3));
			auto [ec, count] = co_await async_read_ws(*ws_ptr, buffer_);
			if (ec == boost::beast::websocket::error::closed) {
				spdlog::error("boost::beast::websocket::error::closed!!!");
				co_await bounded_queue_ptr->enqueue(std::to_string(2));
				co_return;
			}
			if (ec) {
				spdlog::error("---error: {}", ec.message());
				co_await bounded_queue_ptr->enqueue(std::to_string(2));
				co_return;
			}
			if (ws_ptr->got_text())
				spdlog::info("text message");
			std::string text(boost::asio::buffers_begin(buffer_.data()), boost::asio::buffers_end(buffer_.data()));
			spdlog::info("Data read:   {}", text);
		}
		co_return;
	}

	folly::coro::Task<void> start() {
		for (;;) {
			auto& context = m_pool.getIoContext();
			boost::asio::ip::tcp::socket socket(context);
			auto error = co_await async_accept(*m_acceptor_ptr, socket);
			if (error) {
				spdlog::error("Accept failed, error: {}", error.message());
				co_return;
			}
			auto ex = std::make_shared<Executor>(context);
			auto task = [this, ex, socket = std::move(socket)]() mutable -> folly::coro::Task<void> {
				auto opt = co_await isWebsocket(ex, std::move(socket));
				if (!opt)
					co_return;
				auto& [stream, parse] = *opt;
				{
					auto lock = co_await m.co_scoped_lock();
					m_ws_vec.emplace_back(stream);
				}
				co_await session(ex, std::move(stream), std::move(parse));
				co_return;
			};
			folly::coro::co_invoke(std::move(task)).scheduleOn(ex.get()).start();
		}
		co_return;
	}

	void close() {
		folly::coro::blockingWait([&]() -> folly::coro::Task<void> {
			std::vector<std::shared_ptr<folly::coro::detail::Barrier>> barrier_vec;
			{
				auto lock = co_await m.co_scoped_lock();
				for (auto& a : m_ws_vec) {
					if (auto sp = a.lock()) {
						auto barrier_ = std::make_shared<folly::coro::detail::Barrier>(1);
						sp->async_close(boost::beast::websocket::close_code::normal,
							[=](boost::beast::error_code ec) {
								spdlog::error("async_close ec: {}", ec.message());
								barrier_->remaining();
							});
						barrier_vec.emplace_back(std::move(barrier_));
					}
				}
			}
			for (auto& barrier_ : barrier_vec)
				co_await barrier_->arriveAndWait();
		}());
		sleep(5);
		m_acceptor_ptr->close();
	}

private:
	std::shared_ptr<boost::asio::ip::tcp::acceptor> m_acceptor_ptr;
	folly::coro::Mutex m;
	std::vector<std::weak_ptr<WebsocketStream>> m_ws_vec;
	IoContextPool& m_pool;
};

int main() {
	try {
		IoContextPool pool(2);
		std::jthread thd([&] { pool.start(); });
		WebSocket server(pool);
		std::jthread jj([&] {
			sleep(10);
			server.close();
		});
		folly::coro::blockingWait(server.start());
		pool.stop();
	} catch (std::exception& e) {
		spdlog::error("Exception: {}", e.what());
	}
	return 0;
}