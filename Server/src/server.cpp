#include "headers.h"
#include "server.h"

namespace server
{
	Server::Server(unsigned short port, std::function<void()> onConnect, std::function<void()> onDisconnect)
		: _ioContext(),
		_workGuard(asio::make_work_guard(_ioContext)),
		_endpoint(asio::ip::tcp::v4(), port),
		_acceptor(_ioContext, _endpoint)
	{
		if (onConnect) _onConnect = std::move(onConnect);
		if (onDisconnect) _onDisconnect = std::move(onDisconnect);

		start_accept();

		_ioThread = std::thread([this]()
			{
				try {
					_ioContext.run();
				}
				catch (const std::exception& e)
				{
					if constexpr (SERVER_DEBUG)
					{
						std::cerr << "IO context error: " << e.what() << std::endl;
					}
				}
			});

		if constexpr (SERVER_DEBUG)
		{
			std::cout << "Server started on " << _endpoint.address().to_string() << ":" << _endpoint.port() << std::endl; \
		}
	}

	void Server::set_on_disconnect(std::function<void()> callback)
	{
		_onDisconnect = std::move(callback);
	}

	void Server::set_on_connect(std::function<void()> callback)
	{
		_onConnect = std::move(callback);
	}

	size_t Server::client_count() const
	{
		return _clients.size();
	}

	void Server::add_on_message_received(std::function<void(const std::vector<uint8_t>&)> callback)
	{
		_onMessageReceived = std::move(callback);
	}

	void Server::stop_accepting()
	{
		_acceptor.close();
		if constexpr (SERVER_DEBUG)
		{
			std::cout << "Server stopped accepting new connections." << std::endl;
		}
	}

	std::vector<uint32_t> Server::get_all_client_ids() const
	{
		std::vector<uint32_t> ids;
		ids.reserve(_clients.size());
		for (const auto& [id, _] : _clients)
		{
			ids.push_back(id);
		}
		return ids;
	}

	uint32_t Server::get_last_client_sent_data() const
	{
		return _lastClientSentData;
	}

#ifdef SERVER_SAVE_PREV_DATA


	std::vector<uint8_t> Server::get_accumulated_data(size_t id)
	{
		auto it = _clients.find(id);
		if (it == _clients.end())
		{
			if constexpr (SERVER_DEBUG)
			{
				std::cerr << "Client ID " << id << " not found." << std::endl;
			}
			return {};
		}
		const ClientSession& client = it->second;
		if constexpr (SERVER_DEBUG)
		{
			std::cout << "Accumulated data for client " << id << ": ";

			const auto& data = _accumulatedData.at(id);
			for (const auto& byte : data)
			{
				std::cout << static_cast<uint8_t>(byte) << " ";
			}
			std::cout << std::endl;
		}

		return _accumulatedData.at(id);
	}
#endif

	uint32_t Server::get_current_client_id() const
	{
		return _nextClientId;
	}

	void Server::disconnect_client(uint32_t clientId)
	{
		auto it = _clients.find(clientId);
		if (it != _clients.end())
		{
			if (it->second.socket->is_open())
			{
				it->second.socket->close();
			}
			_clients.erase(it);
			if constexpr (SERVER_DEBUG)
			{
				std::cout << "Client " << clientId << " disconnected by server." << std::endl;
			}
		}
		else
		{
			if constexpr (SERVER_DEBUG)
			{
				std::cerr << "Client ID " << clientId << " not found." << std::endl;
			}
		}
	}

	asio::ip::tcp::endpoint Server::get_endpoint() const
	{
		return _endpoint;
	}

	void Server::stop()
	{
		_acceptor.close();
		_ioContext.stop();

		for (auto& [id, socket] : _clients)
		{
			if (socket.socket->is_open())
				socket.socket->close();
		}

		if (_ioThread.joinable())
			_ioThread.join();

		_clients.clear();

		if constexpr (SERVER_DEBUG)
		{
			std::cout << "Server stopped." << std::endl;
		}
	}

	Server::~Server()
	{
		stop();
	}

	void Server::start_accept()
	{
		auto socket = std::make_shared<asio::ip::tcp::socket>(_ioContext);

		_acceptor.async_accept(*socket, [this, socket](const asio::error_code& error)
			{
				handle_accept(socket, error);
			});
	}

	void Server::handle_accept(std::shared_ptr<asio::ip::tcp::socket> socket, const asio::error_code& error)
	{
		if (error)
		{
			if constexpr (SERVER_DEBUG)
			{
				std::cerr << "Accept failed: " << error.message() << std::endl;
			}
		}

		uint32_t clientId = ++_nextClientId;

		if constexpr (SERVER_DEBUG)
		{
			std::cout << "Client connected: " << socket->remote_endpoint() << std::endl;
		}

		_clients[clientId] = std::move(ClientSession{ socket, 1024 });

#ifdef SERVER_SAVE_PREV_DATA
		_accumulatedData[clientId] = std::vector<uint8_t>();
#endif


		start_session(clientId);


		if (_onConnect)
			_onConnect();

		if (_acceptor.is_open())
			start_accept();
	}

	void Server::start_session(uint32_t id)
	{
		if (_clients.find(id) == _clients.end())
		{
			std::cerr << "Client ID " << id << " not found." << std::endl;
			return;
		}
		ClientSession& client = _clients[id];

		if constexpr (SERVER_DEBUG)
		{
			std::cout << "Starting session with client: " << client.socket->remote_endpoint() << std::endl;
		}

		uint32_t netId = htonl(id);
		asio::write(*client.socket, asio::buffer(&netId, sizeof(netId)));

		client.socket->async_read_some(
			asio::buffer(client.buffer),
			[this, id](const asio::error_code& error, std::size_t bytesTransferred)
			{
				handle_incoming_data(error, bytesTransferred, id);
			});
	}

	void Server::handle_incoming_data(const asio::error_code& error, size_t byteSizeTransferred, size_t id)
	{
		if (_clients.find(id) == _clients.end())
		{
			if constexpr (SERVER_DEBUG)
			{
				std::cerr << "Client ID " << id << " not found." << std::endl;
			}
			return;
		}

		if (error || byteSizeTransferred == 0)
		{
			if (error == asio::error::eof)
			{
				if constexpr (SERVER_DEBUG)
				{
					std::cout << "Client " << id << " disconnected." << std::endl;
				}
			}
			else if (error == asio::error::operation_aborted || error == asio::error::connection_reset)
			{
				if constexpr (SERVER_DEBUG)
				{
					std::cout << "Client " << id << " disconnected ungracfully." << std::endl;
				}
			}
			else
			{
				if constexpr (SERVER_DEBUG)
				{
					std::cerr << "Error receiving data from client " << id << ": " << error.message() << std::endl;

				}
			}

			if (_onDisconnect)
				_onDisconnect();

			_clients.erase(id);

			_nextClientId--;

			if constexpr (SERVER_DEBUG)
			{
				std::cout << "Session with client " << id << " ended." << std::endl;
			}
			return;
		}

		ClientSession& client = _clients[id];

		if (byteSizeTransferred > client.buffer.size())
		{
			if constexpr (SERVER_DEBUG)
			{
				std::cerr << "Received data size exceeds buffer capacity for client " << id << ". Resizing buffer." << std::endl;
			}
			client.buffer.resize(byteSizeTransferred);
		}

		_lastClientSentData = id;

		std::vector<uint8_t> data(client.buffer.begin(), client.buffer.begin() + byteSizeTransferred);

#ifdef SERVER_SAVE_PREV_DATA
		_accumulatedData[id].insert(_accumulatedData[id].end(), data.begin(), data.end());
#endif

		_onMessageReceived(data);

		client.socket->async_read_some(
			asio::buffer(client.buffer),
			[this, id](const asio::error_code& err, size_t transferred)
			{
				handle_incoming_data(err, transferred, id);
			});
	}
}
