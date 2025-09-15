#include "headers.h"
#include "client.h"

namespace client
{
	Client::Client(const std::string& host, unsigned short port, std::function<void()> onConnect, std::function<void()> onDisonnect)
		: _ioContext(), _endpoint(asio::ip::make_address(host), port), _socket(_ioContext)
	{
		if constexpr (CLIENT_DEBUG)
		{
			std::cout << "Client initialized. Connecting to " << host << ":" << port << std::endl;
		}
		if (onConnect != nullptr)
		{
			_onConnect = std::move(onConnect);
		}
		if (onDisonnect != nullptr)
		{
			_onDisconnect = std::move(onDisonnect);
		}
	}

	ClientError Client::connect()
	{
		_receiveBuffer.resize(1024);

		asio::error_code socketEc;
		_socket.connect(_endpoint, socketEc);
		if (socketEc)
		{
			std::cerr << "Connect failed: " << socketEc.message() << std::endl;
			return ClientError::ConnectFailed;
		}

		asio::error_code dataEc;
		uint32_t data = 0;
		_socket.receive(asio::buffer(&data, sizeof(data)), 0, dataEc);
		if (dataEc)
		{
			std::cerr << "Initial connection failed: " << dataEc.message() << std::endl;
			return ClientError::ConnectFailed;
		}
		_clientId = ntohl(data);

		if (_onConnect != nullptr)
		{
			_onConnect();
		}

		_socket.async_receive(asio::buffer(_receiveBuffer),
			[this](const asio::error_code& error, size_t bytesTransferred)
			{
				handle_receive(error, bytesTransferred);
			});


		_ioThread = std::thread([this]()
			{
				try {
					_ioContext.run();
				}
				catch (const std::exception& e)
				{
					std::cerr << "IO context error: " << e.what() << std::endl;
				}
			});
		if constexpr (CLIENT_DEBUG)
		{
			std::cout << "Client started on " << _endpoint.address().to_string() << ":" << _endpoint.port() << std::endl;
		}

		return ClientError::None;

	}

	void Client::set_on_connect(std::function<void()> callback)
	{
		_onConnect = std::move(callback);
	}

	void Client::set_on_disconnect(std::function<void()> callback)
	{
		_onDisconnect = std::move(callback);
	}

	void Client::set_on_message_received(std::function<void(const std::vector<uint8_t>&)> callback)
	{
		_onMessageReceived = std::move(callback);
	}

	void Client::stop()
	{

		if (_onDisconnect)
		{
			_onDisconnect();
		}

		_ioContext.stop();

		if (_ioThread.joinable())
		{
			_ioThread.join();
		}
		if (_socket.is_open())
		{
			_socket.close();
		}

		_receiveBuffer.clear();

		if constexpr (CLIENT_DEBUG)
		{
			std::cout << "Client stopped." << std::endl;
		}
	}

	Client::~Client()
	{
		stop();
	}

	void Client::run()
	{
		_ioThread = std::thread([this]()
			{
				try
				{
					_ioContext.run();
				}
				catch (const std::exception& e)
				{
					std::cerr << "Client IO context error: " << e.what() << std::endl;
				}
			});
	}

	void Client::handle_receive(const asio::error_code& error, size_t bytesTransferred)
	{
		if (error)
		{
			std::cerr << "Receive failed: " << error.message() << std::endl;
		}
		if (error == asio::error::eof || bytesTransferred == 0)
		{
			std::cout << "Server disconnected." << std::endl;
		}


		std::vector<uint8_t> data(_receiveBuffer.begin(), _receiveBuffer.begin() + bytesTransferred);

		_onMessageReceived(data);

		_socket.async_receive(asio::buffer(_receiveBuffer),
			[this](const asio::error_code& error, size_t bytesTransferred)
			{
				handle_receive(error, bytesTransferred);
			});
	}

}
