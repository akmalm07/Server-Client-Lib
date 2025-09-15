#pragma once

#include <asio.hpp>



namespace server
{

	class Server
	{
	public:

		Server(unsigned short port, std::function<void()> onConnect = nullptr, std::function<void()> onDisconnect = nullptr);

		void set_on_disconnect(std::function<void()> callback);

		void set_on_connect(std::function<void()> callback);

		size_t client_count() const;

		void add_on_message_received(std::function<void(const std::vector<uint8_t>&)> callback);

		void stop_accepting();

		std::vector<uint32_t> get_all_client_ids() const;

		template<typename T>
		void send_data(size_t clientId, const T& data);

		uint32_t get_last_client_sent_data() const;

#ifdef SERVER_SAVE_PREV_DATA
		std::vector<uint8_t> get_accumulated_data(size_t id);
#endif
		template<typename T>
		void send_data_to_some(const std::vector<size_t>& clientId, const T& data);

		template<typename T>
		void send_data_to_all(const T& data);

		uint32_t get_current_client_id() const;

		void disconnect_client(uint32_t clientId);

		asio::ip::tcp::endpoint get_endpoint() const;

		void stop();

		~Server();

	private:

		std::thread _ioThread;

		asio::io_context _ioContext; 
	
		asio::executor_work_guard<asio::io_context::executor_type> _workGuard;

		asio::ip::tcp::endpoint _endpoint;
		asio::ip::tcp::acceptor _acceptor; 

		struct ClientSession
		{
			std::shared_ptr<asio::ip::tcp::socket> socket;
			std::vector<uint8_t> buffer;

			ClientSession(std::shared_ptr<asio::ip::tcp::socket> sock, size_t bufferSize)
				: socket(std::move(sock)), buffer(bufferSize) {
			}

			ClientSession() = default;
		};

		std::unordered_map<uint32_t, ClientSession> _clients;

		uint32_t _lastClientSentData = 0;

		uint32_t _nextClientId = 1;

		std::function<void()> _onDisconnect = nullptr;
		std::function<void()> _onConnect = nullptr;
		std::function<void(const std::vector<uint8_t>&)> _onMessageReceived;

#ifdef SERVER_SAVE_PREV_DATA
		std::unordered_map<size_t, std::vector<uint8_t>> _accumulatedData;
#endif

		void start_accept(); 
		
		void handle_accept(std::shared_ptr<asio::ip::tcp::socket> socket, const asio::error_code& error);

		//void handle
		void start_session(uint32_t id);

		void handle_incoming_data(const asio::error_code& error, size_t byteSizeTransferred, size_t id);
	};

}

#include "server.inl" 