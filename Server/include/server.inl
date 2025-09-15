#pragma once

#include "server.h"

namespace server
{
	template<typename T>
	inline void Server::send_data(size_t clientId, const T& data)
	{
		auto it = _clients.find(clientId);
		if (it == _clients.end())
		{
			std::cerr << "Client ID " << clientId << " not found." << std::endl;
		}
		try
		{
			const uint8_t* buffer = reinterpret_cast<const uint8_t*>(&data);
			size_t size = sizeof(T);
			asio::write(*it->second.socket, asio::buffer(buffer, size));
		}
		catch (const asio::system_error& e)
		{
			std::cerr << "Send failed: " << e.what() << std::endl;
			it->second.socket->close();
			_clients.erase(it);
		}
	}

	template<>
	inline void Server::send_data<std::string>(size_t clientId, const std::string& data)
	{
		auto it = _clients.find(clientId);
		if (it == _clients.end())
		{
			std::cerr << "Client ID " << clientId << " not found." << std::endl;
			return;
		}
		try
		{
			asio::write(*it->second.socket, asio::buffer(data.data(), data.size()));
		}
		catch (const asio::system_error& e)
		{
			std::cerr << "Send failed: " << e.what() << std::endl;
			it->second.socket->close();
			_clients.erase(it);
		}
	}


	template<typename T>
	inline void Server::send_data_to_some(const std::vector<size_t>& clientId, const T& data)
	{
		for (size_t id : clientId)
		{
			auto it = _clients.find(id);
			if (it != _clients.end())
			{
				send_data(id, data);
			}
			else
			{
				std::cerr << "Client ID " << id << " not found." << std::endl;
			}
		}
	}

	template<typename T>
	inline void Server::send_data_to_all(const T& data)
	{
		for (const auto& [id, socket] : _clients)
		{
			send_data(id, data);
		}
	}
}