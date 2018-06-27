/**
 * @brief   C++ interface to connect to kdb+
 * 
 * @file    kdb_connector.h
 * @author  Cody Feng <cody.feng"AT"outlook.com>
 * @date    2018-06-27
 */

#ifndef __KDB_CONNECTOR_H__
#define __KDB_CONNECTOR_H__

#include <string>

namespace kdb {
    class Result;

    class Connector {
    public:
        ~Connector();

        /**
         * @brief Connect to a kdb+ server
         * 
         * @param host      host address
         * @param port      host port
         * @param usr_pwd   username:password
         * @param timeout   in milliseconds
         * @return true     successfully connected to host:port
         * @return false    failed to connect to host:port
         */
        bool connect(const char* host, int port, const char* usr_pwd=nullptr, int timeout=1000);

        /**
         * @brief Disconnect from kdb+ server
         */
        void disconnect();

        /**
         * @brief Send a synchronous message/command
         * 
         * @param msg 
         * @return Result 
         */
        Result sync(const char* msg);

        /**
         * @brief Send an asynchronous message/command
         * 
         * @param msg 
         */
        void async(const char* msg);

        /**
         * @brief Wait and receive from server
         * 
         * @param timeout   in milliseconds
         * @return Result 
         */
        Result receive(int timeout=1000);
        

    private:
        std::string host_;
        std::string usr_pwd_;
        int port_ = 0;
        int hdl_ = 0;
    };
}

#endif // __KDB_CONNECTOR_H__
