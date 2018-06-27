/**
 * @brief   C++ interface to connect to kdb+
 * 
 * @file    kdb_connector.cpp
 * @author  Cody Feng <cody.feng"AT"outlook.com>
 * @date    2018-06-27
 */

#ifndef KXVER
#define KXVER 3
#endif

#include "../external/k.h"
#include "kdb_result.h"
#include "kdb_connector.h"

namespace kdb {

    Connector::~Connector() {
        disconnect();
    }

    bool Connector::connect(const char* host, int port, const char* usr_pwd, int timeout) {
        host_ = host == nullptr ? "" : host;
        usr_pwd_ = usr_pwd == nullptr ? "" : usr_pwd;
        port_ = port;

        // Disconnect the old connection if exists
        if (hdl_ > 0) {
            disconnect();
        }

        if (timeout > 0) {
            hdl_ = khpun(const_cast<const S>(host_.c_str()), port_, const_cast<const S>(usr_pwd_.c_str()), timeout);
        } else {
            hdl_ = khpu(const_cast<const S>(host_.c_str()), port_, const_cast<const S>(usr_pwd_.c_str()));
        }
        fprintf(stdout, "[kdb+] Host IP: %s  Port: %d  Timeout: %d\n", host_.c_str(), port_, timeout);
        if (hdl_ < 0) {
            fprintf(stderr, "[kdb+] Failed to connect to kdb+ server.\n");
            return false;
        } else if (0 == hdl_) {
            fprintf(stderr, "[kdb+] Wrong credential. Authentication error.\n");
            return false;
        }
        fprintf(stdout, "[kdb+] Successfully connected to %s.\n", host_.c_str());
        return true;
    }

    void Connector::disconnect() {
        if (hdl_ > 0) {
            kclose(hdl_);
            hdl_ = 0;
            fprintf(stdout, "[kdb+] Closed connection to %s.\n", host_.c_str());
        } else {
            fprintf(stdout, "[kdb+] Connection already closed.\n");
        }
    }

    Result Connector::sync(const char* msg) {
        if (hdl_ <= 0) {
            fprintf(stderr, "[kdb+] Connection not established.\n");
            // TODO auto connect
            return Result(nullptr);
        } else {
            fprintf(stdout, "[kdb+][sync] %s\n", msg);
            K res = k(hdl_, const_cast<const S>(msg), (K)0);
            if (nullptr == res) {
                fprintf(stderr, "[kdb+] Network error. Failed to communicate with server.\n");
            } else if (-128 == res->t) {
                fprintf(stderr, "[kdb+] kdb+ syntax/command error : %s\n", res->s);
                r0(res);   // Free memory if error as there is nothing useful in it
                res = nullptr;
            }
            return Result(res, false);
        }
    }

    void Connector::async(const char* msg) {
        if (hdl_ <= 0) {
            fprintf(stderr, "[kdb+] Connection not established.\n");
            // TODO auto connect
        } else {
            fprintf(stdout, "[kdb+][async] %s\n", msg);
            K res = k(-hdl_, const_cast<const S>(msg), (K)0);
            if (nullptr == res) {
                fprintf(stderr, "[kdb+] Network error. Failed to communicate with server.\n");
            }
        }
    }

    // timeout in milliseconds
    Result Connector::receive(int timeout) {
        K result = nullptr;
        if (hdl_ <= 0) {
            fprintf(stderr, "[kdb+] Connection not established.\n");
            // TODO auto connect
        } else {
            // Set timeout
            int retval;
            fd_set fds;
            struct timeval tv;

            tv.tv_sec = (long) timeout / 1000;
            tv.tv_usec = (long) timeout % 1000 * 1000;
            FD_ZERO(&fds);
            FD_SET(hdl_, &fds);
            retval = select(hdl_ + 1, &fds, NULL, NULL, &tv);
            if (retval == -1) {
                fprintf(stderr, "[kdb+] Connection error.\n");
                disconnect();
            }

            if (retval) {
                if (FD_ISSET(hdl_, &fds)) {
                    // Send an empty synchronous request to get the result
                    result = k(hdl_, (S)0);
                }
            } else {
                fprintf(stderr, "[kdb+] Error: no data within %d ms.\n", timeout);
            }
        }

        return Result(result, false);
    }

}
