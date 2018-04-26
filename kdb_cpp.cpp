#include <string>
#include <cstdio>
#include "k.h"

class KdbConnector {
public:
    ~KdbConnector();
    bool connect(const char* host, int port, const char* usr_pwd = nullptr, int timeout=1000);
    bool disconnect();
    K sync(const char* msg);
    void async(const char* msg);
    K receive(int timeout=1000);
    

private:
    std::string _host;     // e.g., 255.255.255.255
    std::string _usr_pwd;
    int _port = 0;
    int _hdl = 0;
};

KdbConnector::~KdbConnector() {
    disconnect();
}

bool KdbConnector::connect(const char* host, int port, const char* usr_pwd, int timeout) {
    _host = host == nullptr ? "" : host;
    _usr_pwd = usr_pwd == nullptr ? "" : usr_pwd;
    _port = port;

    // Disconnect the old connection if exists
    if (_hdl > 0) {
        disconnect();
    }

    if (timeout > 0) {
        _hdl = khpun(const_cast<const S>(_host.c_str()), _port, const_cast<const S>(_usr_pwd.c_str()), timeout);
    } else {
        _hdl = khpu(const_cast<const S>(_host.c_str()), _port, const_cast<const S>(_usr_pwd.c_str()));
    }
    fprintf(stdout, "[kdb+] IP: %s  Port: %d  Usr_pwd: %s  Timeout: %d\n", _host.c_str(), _port, _usr_pwd.c_str(), timeout);
    if (_hdl < 0) {
        fprintf(stderr, "[kdb+] Failed to connect to kdb+ server.\n");
        return false;
    } else if (0 == _hdl) {
        fprintf(stderr, "[kdb+] Wrong credential. Authentication error.\n");
        return false;
    }
    fprintf(stdout, "[kdb+] Successfully connected to %s.\n", _host.c_str());
    return true;
}

bool KdbConnector::disconnect() {
    if (_hdl > 0) {
        kclose(_hdl);
        _hdl = 0;
        fprintf(stdout, "[kdb+] Closed connection to %s.\n", _host.c_str());
    } else {
        fprintf(stdout, "[kdb+] Connection already closed.\n");
    }
}

K KdbConnector::sync(const char* msg) {
    if (_hdl <= 0) {
        fprintf(stderr, "[kdb+] Connection not established.\n");
        // TODO auto connect
    } else {
        fprintf(stdout, "[kdb+][sync] %s\n", msg);
        K res = k(_hdl, const_cast<const S>(msg), (K)0);
        if (nullptr == res) {
            fprintf(stderr, "[kdb+] Network error. Failed to communicate with server.\n");
        } else if (-128 == res->t) {
            fprintf(stderr, "[kdb+] kdb+ syntax/command error : %s\n", res->s);
            r0(res);   // Free memory if error as there is nothing useful in it
            res = nullptr;
        }
        return res;
    }
}

void KdbConnector::async(const char* msg) {
    if (_hdl <= 0) {
        fprintf(stderr, "[kdb+] Connection not established.\n");
        // TODO auto connect
    } else {
        fprintf(stdout, "[kdb+][async] %s\n", msg);
        K res = k(-_hdl, const_cast<const S>(msg), (K)0);
        if (nullptr == res) {
            fprintf(stderr, "[kdb+] Network error. Failed to communicate with server.\n");
        }
    }
}

// timeout in milliseconds
K KdbConnector::receive(int timeout) {
    K result = nullptr;
    if (_hdl <= 0) {
        fprintf(stderr, "[kdb+] Connection not established.\n");
        // TODO auto connect
    } else {
        /* Set timeout */
        int retval;
        fd_set fds;
        struct timeval tv;

        tv.tv_sec = (long) timeout / 1000;
        tv.tv_usec = (long) timeout % 1000 * 1000;
        FD_ZERO(&fds);
        FD_SET(_hdl, &fds);
        retval = select(_hdl + 1, &fds, NULL, NULL, &tv);
        if (retval == -1) {
            fprintf(stderr, "[kdb+] Connection error.\n");
            disconnect();
        }

        if (retval) {
            if (FD_ISSET(_hdl, &fds)) {
                /* Send an empty synchronous request to get the result */
                result = k(_hdl, (S)0);
            }
        } else {
            fprintf(stderr, "[kdb+] Error: no data within %d ms.\n", timeout);
        }
    }

    return result;
}

int main() {
    KdbConnector k;
    k.connect("127.0.0.1", 5000);
    k.sync("1+1`");
    k.sync("1+1");
    k.sync("a:1");
    k.sync("a");
    k.async("a:2");
    k.sync("a");
    k.async("(neg .z.w) 999");
    k.receive();

    k.receive(); // Wait for non-existing message

    k.disconnect();
    k.async("(neg .z.w) 999");
    k.receive();

    
    return 0;
}

