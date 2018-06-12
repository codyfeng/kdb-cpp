#include <iostream>
#include <cstdio>
#include "kdb_cpp.h"


////////////////////////////////////////////
// Definition of kdb+ Connector
////////////////////////////////////////////
kdb::Connector::~Connector() {
    disconnect();
}

bool kdb::Connector::connect(const char* host, int port, const char* usr_pwd, int timeout) {
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
    fprintf(stdout, "[kdb+] IP: %s  Port: %d  Usr_pwd: %s  Timeout: %d\n", host_.c_str(), port_, usr_pwd_.c_str(), timeout);
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

void kdb::Connector::disconnect() {
    if (hdl_ > 0) {
        kclose(hdl_);
        hdl_ = 0;
        fprintf(stdout, "[kdb+] Closed connection to %s.\n", host_.c_str());
    } else {
        fprintf(stdout, "[kdb+] Connection already closed.\n");
    }
}

kdb::Result kdb::Connector::sync(const char* msg) {
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
        return Result(res);
    }
}

void kdb::Connector::async(const char* msg) {
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
kdb::Result kdb::Connector::receive(int timeout) {
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

    return Result(result);
}

////////////////////////////////////////////
// Definition of kdb+ Result
////////////////////////////////////////////
kdb::Result::Result(K res) {
    res_ = res;
}

kdb::Result::~Result() {
    if (res_) {
        r0(res_);   // Reduce reference count
        res_ = nullptr; // Avoid double free
    }
}

// Copy constructor, e.g., Result r1 = r2;
kdb::Result::Result(const kdb::Result &r) {
    res_ = r.res_;
    if (res_) {
        r1(res_);  // Increase reference count
    }
}

// Move constructor
kdb::Result::Result(kdb::Result &&r) noexcept {
    res_ = r.res_;
    r.res_ = nullptr;
}


// Assignment operator, e.g., r1 = r2;
kdb::Result & kdb::Result::operator = (const kdb::Result &r) {
    if (this != &r) {
        if (res_) {
            r0(res_);  // Reduce reference count
        }
        res_ = r.res_;
        if (res_) {
            r1(res_);  // Increase reference count
        }
    }
    return *this;
}

std::ostream &operator<<(std::ostream &os, K const &res) {
    if (res) {
        int idx;
        switch (res->t) {
        case(-1) : // Boolean atom
            if (res->g) {
                os << "true";
            } else {
                os << "false";
            }
            break;
        case(-4) : // Byte atom
            os << res->g; break;
        case(-5) : // Short atom
            os << res->h; break;
        case(-6) : case(-13) : case(-17) : case(-18) : // Int Month Minute Second atom
            os << res->i; break;
        case(-7) : case(-12) : case(-16) : // Long Timestamp Timespan atom
            os << res->j; break;
        case(-8) : // Real atom
            os << res->e; break;
        case(-9) : case(-15) : // Float Datetime atom
            os << res->f; break;
        case(-10) : // Char atom
            os << res->g; break;
        case(-11) : // Symbol atom
            os << res->s; break;
        case(-19) : // Time atom
            os << res->i; break;
        case(-14) : // Date atom
            os << dj(res->i); break;
        case(0) : // Mixed List
            for (idx = 0; idx < res->n; idx++) {
                os << kK(res)[idx]  << ' ';
            }
            break;
        case(1) : // Boolean vector
            for (idx = 0; idx < res->n; idx++) {
                if (kG(res)[idx]) {
                    os << "true";
                } else {
                    os << "false";
                }
                os << ' ';
            }
            break;
        case(4) : // Byte vector
            for (idx = 0; idx < res->n; idx++) {
                os << kG(res)[idx] << ' ';
            }
            break;
        case(5) : // Short vector
            for (idx = 0; idx < res->n; idx++) {
                os << kH(res)[idx] << ' ';
            }
            break;
        case(6) : case(13) : case(17) : case(18) : // Int Month Minute Second vector
            for (idx = 0; idx < res->n; idx++) {
                os << kI(res)[idx] << ' ';
            }
            break;
        case(7) : case(12) : case(16) : // Long Timestamp Timespan vector
            for (idx = 0; idx < res->n; idx++) {
                os << kJ(res)[idx] << ' ';
            }
            break;
        case(8) : // Real vector
            for (idx = 0; idx < res->n; idx++) {
                os << kE(res)[idx] << ' ';
            }
            break;
        case(9) : case(15) : // Float Datetime vector
            for (idx = 0; idx < res->n; idx++) {
                os << kF(res)[idx] << ' ';
            }
            break;
        case(10) : // Char vector, i.e., String
            for (idx = 0; idx < res->n; idx++) {
                os << kC(res)[idx];
            }
            break;
        case(11) : // Symbol vector
            for (idx = 0; idx < res->n; idx++) {
                os << kS(res)[idx] << ' ';
            }
            break;
        case(19) : // Time vector
            for (idx = 0; idx < res->n; idx++) {
                os << kI(res)[idx] << ' ';
            }
            break;
        case(14) : // Date vector
            for (idx = 0; idx < res->n; idx++) {
                os << dj(kI(res)[idx]) << ' ';
            }
            break;
        case(98) : // Non-keyed Table
            os << kK(res->k)[0] << kK(res->k)[1];
            break;
        case(99) : // Dictionary or keyed table
            os << kK(res)[0] << kK(res)[1];
            break;
        }
    }
    return os;
}

std::ostream &kdb::operator<<(std::ostream &os, kdb::Result const &result) {
    return os << result.res_;
}

