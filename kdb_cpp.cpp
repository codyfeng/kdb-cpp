#include <iostream>
#include <string>
#include <cstdio>
#include "k.h"

namespace kdb {
    class Connector;
    class Result;
    std::ostream &operator<<(std::ostream &os, const Result &result);
}



class kdb::Connector {
public:
    ~Connector();
    int connect(const char* host, int port, const char* usr_pwd = nullptr, int timeout=1000);
    int disconnect();
    Result sync(const char* msg);
    void async(const char* msg);
    Result receive(int timeout=1000);
    

private:
    std::string host_;
    std::string usr_pwd_;
    int port_ = 0;
    int hdl_ = 0;
};

class kdb::Result {
public:
    Result() = delete;
    Result(K res);
    Result(const Result &r);
    ~Result();
    int type();
    Result & operator = (const Result &r);
    friend std::ostream &operator<<(std::ostream &os, const Result &result);

private:
    K res_;
};


////////////////////////////////////////////
// Definition of kdb+ Connector
////////////////////////////////////////////
kdb::Connector::~Connector() {
    disconnect();
}

int kdb::Connector::connect(const char* host, int port, const char* usr_pwd, int timeout) {
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

int kdb::Connector::disconnect() {
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

int kdb::Result::type() {
    if (res_) {
        return res_->t;
    } else {
        return -128;
    }
}

std::ostream &operator<<(std::ostream &os, K const &res) {
    if (res) {
        int idx;
        switch (res->t) {
        case(-1) :
            if (res->g) {
                os << "true";
            } else {
                os << "false";
            }
            break;
        case(-4) :
            os << res->g; break;
        case(-5) :
            os << res->h; break;
        case(-6) : case(-13) : case(-17) : case(-18) :
            os << res->i; break;
        case(-7) : case(-12) : case(-16) :
            os << res->j; break;
        case(-8) :
            os << res->e; break;
        case(-9) : case(-15) :
            os << res->f; break;
        case(-10) :
            os << res->g; break;
        case(-11) :
            os << res->s; break;
        case(-19) :
            os << res->i; break;
        case(-14) :
            os << dj(res->i); break;
        case(0) :
            for (idx = 0; idx < res->n; idx++) {
                os << kK(res)[idx];
            }
            break;
        case(1) :
            for (idx = 0; idx < res->n; idx++) {
                if (kG(res)[idx]) {
                    os << "true";
                } else {
                    os << "false";
                }
            }
            break;
        case(4) :
            for (idx = 0; idx < res->n; idx++) {
                os << kG(res)[idx];
            }
            break;
        case(5) :
            for (idx = 0; idx < res->n; idx++) {
                os << kH(res)[idx];
            }
            break;
        case(6) : case(13) : case(17) : case(18) :
            for (idx = 0; idx < res->n; idx++) {
                os << kI(res)[idx];
            }
            break;
        case(7) : case(12) : case(16) :
            for (idx = 0; idx < res->n; idx++) {
                os << kJ(res)[idx];
            }
            break;
        case(8) :
            for (idx = 0; idx < res->n; idx++) {
                os << kE(res)[idx];
            }
            break;
        case(9) : case(15) :
            for (idx = 0; idx < res->n; idx++) {
                os << kF(res)[idx];
            }
            break;
        case(10) :
            for (idx = 0; idx < res->n; idx++) {
                os << kC(res)[idx];
            }
            break;
        case(11) :
            for (idx = 0; idx < res->n; idx++) {
                os << kS(res)[idx];
            }
            break;
        case(19) :
            for (idx = 0; idx < res->n; idx++) {
                os << kI(res)[idx];
            }
            break;
        case(14) :
            for (idx = 0; idx < res->n; idx++) {
                os << dj(kI(res)[idx]);
            }
            break;
        case(98) : // Non-keyed Table
            
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

////////////////////////////////////////////
// Test
////////////////////////////////////////////

inline void print_kdb(kdb::Result r) {
    std::cout << "type: " << r.type() << " value: " << r << '\n';
}

int main() {
    kdb::Connector k;
    k.connect("127.0.0.1", 5000);
    kdb::Result res = k.sync("1+1");
    print_kdb(res);

    res = k.sync("1+1`");
    print_kdb(res);

    res = k.sync("a:1");
    print_kdb(res);

    kdb::Result res1 = k.sync("a");
    print_kdb(res1);

    k.async("a:2");
    res = k.sync("a");
    print_kdb(res);

    kdb::Result res2 = res;
    print_kdb(res2);

    k.async("(neg .z.w) 999");
    res = k.receive(); // Receive 999
    print_kdb(res);

    print_kdb(res2);

    res = k.receive(); // Wait for non-existing message
    print_kdb(res);

    k.disconnect();
    k.async("(neg .z.w) 999");
    res = k.receive();
    print_kdb(res);


    
    k.connect("127.0.0.1", 5000);
    
    // Test atoms
    res = k.sync("1b");  print_kdb(res);
    res = k.sync("0x37"); print_kdb(res);
    res = k.sync("10h"); print_kdb(res);
    res = k.sync("11i"); print_kdb(res);
    res = k.sync("12j"); print_kdb(res);
    res = k.sync("13.1e"); print_kdb(res);
    res = k.sync("14.2f"); print_kdb(res);
    res = k.sync("\"a\""); print_kdb(res);
    res = k.sync("`sym"); print_kdb(res);
    res = k.sync("2016.01.01D10:00:00.000000000"); print_kdb(res);
    res = k.sync("2016.01m"); print_kdb(res);
    res = k.sync("2016.01.01"); print_kdb(res);

    // Test vectors
    res = k.sync("10110011b");  print_kdb(res);
    res = k.sync("0x3738"); print_kdb(res);
    res = k.sync("10 11h"); print_kdb(res);
    res = k.sync("11 12i"); print_kdb(res);
    res = k.sync("12 13j"); print_kdb(res);
    res = k.sync("13.1 14.1e"); print_kdb(res);
    res = k.sync("14.2 15.2f"); print_kdb(res);
    res = k.sync("\"ab\""); print_kdb(res);
    res = k.sync("`sym1`sym2"); print_kdb(res);
    res = k.sync("2016.01.01D10:00:00.000000000 2016.01.02D10:00:00.000000000"); print_kdb(res);
    res = k.sync("2016.01 2016.02m"); print_kdb(res);
    res = k.sync("2016.01.01 2016.01.02"); print_kdb(res);

    // Test mixed lists
    res = k.sync("(1b; 0x37; 10h; 11i; 12j; 13.1e; 14.2f; \"a\"; `sym)"); print_kdb(res);

    // Test dictionaries
    res = k.sync("`a`b`c!1 2 3"); print_kdb(res);

    // Test tables
    res = k.sync("([]a:1 2 3;b:1.1 2.2 3.3f;c:`first`second`third)"); print_kdb(res);
    //res = k.sync("([k:`a`b`c]a:1 2 3;b:1.1 2.2 3.3f;c:`first`second`third)"); print_kdb(res);

    return 0;
}

