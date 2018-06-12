#include <string>
#include "k.h"

namespace kdb {
    class Connector;
    class Result;
    std::ostream &operator<<(std::ostream &os, const Result &result);

    enum class Type {
        MixedList = 0,
        Boolean = 1,
        GUID = 2,
        Byte = 4,
        Short = 5,
        Int = 6,
        Long = 7,
        Real = 8,
        Float = 9,
        Char = 10,
        Symbol = 11,
        Timestamp = 12,
        Month = 13,
        Date = 14,
        Datetime = 15,
        Timespan = 16,
        Minute = 17,
        Second = 18,
        Time = 19,
        Table = 98,
        Dict = 99,
        Error = -128
    };
}



class kdb::Connector {
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

class kdb::Result {
public:
    Result() = delete;
    Result(K res);
    Result(const Result &r);
    Result(Result &&r) noexcept;
    ~Result();
    Result & operator = (const Result &r);
    friend std::ostream &operator<<(std::ostream &os, const Result &result);

    /**
     * @brief Get type of the Result, see enum class Type for all types
     * 
     * @return Type 
     */
    inline Type type() const {
        if (res_) {
            return static_cast<Type>(res_->t);
        } else {
            return Type::Error;
        }
    };

    template<Type T>
    struct c_type; // { typedef void type; }; Declare but not define as we only allow certain Types

    template<Type T>
    inline typename c_type<T>::type get() const;

private:
    K res_;
};

namespace kdb {

    template<> struct Result::c_type<Type::Boolean> { typedef bool type; };
    // template<> struct Result::c_type<Type::GUID> {typedef U type; };
    template<> struct Result::c_type<Type::Byte> {typedef char type; };
    template<> struct Result::c_type<Type::Short> {typedef short type; };
    template<> struct Result::c_type<Type::Int> {typedef int type; };
    template<> struct Result::c_type<Type::Long> {typedef long long type; };
    template<> struct Result::c_type<Type::Real> {typedef float type; };
    template<> struct Result::c_type<Type::Float> {typedef double type; };
    template<> struct Result::c_type<Type::Char> {typedef char type; };
    template<> struct Result::c_type<Type::Symbol> {typedef char* type; };
    template<> struct Result::c_type<Type::Timestamp> {typedef long long type; };
    template<> struct Result::c_type<Type::Month> {typedef int type; };
    template<> struct Result::c_type<Type::Date> {typedef int type; };
    template<> struct Result::c_type<Type::Datetime> {typedef double type; };
    template<> struct Result::c_type<Type::Timespan> {typedef long long type; };
    template<> struct Result::c_type<Type::Minute> {typedef int type; };
    template<> struct Result::c_type<Type::Second> {typedef int type; };
    template<> struct Result::c_type<Type::Time> {typedef int type; };

    template<> inline bool Result::get<Type::Boolean>() const { return res_->g; }
    // template<> inline U Result::get<Type::GUID>() const { return res_->g; }
    template<> inline char Result::get<Type::Byte>() const { return res_->g; }
    template<> inline short Result::get<Type::Short>() const { return res_->h; }
    template<> inline int Result::get<Type::Int>() const { return res_->i; }
    template<> inline long long Result::get<Type::Long>() const { return res_->j; }
    template<> inline float Result::get<Type::Real>() const { return res_->e; }
    template<> inline double Result::get<Type::Float>() const { return res_->f; }
    template<> inline char Result::get<Type::Char>() const { return res_->g; }
    template<> inline char* Result::get<Type::Symbol>() const { return res_->s; }
    template<> inline long long Result::get<Type::Timestamp>() const { return res_->j; }
    template<> inline int Result::get<Type::Month>() const { return res_->i; }
    template<> inline int Result::get<Type::Date>() const { return res_->i; }
    template<> inline double Result::get<Type::Datetime>() const { return res_->f; }
    template<> inline long long Result::get<Type::Timespan>() const { return res_->j; }
    template<> inline int Result::get<Type::Minute>() const { return res_->i; }
    template<> inline int Result::get<Type::Second>() const { return res_->i; }
    template<> inline int Result::get<Type::Time>() const { return res_->i; }
}

