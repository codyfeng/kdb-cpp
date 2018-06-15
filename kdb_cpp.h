#include <string>
#include "k.h"

namespace kdb {
    enum class Type;
    enum class StructType;
    class Connector;
    class Result;
    template<Type> class Vector;

    /**
     * @brief Mapping from kdb type to c type.
     *        Declare but not define as we only allow certain Types
     *        e.g., template<> struct c_type<Type::Long> {typedef long long type; };
     * 
     * @tparam T 
     */
    template<Type T> struct c_type;

    std::ostream &operator<<(std::ostream &os, const Result &result);

    
}

enum class kdb::Type {
    List = 0,

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

/**
 * @brief   data strucuture type
 * @see     https://code.kx.com/q/ref/elements/#nouns
 */
enum class kdb::StructType {
    Unknown, Atom, Vector, List, Dictionary, Table, KeyedTable
};

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
            return static_cast<Type>(res_->t < 0 ? -res_->t : res_->t);
        } else {
            return Type::Error;
        }
    }

    inline StructType struct_type() const {
        if (res_) {
            if (res_->t < 0) {
                return StructType::Atom;
            } else if (res_->t == 0) {
                return StructType::List;
            } else if (res_->t < 20) {
                return StructType::Vector;
            } else if (res_->t == 98) {
                return StructType::Table;
            } else if (res_->t == 99) {
                return StructType::Dictionary; // TODO identify KeyedTable
            }
        }

        return StructType::Unknown;
    }

    inline long long size() const {
        if (res_->t >= 0 && res_->t < 20) {
            // vector and mixed list
            return static_cast<long long>(res_->n);
        } else {
            // atom, table and dict
            return 1;
        }
    }

    template<Type>
    friend class kdb::Vector;

    template<Type T, typename U = typename c_type<T>::type>
    inline U get() const;

    template<Type T>
    inline kdb::Vector<T> get_vector() const {
        return kdb::Vector<T>(*this);
    }

private:
    K res_;
};


template<kdb::Type T>
class kdb::Vector {
public:
    Vector(const Result &r) : res_(r.res_), size_(r.size()) {
        if (res_) {
            r1(res_);  // Increase reference count
        }
    }
    ~Vector() {
        if (res_) {
            r0(res_);   // Reduce reference count
            res_ = nullptr; // Avoid double free
        }
    }

    typedef typename kdb::c_type<T>::type * iterator;
    typedef const typename kdb::c_type<T>::type * const_iterator;
    typedef std::reverse_iterator<iterator> reverse_iterator;
    typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

    iterator begin() { return reinterpret_cast<iterator>(res_->G0); }
    iterator end() { return reinterpret_cast<iterator>(res_->G0) + size_; }

    const_iterator cbegin() { return reinterpret_cast<const_iterator>(res_->G0); }
    const_iterator cend() { return reinterpret_cast<const_iterator>(res_->G0) + size_; }

    const_iterator begin() const { return reinterpret_cast<iterator>(res_->G0); }
    const_iterator end() const { return reinterpret_cast<iterator>(res_->G0) + size_; }

    const_iterator cbegin() const { return reinterpret_cast<const_iterator>(res_->G0); }
    const_iterator cend()   const { return reinterpret_cast<const_iterator>(res_->G0) + size_; }

    reverse_iterator rbegin() { return reverse_iterator(end()); }
    reverse_iterator rend()   { return reverse_iterator(begin()); }

    const_reverse_iterator crbegin() { return const_reverse_iterator(end()); }
    const_reverse_iterator crend()   { return const_reverse_iterator(begin()); }

    const_reverse_iterator rbegin() const { return const_reverse_iterator(end()); }
    const_reverse_iterator rend()   const { return const_reverse_iterator(begin()); }

    const_reverse_iterator crbegin() const { return const_reverse_iterator(end()); }
    const_reverse_iterator crend()   const { return const_reverse_iterator(begin()); }

private:
    K res_;
    long long size_;
};

namespace kdb {

    template<> struct c_type<Type::Boolean> { typedef bool type; };
    // template<> struct c_type<Type::GUID> {typedef U type; };
    template<> struct c_type<Type::Byte> {typedef char type; };
    template<> struct c_type<Type::Short> {typedef short type; };
    template<> struct c_type<Type::Int> {typedef int type; };
    template<> struct c_type<Type::Long> {typedef long long type; };
    template<> struct c_type<Type::Real> {typedef float type; };
    template<> struct c_type<Type::Float> {typedef double type; };
    template<> struct c_type<Type::Char> {typedef char type; };
    template<> struct c_type<Type::Symbol> {typedef char* type; };
    template<> struct c_type<Type::Timestamp> {typedef long long type; };
    template<> struct c_type<Type::Month> {typedef int type; };
    template<> struct c_type<Type::Date> {typedef int type; };
    template<> struct c_type<Type::Datetime> {typedef double type; };
    template<> struct c_type<Type::Timespan> {typedef long long type; };
    template<> struct c_type<Type::Minute> {typedef int type; };
    template<> struct c_type<Type::Second> {typedef int type; };
    template<> struct c_type<Type::Time> {typedef int type; };

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

