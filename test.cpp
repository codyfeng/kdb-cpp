#include <iostream>
#include <numeric>
#include <type_traits>
#include "kdb_cpp.h"

#define HOST_ADDR "127.0.0.1"
#define HOST_PORT 5000

////////////////////////////////////////////
// Test
////////////////////////////////////////////

static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Boolean>()), bool>::value, "");
// static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::GUID>()), void>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Byte>()), char>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Short>()), short>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Int>()), int>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Long>()), long long>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Real>()), float>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Float>()), double>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Char>()), char>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Symbol>()), char*>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Timestamp>()), long long>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Month>()), int>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Date>()), int>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Datetime>()), double>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Timespan>()), long long>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Minute>()), int>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Second>()), int>::value, "");
static_assert(std::is_same<decltype((static_cast<kdb::Result *>(nullptr))->get<kdb::Type::Time>()), int>::value, "");


inline void test_cout(kdb::Result const &r) {
    std::cout << "type: " << static_cast<std::underlying_type<kdb::Type>::type>(r.type()) << " value: " << r << '\n';
}

template <kdb::Type T>
void test_vector_accessor(kdb::Connector &kcon, const char *msg) {
    // Test vector accessors
    kdb::Result res = kcon.sync(msg);
    if (res.struct_type() != kdb::StructType::Vector) {
        std::cout << "Error - result is not a vector.\n";
        return;
    } else if (res.type() != T) {
        std::cout << "Error - expected type " << static_cast<std::underlying_type<kdb::Type>::type>(T)
        << " but got type " << static_cast<std::underlying_type<kdb::Type>::type>(res.type()) << '\n';
        return;
    }
    typename kdb::Vector<T> kv = res.get_vector<T>();
    
    std::cout << "Access by [] operator: ";
    for (long long i = 0; i < kv.size(); ++i) {
        std::cout << kv[i] << ' ';
    }
    std::cout << "\nIterator: ";
    for (typename kdb::Vector<T>::iterator it = kv.begin(); it != kv.end(); ++it) {
        std::cout << *it << ' ';
    }
    std::cout << "\nConst Iterator: ";
    for (typename kdb::Vector<T>::const_iterator it = kv.cbegin(); it != kv.cend(); ++it) {
        std::cout << *it << ' ';
    }
    std::cout << "\nReverse Iterator: ";
    for (typename kdb::Vector<T>::reverse_iterator it = kv.rbegin(); it != kv.rend(); ++it) {
        std::cout << *it << ' ';
    }
    std::cout << "\nReverse Const Iterator: ";
    for (typename kdb::Vector<T>::const_reverse_iterator it = kv.crbegin(); it != kv.crend(); ++it) {
        std::cout << *it << ' ';
    }
    std::cout << "\nRange-based for loop: ";
    for (typename kdb::c_type<T>::type &it : kv) {
        std::cout << it << ' ';
    }
    std::cout << "\nRange-based const for loop: ";
    for (typename kdb::c_type<T>::type const &it : kv) {
        std::cout << it << ' ';
    }
    std::cout << '\n';
}

int main() {
    kdb::Connector kcon;
    if (!kcon.connect(HOST_ADDR, HOST_PORT))
        return -1;

    kdb::Result res = kcon.sync("1+1");
    test_cout(res);

    res = kcon.sync("1+1`"); // Test error catch
    test_cout(res);

    res = kcon.sync("a:1"); // Test assignment
    test_cout(res);

    kdb::Result res1 = kcon.sync("a"); // Test variable fetch
    test_cout(res1);

    kcon.async("a:2"); // Test async request
    res = kcon.sync("a");
    test_cout(res);

    kdb::Result res2 = res; // Test copy constructor
    test_cout(res2);

    kdb::Result res3 = std::move(kcon.sync("12j")); // Test move constructor
    test_cout(res3);

    kcon.async("(neg .z.w) 999"); // Test async request and response
    res = kcon.receive(); // Receive 999
    test_cout(res);
    test_cout(res2);  // res2 should be different from res

    res = kcon.receive(); // Test waiting for non-existing message
    test_cout(res);

    kcon.disconnect();
    kcon.async("(neg .z.w) 999"); // Test async request when disconnected.
    res = kcon.receive();
    test_cout(res);
  
    kcon.connect(HOST_ADDR, HOST_PORT);
    
    // Test atoms
    test_cout(kcon.sync("1b"));
    test_cout(kcon.sync("0x37"));
    test_cout(kcon.sync("10h"));
    test_cout(kcon.sync("11i"));
    test_cout(kcon.sync("12j"));
    test_cout(kcon.sync("13.1e"));
    test_cout(kcon.sync("14.2f"));
    test_cout(kcon.sync("\"a\""));
    test_cout(kcon.sync("`sym"));
    test_cout(kcon.sync("2016.01.01D10:00:00.000000000"));
    test_cout(kcon.sync("2016.01m"));
    test_cout(kcon.sync("2016.01.01"));

    // Test vectors
    test_cout(kcon.sync("10110011b"));
    test_cout(kcon.sync("0x3738"));
    test_cout(kcon.sync("10 11h"));
    test_cout(kcon.sync("11 12i"));
    test_cout(kcon.sync("12 13j"));
    test_cout(kcon.sync("13.1 14.1e"));
    test_cout(kcon.sync("14.2 15.2f"));
    test_cout(kcon.sync("\"ab\""));
    test_cout(kcon.sync("`sym1`sym2"));
    test_cout(kcon.sync("2016.01.01D10:00:00.000000000 2016.01.02D10:00:00.000000000"));
    test_cout(kcon.sync("2016.01 2016.02m"));
    test_cout(kcon.sync("2016.01.01 2016.01.02"));

    // Test dictionaries
    test_cout(kcon.sync("`a`b`c!1 2 3"));

    // Test tables
    test_cout(kcon.sync("([]a:1 2 3 4;b:1.1 2.2 3.3 4.4f;c:`first`second`third`fourth)")); // regular
    test_cout(kcon.sync("([k:`a`b`c]a:1 2 3;b:1.1 2.2 3.3f;c:`first`second`third)")); // keyed

    // Test mixed lists
    test_cout(kcon.sync("(1b; 0x37; 10h; 11i; 12j; 13.1e; 14.2f; \"a\"; `sym)"));
    test_cout(kcon.sync("(1b; 0x37; 10h; 11i; 12j; 13.1e; 14.2f; \"a\"; `sym; ([]a:1 2 3;b:1.1 2.2 3.3f;c:`first`second`third); ([k:`a`b`c]a:1 2 3;b:1.1 2.2 3.3f;c:`first`second`third))"));

    ///////////////////////////////////////
    // Test atom accessors
    ///////////////////////////////////////
    int i = kcon.sync("100i").get<kdb::Type::Int>();
    std::cout << i << '\n';

    double j = kcon.sync("100.0").get<kdb::Type::Float>();
    std::cout << j << '\n';

    int k = kcon.sync("2016i").get<kdb::Type::Int>();
    std::cout << k << '\n';

    char* l = kcon.sync("`abcdefghijk").get<kdb::Type::Symbol>();
    std::cout << l << '\n';
    

    ///////////////////////////////////////
    // Test vector accessors
    ///////////////////////////////////////
    kdb::Vector<kdb::Type::Float> kv = kcon.sync("1.1 2.2 3.3 4.4 5.5").get_vector<kdb::Type::Float>();

    std::cout << "Access by [] operator: ";
    for (long long i = 0; i < kv.size(); ++i) {
        std::cout << kv[i] << ' ';
    }
    std::cout << "\nIterator: ";
    for (kdb::Vector<kdb::Type::Float>::iterator it = kv.begin(); it != kv.end(); ++it) {
        std::cout << *it << ' ';
    }
    std::cout << "\nConst Iterator: ";
    for (kdb::Vector<kdb::Type::Float>::const_iterator it = kv.cbegin(); it != kv.cend(); ++it) {
        std::cout << *it << ' ';
    }
    std::cout << "\nReverse Iterator: ";
    for (kdb::Vector<kdb::Type::Float>::reverse_iterator it = kv.rbegin(); it != kv.rend(); ++it) {
        std::cout << *it << ' ';
    }
    std::cout << "\nReverse Const Iterator: ";
    for (kdb::Vector<kdb::Type::Float>::const_reverse_iterator it = kv.crbegin(); it != kv.crend(); ++it) {
        std::cout << *it << ' ';
    }
    std::cout << "\nRange-based for loop: ";
    for (double it : kv) {
        std::cout << it << ' ';
    }
    std::cout << "\nRange-based const for loop: ";
    for (double const it : kv) {
        std::cout << it << ' ';
    }
    std::cout << '\n';
    std::cout << "sum = " << std::accumulate(kv.begin(), kv.end(), 0.0) << '\n';

    test_vector_accessor<kdb::Type::Boolean>(kcon, "10110011b");
    test_vector_accessor<kdb::Type::Byte>(kcon, "0x373839404142");
    test_vector_accessor<kdb::Type::Short>(kcon, "10 11 12 13 14h");
    test_vector_accessor<kdb::Type::Int>(kcon, "11 12 13 14 15i");
    test_vector_accessor<kdb::Type::Long>(kcon, "101 102 103 104 105");
    test_vector_accessor<kdb::Type::Real>(kcon, "13.1 14.1 15.1 16.1 17.1");
    test_vector_accessor<kdb::Type::Float>(kcon, "14.2 15.2 16.3 17.4 18.5");
    test_vector_accessor<kdb::Type::Char>(kcon, "\"abcdefg\"");
    test_vector_accessor<kdb::Type::Symbol>(kcon, "`sym1`sym2`something`to`test");
    test_vector_accessor<kdb::Type::Timestamp>(kcon, "2016.01.01D10:00:00.000000000 2016.01.02D10:00:00.000000000");
    test_vector_accessor<kdb::Type::Month>(kcon, "2016.01 2016.02 2018.03 2020.12 2035.11 2040.07m");
    test_vector_accessor<kdb::Type::Date>(kcon, "2016.01.01 2016.01.02 2018.03.25 2019.04.11 2020.10.20");

    ///////////////////////////////////////
    // Test table accessor
    ///////////////////////////////////////
    kdb::Table tbl = kcon.sync("([]col1:1 2 3 4;col2:1.1 2.2 3.3 4.4f;col3:`first`second`third`fourth)").get_table();
    kdb::Vector<kdb::Type::Symbol> header = tbl.get_header();
    for (auto const &it : header) {
        std::cout << it << ' ';
    }
    std::cout << '\n';

    // Traverse a column
    kdb::Vector<kdb::Type::Long> col1 = tbl.get_column<kdb::Type::Long>(0);
    for (auto const &it : col1) {
        std::cout << it << ' ';
    }
    std::cout << '\n';
    
    kdb::Vector<kdb::Type::Float> col2 = tbl.get_column<kdb::Type::Float>(1);
    for (auto const &it : col2) {
        std::cout << it << ' ';
    }
    std::cout << '\n';

    // Get a specific cell
    long long cell1 = tbl.get<kdb::Type::Long>(2, 0);
    std::cout << cell1 << ' ';

    double cell2 = tbl.get<kdb::Type::Float>(2, 1);
    std::cout << cell2 << ' ';

    char * cell3 = tbl.get<kdb::Type::Symbol>(2, 2);
    std::cout << cell3 << '\n';

    long long cell4 = tbl.get<long long>(2, 0);
    std::cout << cell4 << ' ';

    double cell5 = tbl.get<double>(2, 1);
    std::cout << cell5 << ' ';

    char * cell6 = tbl.get<char *>(2, 2);
    std::cout << cell6 << '\n';
    

    return 0;
}
