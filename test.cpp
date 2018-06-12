#include <iostream>
#include <type_traits>
#include "kdb_cpp.h"


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

int main() {
    kdb::Connector kcon;
    if (!kcon.connect("127.0.0.1", 5000))
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

    kdb::Result res2 = res;
    test_cout(res2);

    kdb::Result res3 = std::move(kcon.sync("12j"));
    test_cout(res3);


    kcon.async("(neg .z.w) 999"); // Test async request and response
    res = kcon.receive(); // Receive 999
    test_cout(res);

    test_cout(res2);

    res = kcon.receive(); // Test waiting for non-existing message
    test_cout(res);

    kcon.disconnect();
    kcon.async("(neg .z.w) 999");
    res = kcon.receive();
    test_cout(res);


    
    kcon.connect("127.0.0.1", 5000);
    
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
    test_cout(kcon.sync("([]a:1 2 3;b:1.1 2.2 3.3f;c:`first`second`third)"));
    test_cout(kcon.sync("([k:`a`b`c]a:1 2 3;b:1.1 2.2 3.3f;c:`first`second`third)"));

    // Test mixed lists
    test_cout(kcon.sync("(1b; 0x37; 10h; 11i; 12j; 13.1e; 14.2f; \"a\"; `sym)"));
    test_cout(kcon.sync("(1b; 0x37; 10h; 11i; 12j; 13.1e; 14.2f; \"a\"; `sym; ([]a:1 2 3;b:1.1 2.2 3.3f;c:`first`second`third); ([k:`a`b`c]a:1 2 3;b:1.1 2.2 3.3f;c:`first`second`third))"));

    int i = kcon.sync("100i").get<kdb::Type::Int>();
    std::cout << i << std::endl;

    double j = kcon.sync("100.0").get<kdb::Type::Float>();
    std::cout << j << std::endl;

    int k = kcon.sync("2016.01.01").get<kdb::Type::Date>();
    std::cout << k << std::endl;
    
    
    return 0;
}
