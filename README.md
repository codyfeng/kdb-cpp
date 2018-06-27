# Sample C++ code connecting to kdb+

## How to Use

1. Download k.h from https://github.com/KxSystems/kdb/blob/master/c/c/k.h and c.o from https://github.com/KxSystems/kdb/blob/master/l64/c.o. Put them in `include/external/`.

2. Compile and build examples
   
    * Simple table query - examples/query_table.cpp
        ~~~
        g++ -std=c++11 -lpthread -o kdb_cpp include/internal/*.cpp examples/query_table.cpp include/external/c.o
        ~~~

    * A lot of test cases - examples/test.cpp
        ~~~
        g++ -std=c++11 -lpthread -o kdb_cpp include/internal/*.cpp examples/test.cpp include/external/c.o
        ~~~

3. Run the binary
   ~~~
   ./kdb_cpp
   ~~~


