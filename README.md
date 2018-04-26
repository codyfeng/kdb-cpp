# Sample C++ code connecting to kdb+

## How to Use

1. Download k.h from https://github.com/KxSystems/kdb/blob/master/c/c/k.h and c.o from https://github.com/KxSystems/kdb/blob/master/l64/c.o. Put them in this directory.

2. Compile with command 
   ~~~
   g++ -DKXVER=3 -std=c++11 -lpthread -o kdb_cpp kdb_cpp.cpp c.o
   ~~~

3. Run the binary
   ~~~
   ./kdb_cpp
   ~~~


