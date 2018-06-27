#include <iostream>
#include "../include/kdb_cpp.h"

#define HOST_ADDR "127.0.0.1"
#define HOST_PORT 5000

int main() {
    kdb::Connector kcon;
    if (!kcon.connect(HOST_ADDR, HOST_PORT))
        return -1;
    
    // Create a table
    kcon.sync("tbl_test:([]col1:1 2 3 4;col2:1.1 2.2 3.3 4.4f;col3:`first`second`third`fourth)");

    kdb::Result res = kcon.sync("select from tbl_test");
    kdb::Table tbl = res.get_table();

    // Read header
    kdb::Vector<kdb::Type::Symbol> header = tbl.get_header();
    std::cout << "=======================================\nHeaders: ";
    for (auto const &it : header) {
        std::cout << it << ' ';
    }
    std::cout << '\n';

    // Traverse a column
    kdb::Vector<kdb::Type::Long> column = tbl.get_column<kdb::Type::Long>(0);
    std::cout << "=======================================\nColumn 0: ";
    for (auto const &it : column) {
        std::cout << it << ' ';
    }
    std::cout << '\n';

    // Access cells by rows
    std::cout << "=======================================\nPrinting a table:\n";
    std::cout << "|| ";
    for (auto const &it : header) {
        std::cout << it << "\t|| ";
    }
    std::cout << "\n";
    for (long long row = 0; row < tbl.nrow(); ++row) {
        std::cout << "|  " << tbl.get<kdb::Type::Long>(row, 0) << "\t|  "
                  << tbl.get<kdb::Type::Float>(row, 1) << "\t|  "
                  << tbl.get<kdb::Type::Symbol>(row, 2) << "\t|\n";
    }
    
    return 0;
}