/**
 * @brief   C++ interface to read kdb+ table
 * 
 * @file    kdb_table.cpp
 * @author  Cody Feng <cody.feng"AT"outlook.com>
 * @date    2018-06-27
 */

#include "kdb_type.h"
#include "kdb_table.h"

namespace kdb {
    Table::Table(const Result &r) : res_(r.res_),
                                    n_rows_(kK(kK(res_->k)[1])[0]->n),
                                    n_cols_(kK(res_->k)[0]->n) {
        if (res_) {
            r1(res_);
        }
    }

    Table::~Table() {
        if (res_) {
            r0(res_);
            res_ = nullptr;
        }
    }

    Vector<Type::Symbol> Table::get_header() {
        return Result(kK(res_->k)[0]).get_vector<Type::Symbol>();
    }

}
