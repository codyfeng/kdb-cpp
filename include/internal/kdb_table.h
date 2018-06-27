/**
 * @brief   C++ interface to read kdb+ table
 * 
 * @file    kdb_table.h
 * @author  Cody Feng <cody.feng"AT"outlook.com>
 * @date    2018-06-27
 */

#ifndef __KDB_TABLE_H__
#define __KDB_TABLE_H__

#ifndef KXVER
#define KXVER 3
#endif

#include "../external/k.h"
#include "kdb_type.h"
#include "kdb_result.h"
#include "kdb_vector.h"

namespace kdb {

    class Table {
    public:
        Table(const Result &r);
        ~Table();

        /**
         * @brief   Number of columns
         */
        inline long long ncol() const { return n_cols_; }

        /**
         * @brief   Number of rows
         */
        inline long long nrow() const { return n_rows_; }

        /**
         * @brief Get the table header as a vector of symbols
         * 
         * @return Vector<Type::Symbol> 
         */
        Vector<Type::Symbol> get_header();
        
        /**
         * @brief Get the column object as a vector
         * 
         * @tparam T    kdb::Type
         * @param col   column index
         * @return kdb::Vector<T> 
         */
        template<Type T>
        Vector<T> get_column(long long col) const;

        /**
         * @brief       Get value of a cell
         * 
         * @tparam T    kdb::Type
         * @param row   row index [0, ...)
         * @param col   column index [0, ...)
         * @return U    corresponding C type of kdb::Type
         */
        template<Type T, typename U = typename c_type<T>::type>
        U get(long long row, long long col) const;

        /**
         * @brief       Get value of a cell
         * 
         * @tparam T    int, long long, double, ...
         * @param row   row index [0, ...)
         * @param col   column index [0, ...)
         * @return T
         */
        template<typename T>
        T get(long long row, long long col) const;

    private:
        K res_;
        long long n_rows_; // number of rows
        long long n_cols_; // number of columns
    };

    template<Type T>
    Vector<T> Table::get_column(long long col) const {
        return Result(kK(kK(res_->k)[1])[col]).get_vector<T>();
    }

    template<typename T>
    T Table::get(long long row, long long col) const {
        return (reinterpret_cast<T *>(kK(kK(res_->k)[1])[col]->G0))[row];
    }

    template<Type T, typename U>
    U Table::get(long long row, long long col) const {
        return get<U>(row, col);
    }

}

#endif // __KDB_TABLE_H__
