/**
 * @brief   C++ interface to read kdb+ result
 * 
 * @file    kdb_result.h
 * @author  Cody Feng <cody.feng"AT"outlook.com>
 * @date    2018-06-27
 */

#ifndef __KDB_RESULT_H__
#define __KDB_RESULT_H__

#include "kdb_type.h"
#include "kdb_vector.h"

namespace kdb {
    class Table;
    
    class Result {
    public:
        Result() = delete;
        Result(K res, bool inc_ref_count = true);
        Result(const Result &r);
        Result(Result &&r) noexcept;
        ~Result();
        Result & operator = (const Result &r);
        
        friend std::ostream &operator<<(std::ostream &os, const Result &result);
        friend class kdb::Table;

        template<Type> friend class kdb::Vector;

        /**
         * @brief   Get type of the result, see enum class Type for all types.
         * 
         * @return  kdb::Type 
         */
        Type type() const;

        /**
         * @brief   Get structure type of the result, see enum class StructType for all types.
         * 
         * @return  kdb::StructType 
         */
        StructType struct_type() const;

        /**
         * @brief   Get size of the result
         * 
         * @return  long long 
         */
        long long size() const;

        /**
         * @brief       Get atom of type T
         * 
         * @tparam T    int, double, etc
         * @return T    int, double, etc
         */
        template<typename T>
        T get() const {
            return *reinterpret_cast<T *>(&res_->g);
        }

        /**
         * @brief       Get atom of kdb::Type
         * 
         * @tparam T    kdb::Type
         * @return U    corresponding C type
         */
        template<Type T, typename U = typename c_type<T>::type>
        U get() const {
            return get<U>();
        }

        /**
         * @brief   Get the table object. Undefined behavior if the result is not a table.
         *          Use struct_type() to check if the result is a table.
         * 
         * @return  kdb::Table 
         */
        kdb::Table get_table() const;

        /**
         * @brief   Get the vector object. Undefined behavior if the result is not a vector.
         *          Use struct_type() to check if the result is a vector.
         * 
         * @tparam T    kdb::Type
         * @return kdb::Vector<T> 
         */
        template<Type T>
        kdb::Vector<T> get_vector() const {
            return kdb::Vector<T>(res_, size());
        }

    private:
        K res_;
    };

    std::ostream &operator<<(std::ostream &os, const Result &result);
}

#endif // __KDB_RESULT_H__
