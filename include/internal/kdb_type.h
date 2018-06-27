/**
 * @brief   kdb+ types
 * 
 * @file    kdb_type.h
 * @author  Cody Feng <cody.feng"AT"outlook.com>
 * @date    2018-06-27
 */

#ifndef __KDB_TYPE_H__
#define __KDB_TYPE_H__

namespace kdb {
    
    /**
     * @brief   kdb+ data types
     * @see     https://code.kx.com/q/ref/card/#datatypes
     */
    enum class Type {
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
     * @brief   kdb+ data structure types
     * @see     https://code.kx.com/q/ref/elements/#nouns
     */
    enum class StructType {
        Unknown, Atom, Vector, List, Dictionary, Table, KeyedTable
    };

    /**
     * @brief       Mapping from kdb+ data types to c types.
     * @tparam T    enum class Type from above
     */
    template<Type T> struct c_type;

    template<> struct c_type<Type::Boolean> { typedef bool type; };
    // template<> struct c_type<Type::GUID> { typedef U type; };
    template<> struct c_type<Type::Byte> { typedef char type; };
    template<> struct c_type<Type::Short> { typedef short type; };
    template<> struct c_type<Type::Int> { typedef int type; };
    template<> struct c_type<Type::Long> { typedef long long type; };
    template<> struct c_type<Type::Real> { typedef float type; };
    template<> struct c_type<Type::Float> { typedef double type; };
    template<> struct c_type<Type::Char> { typedef char type; };
    template<> struct c_type<Type::Symbol> { typedef char* type; };
    template<> struct c_type<Type::Timestamp> { typedef long long type; };
    template<> struct c_type<Type::Month> { typedef int type; };
    template<> struct c_type<Type::Date> { typedef int type; };
    template<> struct c_type<Type::Datetime> { typedef double type; };
    template<> struct c_type<Type::Timespan> { typedef long long type; };
    template<> struct c_type<Type::Minute> { typedef int type; };
    template<> struct c_type<Type::Second> { typedef int type; };
    template<> struct c_type<Type::Time> { typedef int type; };
}

#endif // __KDB_TYPE_H__
