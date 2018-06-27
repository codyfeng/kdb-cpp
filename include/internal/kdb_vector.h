/**
 * @brief   C++ interface to read kdb+ vector
 * 
 * @file    kdb_vector.h
 * @author  Cody Feng <cody.feng"AT"outlook.com>
 * @date    2018-06-27
 */

#ifndef __KDB_VECTOR_H__
#define __KDB_VECTOR_H__

#ifndef KXVER
#define KXVER 3
#endif

#include <iterator>
#include "../external/k.h"
#include "kdb_type.h"

namespace kdb {
    template<Type T>
    class Vector {
    public:
        Vector(K res, long long size) : res_(res), size_(size) { if (res_) { r1(res_); }};
        ~Vector() { if (res_) r0(res_); };
        inline long long size() const { return size_; }

        typedef typename c_type<T>::type & reference;
        typedef typename c_type<T>::type const & const_reference;
        typedef typename c_type<T>::type * iterator;
        typedef typename c_type<T>::type const * const_iterator;
        typedef std::reverse_iterator<iterator> reverse_iterator;
        typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

        reference operator[](const long long i) { return reinterpret_cast<iterator>(res_->G0)[i]; }
        const_reference operator[](const long long i) const { return reinterpret_cast<iterator>(res_->G0)[i]; }

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
}

#endif // __KDB_VECTOR_H__
