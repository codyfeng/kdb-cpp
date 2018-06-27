/**
 * @brief   C++ interface to read kdb+ result
 * 
 * @file    kdb_result.cpp
 * @author  Cody Feng <cody.feng"AT"outlook.com>
 * @date    2018-06-27
 */

#include "kdb_table.h"
#include "kdb_result.h"

namespace kdb {
    Result::Result(K res, bool inc_ref_count) {
        res_ = res;
        if (inc_ref_count && res_) {
            r1(res_);
        }
    }

    Result::~Result() {
        if (res_) {
            r0(res_);   // Reduce reference count
            res_ = nullptr; // Avoid double free
        }
    }

    // Copy constructor, e.g., Result r1 = r2;
    Result::Result(const Result &r) {
        res_ = r.res_;
        if (res_) {
            r1(res_);  // Increment reference count
        }
    }

    // Move constructor
    Result::Result(Result &&r) noexcept {
        res_ = r.res_;
        r.res_ = nullptr;
    }

    // Assignment operator, e.g., r1 = r2;
    Result & Result::operator = (const Result &r) {
        if (this != &r) {
            if (res_) {
                r0(res_);  // Reduce reference count
            }
            res_ = r.res_;
            if (res_) {
                r1(res_);  // Increase reference count
            }
        }
        return *this;
    }

    Table Result::get_table() const {
        return Table(*this);
    }

    std::ostream &operator<<(std::ostream &os, K const &res) {
        if (res) {
            int idx;
            switch (res->t) {
            case(-1) : // Boolean atom
                if (res->g) {
                    os << "true";
                } else {
                    os << "false";
                }
                break;
            case(-4) : // Byte atom
                os << res->g; break;
            case(-5) : // Short atom
                os << res->h; break;
            case(-6) : case(-13) : case(-17) : case(-18) : // Int Month Minute Second atom
                os << res->i; break;
            case(-7) : case(-12) : case(-16) : // Long Timestamp Timespan atom
                os << res->j; break;
            case(-8) : // Real atom
                os << res->e; break;
            case(-9) : case(-15) : // Float Datetime atom
                os << res->f; break;
            case(-10) : // Char atom
                os << res->g; break;
            case(-11) : // Symbol atom
                os << res->s; break;
            case(-19) : // Time atom
                os << res->i; break;
            case(-14) : // Date atom
                os << dj(res->i); break;
            case(0) : // Mixed List
                for (idx = 0; idx < res->n; idx++) {
                    os << kK(res)[idx]  << ' ';
                }
                break;
            case(1) : // Boolean vector
                for (idx = 0; idx < res->n; idx++) {
                    if (kG(res)[idx]) {
                        os << "true";
                    } else {
                        os << "false";
                    }
                    os << ' ';
                }
                break;
            case(4) : // Byte vector
                for (idx = 0; idx < res->n; idx++) {
                    os << kG(res)[idx] << ' ';
                }
                break;
            case(5) : // Short vector
                for (idx = 0; idx < res->n; idx++) {
                    os << kH(res)[idx] << ' ';
                }
                break;
            case(6) : case(13) : case(17) : case(18) : // Int Month Minute Second vector
                for (idx = 0; idx < res->n; idx++) {
                    os << kI(res)[idx] << ' ';
                }
                break;
            case(7) : case(12) : case(16) : // Long Timestamp Timespan vector
                for (idx = 0; idx < res->n; idx++) {
                    os << kJ(res)[idx] << ' ';
                }
                break;
            case(8) : // Real vector
                for (idx = 0; idx < res->n; idx++) {
                    os << kE(res)[idx] << ' ';
                }
                break;
            case(9) : case(15) : // Float Datetime vector
                for (idx = 0; idx < res->n; idx++) {
                    os << kF(res)[idx] << ' ';
                }
                break;
            case(10) : // Char vector, i.e., String
                for (idx = 0; idx < res->n; idx++) {
                    os << kC(res)[idx];
                }
                break;
            case(11) : // Symbol vector
                for (idx = 0; idx < res->n; idx++) {
                    os << kS(res)[idx] << ' ';
                }
                break;
            case(19) : // Time vector
                for (idx = 0; idx < res->n; idx++) {
                    os << kI(res)[idx] << ' ';
                }
                break;
            case(14) : // Date vector
                for (idx = 0; idx < res->n; idx++) {
                    os << dj(kI(res)[idx]) << ' ';
                }
                break;
            case(98) : // Non-keyed Table
                os << kK(res->k)[0] << kK(res->k)[1];
                break;
            case(99) : // Dictionary or keyed table
                os << kK(res)[0] << kK(res)[1];
                break;
            }
        }
        return os;
    }

    std::ostream &operator<<(std::ostream &os, Result const &result) {
        return os << result.res_;
    }

    Type Result::type() const {
        if (res_) {
            return static_cast<Type>(res_->t < 0 ? -res_->t : res_->t);
        } else {
            return Type::Error;
        }
    }

    StructType Result::struct_type() const {
        if (res_) {
            if (res_->t < 0) {
                return StructType::Atom;
            } else if (res_->t == 0) {
                return StructType::List;
            } else if (res_->t < 20) {
                return StructType::Vector;
            } else if (res_->t == 98) {
                return StructType::Table;
            } else if (res_->t == 99) {
                return StructType::Dictionary; // TODO identify KeyedTable
            }
        }

        return StructType::Unknown;
    }

    long long Result::size() const {
        if (res_->t >= 0 && res_->t < 20) {
            // vector and mixed list
            return static_cast<long long>(res_->n);
        } else {
            // atom, table and dict
            return 1;
        }
    }
}