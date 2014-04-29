#ifndef _DECIMAL_UTILS_H
#define _DECIMAL_UTILS_H

#include <boost/multiprecision/cpp_int.hpp>

namespace Drill
{
    class SlicedByteBuf;

struct DecimalValue
{
    DecimalValue() :
        m_unscaledValue(0),
        m_scale(0)
    {
        ;
    }

    DecimalValue(int32_t val, int32_t scale) : m_unscaledValue(val), m_scale(scale){ }
    DecimalValue(int64_t val, int32_t scale) : m_unscaledValue(val), m_scale(scale){ }
    boost::multiprecision::cpp_int m_unscaledValue;
    int32_t m_scale;
};

DecimalValue getDecimalValueFromByteBuf(SlicedByteBuf& data, int startIndex, int nDecimalDigits, int scale, bool truncateScale);
DecimalValue getDecimalValueFromDense(SlicedByteBuf& data, int startIndex, int nDecimalDigits, int scale, int maxPrecision, int width);

inline DecimalValue getDecimalValueFromIntermediate(SlicedByteBuf& data, int startIndex, int nDecimalDigits, int scale)
{
    // In the intermediate representation, we don't pad the scale with zeros. Set truncate to false.
    return getDecimalValueFromByteBuf(data, startIndex, nDecimalDigits, scale, false);
}

inline DecimalValue getDecimalValueFromSparse(SlicedByteBuf& data, int startIndex, int nDecimalDigits, int scale)
{
    // In the sparse representation, we pad the scale with zeroes for ease of arithmetic. Set truncate to true.
    return getDecimalValueFromByteBuf(data, startIndex, nDecimalDigits, scale, true);
}

}
#endif
