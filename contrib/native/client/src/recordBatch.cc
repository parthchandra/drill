#include <boost/log/trivial.hpp>

#include "recordBatch.h"

using namespace common;
using namespace exec;
using namespace exec::user;
using namespace Drill;

ValueVectorBase* ValueVectorFactory::allocateValueVector(const FieldMetadata & f, SlicedByteBuf* b){
    const FieldDef& fieldDef = f.def();
    const MajorType& majorType=fieldDef.major_type();
    int type = majorType.minor_type();
    int mode = majorType.mode();

    switch (mode) {

    case DM_REQUIRED:
        switch (type)
        {
            case TINYINT:
                return new ValueVectorFixed<int8_t>(b,f.value_count());
            case SMALLINT:
                return new ValueVectorFixed<int16_t>(b,f.value_count());
            case INT:
                return new ValueVectorFixed<int32_t>(b,f.value_count());
            case BIGINT:
                return new ValueVectorFixed<int64_t>(b,f.value_count());

            // Decimal digits, width, max precision is defined in
            //
            // /exec/java-exec/src/main/codegen/data/ValueVectorTypes.tdd
            // 
            // Decimal Design Document: http://bit.ly/drilldecimal
            case DECIMAL9:
                return new ValueVectorDecimal9(b,f.value_count(), majorType.scale());
            case DECIMAL18:
                return new ValueVectorDecimal18(b,f.value_count(), majorType.scale());
            case DECIMAL28DENSE:
                return new ValueVectorDecimal28Dense(b,f.value_count(), majorType.scale());
            case DECIMAL38DENSE:
                return new ValueVectorDecimal38Dense(b,f.value_count(), majorType.scale());
            case DECIMAL28SPARSE:
                return new ValueVectorDecimal28Sparse(b,f.value_count(), majorType.scale());
            case DECIMAL38SPARSE:
                return new ValueVectorDecimal38Sparse(b,f.value_count(), majorType.scale());
            /*
             * NOT IMPLEMENTED
            case MONEY:
            case INTERVAL:
                return new ValueVectorFixed<>(b,f.value_count());
            */
            case DATE:
                return new ValueVectorTyped<DateWrapper, uint64_t>(b,f.value_count());
            case TIMESTAMP:
                return new ValueVectorTyped<DateTimeWrapper, uint64_t>(b,f.value_count());
            case TIME:
                return new ValueVectorTyped<TimeWrapper, uint32_t>(b,f.value_count());
            
            case FLOAT4:
                return new ValueVectorFixed<float>(b,f.value_count());
            case FLOAT8:
                return new ValueVectorFixed<double>(b,f.value_count());
            case BIT:
                return new ValueVectorBit(b,f.value_count());
            case VARBINARY:
                return new ValueVectorVarBinary(b, f.value_count()); 
            case VARCHAR:
                return new ValueVectorVarChar(b, f.value_count()); 

            default:
                return new ValueVectorBase(b, f.value_count()); 
        }
    case DM_OPTIONAL:
        switch (type)
        {
            case TINYINT:
                return new NullableValueVectorFixed<int8_t>(b,f.value_count());
            case SMALLINT:
                return new NullableValueVectorFixed<int16_t>(b,f.value_count());
            case INT:
                return new NullableValueVectorFixed<int32_t>(b,f.value_count());
            case BIGINT:
                return new NullableValueVectorFixed<int64_t>(b,f.value_count());
            case FLOAT4:
                return new NullableValueVectorFixed<float>(b,f.value_count());
            case FLOAT8:
                return new NullableValueVectorFixed<double>(b,f.value_count());
            case DATE:
                return new NullableValueVectorTyped<DateWrapper, ValueVectorTyped<DateWrapper, uint64_t> >(b,f.value_count());
            case TIMESTAMP:
                return new NullableValueVectorTyped<DateTimeWrapper, ValueVectorTyped<DateTimeWrapper, uint64_t> >(b,f.value_count());
            case TIME:
                return new ValueVectorTyped<TimeWrapper, uint32_t>(b,f.value_count());
            // not implemented yet
            default:
                return new ValueVectorBase(b, f.value_count()); 
        }
    case DM_REPEATED:
        switch (type)
        {
             // not implemented yet
            default:
                return new ValueVectorBase(b, f.value_count()); 
        }
    default:
        // TODO should throw an exception or reporting errors 
        return new ValueVectorBase(b, f.value_count()); 
    }

}


int FieldBatch::load(){
    const FieldMetadata& fmd = this->m_fieldMetadata;
    this->m_pValueVector=ValueVectorFactory::allocateValueVector(fmd, this->m_pFieldData);
    return 0;
}

int RecordBatch::build(){
    // For every Field, get the corresponding SlicedByteBuf.
    // Create a Materialized field. Set the Sliced Byted Buf to the correct slice. 
    // Set the Field Metadata.
    // Load the vector.(Load creates a valuevector object of the correct type:
    //    Use ValueVectorFactory(type) to create the right type. 
    //    Create a Value Vector of the Sliced Byte Buf. 
    // Add the field batch to vector
    size_t startOffset=0;
    //TODO: handle schema changes here. Call a client provided callback?
    for(int i=0; i<this->m_numFields; i++){
        const FieldMetadata& fmd=this->m_pRecordBatchDef->field(i);
        size_t len=fmd.buffer_length();
        FieldBatch* pField = new FieldBatch(fmd, this->m_buffer, startOffset, len) ;
        startOffset+=len;
        pField->load(); // set up the value vectors
        this->m_fields.push_back(pField);
        this->m_fieldDefs.push_back(&fmd);
    }
    return 0;
}

void RecordBatch::print(size_t num){
    std::string nameList;
    for(std::vector<FieldBatch*>::size_type i = 0; i != this->m_fields.size(); i++) {
        FieldMetadata fmd=this->getFieldMetadata(i);
        std::string name= fmd.def().name(0).name();
        nameList+=name;
        nameList+="    ";
    } 
    int numToPrint=this->m_numRecords;
    if(num>0)numToPrint=num;
    BOOST_LOG_TRIVIAL(trace) << nameList;
    std::string values;
    for(size_t n=0; n<numToPrint; n++){
        values="";
        for(std::vector<FieldBatch*>::size_type i = 0; i != this->m_fields.size(); i++) {
            const ValueVectorBase * v = m_fields[i]->getVector();
            char valueBuf[1024+1];
            memset(valueBuf, 0, sizeof(valueBuf)*sizeof(char));
            if(v->isNull(n)){
                strncpy(valueBuf,"null", (sizeof(valueBuf)-1)*sizeof(char));
            } else{
                v->getValueAt(n, valueBuf, (sizeof(valueBuf)-1)*sizeof(char));
            }
            values+=valueBuf;
            values+="    ";
        } 
        BOOST_LOG_TRIVIAL(trace) << values;
    }
}

void DateWrapper::load(){
    m_year=1970;
    m_month=1;
    m_day=1;

    time_t  t= m_datetime/1000; // number of seconds since beginning of the Unix Epoch.
    struct tm * tm = gmtime(&t);
    m_year=tm->tm_year+1900;
    m_month=tm->tm_mon+1;
    m_day=tm->tm_mday;
}
void TimeWrapper::load(){
    m_hr=0;
    m_min=0;
    m_sec=0;
    m_msec=0;

    time_t  t= m_datetime/1000; // number of seconds since beginning of the Unix Epoch.
    struct tm * tm = gmtime(&t);
    m_hr=tm->tm_hour;
    m_min=tm->tm_min;
    m_sec=tm->tm_sec;
    m_msec=m_datetime%1000;
}
void DateTimeWrapper::load(){
    m_year=1970;
    m_month=1;
    m_day=1;
    m_hr=0;
    m_min=0;
    m_sec=0;
    m_msec=0;

    time_t  t= m_datetime/1000; // number of seconds since beginning of the Unix Epoch.
    struct tm * tm = gmtime(&t);
    m_year=tm->tm_year+1900;
    m_month=tm->tm_mon+1;
    m_day=tm->tm_mday;
    m_hr=tm->tm_hour;
    m_min=tm->tm_min;
    m_sec=tm->tm_sec;
    m_msec=m_datetime%1000;
}

std::string DateWrapper::toString(){
    std::stringstream sstr;
    sstr << m_year << "-" << m_month << "-" << m_day;
    return sstr.str();
};

std::string TimeWrapper::toString(){
    std::stringstream sstr;
    sstr << m_hr <<":" << m_min<<":"<<m_sec<<"."<<m_msec;
    return sstr.str();
};

std::string DateTimeWrapper::toString(){
    //TODO: Allow config flag to set delimiter
    std::stringstream sstr;
    sstr << m_year << "-" << m_month << "-" << m_day << " " << m_hr <<":" << m_min<<":"<<m_sec<<"."<<m_msec;
    return sstr.str();
};


