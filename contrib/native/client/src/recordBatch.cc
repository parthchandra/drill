#include <boost/log/trivial.hpp>

#include "common.h"
#include "recordBatch.h"

using namespace common;
using namespace exec;
using namespace exec::user;
using namespace Drill;

ValueVectorBase* ValueVectorFactory::allocateValueVector(const FieldMetadata & f, SlicedByteBuf* b){
    ValueVectorBase* v=NULL;
    const FieldDef& fieldDef = f.def();
    const MajorType& majorType=fieldDef.major_type();
    int type = majorType.minor_type();
    int mode = majorType.mode();

    switch (type) {
        case BIGINT:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorInt64(b, f.value_count()); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorInt64(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorInt64(b); 
                    break;
            }
            break;
        case VARBINARY:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorVarBinary(b, f.value_count()); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case VARCHAR:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorVarChar(b, f.value_count()); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;

        /*  
        case TINYINT:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case UINT1:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case UINT2:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case SMALLINT:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case INT:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case UINT4:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case FLOAT4:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case BIGINT:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case UINT8:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case FLOAT8:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case VARBINARY:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case VARCHAR:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case VAR16CHAR:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
        case BIT:
            switch (mode) {
                case DM_REQUIRED:
                    v=new ValueVectorXXXX(b); break;
                case DM_OPTIONAL:
                    //v=new NullableValueVectorXXXX(b); 
                    break;
                case DM_REPEATED:
                    //v=new RepeatedValueVectorXXXX(b); 
                    break;
            }
            break;
            */
        default:
                v=new ValueVectorBase(b, f.value_count());
            break;
    }

    return v;
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
            v->getValueAt(n, valueBuf, (sizeof(valueBuf)-1)*sizeof(char));
            values+=valueBuf;
            values+="    ";
        } 
        BOOST_LOG_TRIVIAL(trace) << values;
    }
}

