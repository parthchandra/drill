#ifndef RECORDBATCH_H
#define RECORDBATCH_H

//using namespace exec::shared;
#include <stdint.h>
#include <vector>
#include <assert.h>
#include <proto-cpp/User.pb.h>

using namespace exec::shared;
using namespace exec::user;

namespace Drill {

    class FieldBatch;
    class ValueVectorBase;

    //TODO: The base classes for value vectors should have abstract functions instead of implementations that return 'NOT IMPLEMENTED YET'

    // A Read Only Sliced byte buffer
    class SlicedByteBuf{
        public:
            //TODO: check the size and offset parameters. What is the largest they can be?
            SlicedByteBuf(const ByteBuf_t b, size_t offset, size_t length){
                this->m_buffer=b;
                this->m_start=offset;
                this->m_end=offset+length;
                this->m_length=length;
            }

            // Carve a sliced buffer out of another sliced buffer
            SlicedByteBuf(const SlicedByteBuf& sb, size_t offset, size_t length){
                this->m_buffer=sb.m_buffer;
                this->m_start=sb.m_start+offset;
                this->m_end=sb.m_start+offset+length;
                this->m_length=length;
            }

            //Copy ctor
            SlicedByteBuf(const SlicedByteBuf& other ){
                if(this!=&other){
                    this->m_buffer=other.m_buffer;
                    this->m_start=other.m_start;
                    this->m_end=other.m_end;
                    this->m_length=other.m_length;
                }
            }

            SlicedByteBuf& operator=( const SlicedByteBuf& rhs ){
                if(this!=&rhs){
                    this->m_buffer=rhs.m_buffer;
                    this->m_start=rhs.m_start;
                    this->m_end=rhs.m_end;
                    this->m_length=rhs.m_length;
                }
                return *this;
            }

            size_t getStart(){
                return this->m_start;
            }
            size_t getEnd(){
                return this->m_end;
            }
            size_t getLength(){
                return this->m_length;
            }

            // ByteBuf_t getBuffer(){ return m_buffer;}
            ByteBuf_t getSliceStart(){ return this->m_buffer+this->m_start;}

            //TODO: implement accessor functions
            /*  
                TYPE getTYPE(size_t index){
                if(index>=m_length) return 0;
                return (TYPE) m_buffer[offset+index];
                }
            */

            template <typename T> T readAt(uint32_t index) const {
                if(index + sizeof(T) <= this->m_length)
                    //return *((T*)&(this->m_buffer+this->m_start)[index]);
                    //return *((T*)&(this->m_buffer)[this->m_start+index]);
                    return *((T*)(this->m_buffer+this->m_start+index));
                return 0;
            }

            uint8_t getByte(uint8_t index){
                return readAt<uint8_t>(index);
            }

            uint32_t getUint32(uint32_t index){
                return readAt<uint32_t>(index);
            }

            uint64_t getUint64(uint64_t index){
                return readAt<uint64_t>(index);
            }

        private:
            ByteBuf_t m_buffer; // the backing store
            size_t  m_start;    //offset within the backing store where this slice begins
            size_t  m_end;      //offset within the backing store where this slice ends
            size_t  m_length;   //length
    };

    class ValueVectorBase{
        public:
            ValueVectorBase(SlicedByteBuf *b, size_t rowCount){
                m_pBuffer=b;
                m_rowCount=rowCount;
            }

            virtual ~ValueVectorBase(){
            }

            const char* get(size_t index) const {
                return "NOT IMPLEMENTED YET";
            }

            virtual void getValueAt(size_t index, char* buf, size_t nChars) const {
                strncpy(buf, "NOT IMPLEMENTED YET", nChars);
                return;
            }

            virtual const ByteBuf_t getRaw(size_t index) const {
                return (ByteBuf_t)"NOT IMPLEMENTED YET";
            }

            virtual uint32_t getSize(size_t index) const {
                return 0;
            }

            virtual bool isNull(size_t index) const {
                return false;
            }

        protected:
            SlicedByteBuf* m_pBuffer;
            size_t m_rowCount;
    };

    class ValueVectorFixedWidth:public ValueVectorBase{
        public:
            ValueVectorFixedWidth(SlicedByteBuf *b, size_t rowCount):ValueVectorBase(b, rowCount){
            }

            void getValueAt(size_t index, char* buf, size_t nChars) const {
                strncpy(buf, "NOT IMPLEMENTED YET", nChars);
                return;
            }

            const ByteBuf_t getRaw(size_t index) const {
                return this->m_pBuffer->getSliceStart()+index*this->getSize(index);
            }

            uint32_t getSize(size_t index) const {
                return 0;
            }
    };

    
    //<Template class??>
    class ValueVectorInt32:public ValueVectorFixedWidth{
        public:
            ValueVectorInt32(SlicedByteBuf *b, size_t rowCount):ValueVectorFixedWidth(b, rowCount){
            }
            uint32_t get(size_t index) const {
                return m_pBuffer->getUint32(index*sizeof(uint32_t));
            }
            void getValueAt(size_t index, char* buf, size_t nChars) const {
                char str[64]; // Can't have more than 64 digits of precision
                //could use itoa instead of sprintf which is slow,  but it is not portable
                sprintf(str, "%d", this->get(index));
                strncpy(buf, str, nChars);
                return;
            }
            uint32_t getSize(size_t index) const {
                return sizeof(uint32_t);
            }
    };

    class ValueVectorInt64:public ValueVectorFixedWidth{
        public:
            ValueVectorInt64(SlicedByteBuf *b, size_t rowCount):ValueVectorFixedWidth(b, rowCount){
            }
            uint64_t get(size_t index) const {
                return m_pBuffer->getUint64(index*sizeof(uint64_t));
            }
            void getValueAt(size_t index, char* buf, size_t nChars) const {
                char str[64]; // Can't have more than 64 digits of precision
                //could use itoa instead of sprintf which is slow,  but it is not portable
                sprintf(str, "%lld", this->get(index));
                strncpy(buf, str, nChars);
                return;
            }
            uint32_t getSize(size_t index) const {
                return sizeof(uint64_t);
            }
    };

    class ValueVectorByte:public ValueVectorFixedWidth{
        public:
            ValueVectorByte(SlicedByteBuf *b, size_t rowCount):ValueVectorFixedWidth(b, rowCount){
            }
            uint8_t get(size_t index) const {
                return m_pBuffer->getByte(index*sizeof(uint8_t));
            }
            void getValueAt(size_t index, char* buf, size_t nChars) const {
                char str[64]; // Can't have more than 64 digits of precision
                //could use itoa instead of sprintf which is slow,  but it is not portable
                sprintf(str, "%x", this->get(index));
                strncpy(buf, str, nChars);
                return;
            }
            uint32_t getSize(size_t index) const {
                return sizeof(uint8_t);
            }
    };

    class ValueVectorBit:public ValueVectorFixedWidth{
        public:
            ValueVectorBit(SlicedByteBuf *b, size_t rowCount):ValueVectorFixedWidth(b, rowCount){
            }
            uint8_t get(size_t index) const {
#ifdef DEBUG
                uint8_t b = m_pBuffer->getByte((index)/sizeof(uint8_t));
                uint8_t bitOffset = index%sizeof(uint8_t);
                uint8_t setBit = (1<<bitOffset); // sets the Nth bit.
                uint8_t isSet = (b&setBit);
#endif
                return (m_pBuffer->getByte((index)/sizeof(uint8_t))) & (1<< (index%sizeof(uint8_t)) );

            }
            void getValueAt(size_t index, char* buf, size_t nChars) const {
                char str[64]; // Can't have more than 64 digits of precision
                //could use itoa instead of sprintf which is slow,  but it is not portable
                sprintf(str, "%x", this->get(index));
                strncpy(buf, str, nChars);
                return;
            }
            uint32_t getSize(size_t index) const {
                return sizeof(uint64_t);
            }
    };

    class VarWidthWrapper{
        public:
            ByteBuf_t data;
            size_t size;
    };

    class ValueVectorVarWidth:public ValueVectorBase{
        public:
            ValueVectorVarWidth(SlicedByteBuf *b, size_t rowCount):ValueVectorBase(b, rowCount){
                size_t offsetEnd = (rowCount+1)*sizeof(uint32_t);
                this->m_pOffsetArray= new SlicedByteBuf(*b, 0, offsetEnd);
                this->m_pData= new SlicedByteBuf(*b, offsetEnd, b->getLength());
            }
            ~ValueVectorVarWidth(){
                delete this->m_pOffsetArray;
                delete this->m_pData;
            }

            //IMPORTANT: This function allocates memory for the value and passes ownership to the caller. 
            //Caller's responsibility to delete the memory.
            ByteBuf_t getCopy(size_t index) const {
                size_t startIdx = this->m_pOffsetArray->getUint32(index*sizeof(uint32_t));
                size_t endIdx = this->m_pOffsetArray->getUint32((index+1)*sizeof(uint32_t));
                size_t length = endIdx - startIdx;
                assert(length >= 0);
                ByteBuf_t dst = new Byte_t[length];
                memcpy(dst, this->m_pData->getSliceStart()+startIdx, length);
                return dst;
            }

            VarWidthWrapper get(size_t index) const {
                size_t startIdx = this->m_pOffsetArray->getUint32(index*sizeof(uint32_t));
                size_t endIdx = this->m_pOffsetArray->getUint32((index+1)*sizeof(uint32_t));
                size_t length = endIdx - startIdx;
                assert(length >= 0);
                // Return an object created on the stack. The compiler will return a 
                // copy and destroy the stack object. The optimizer will hopefully 
                // elide this so we can return an object with no extra memory allocation
                // and no copies.(SEE: http://en.wikipedia.org/wiki/Return_value_optimization) 
                VarWidthWrapper dst;
                dst.data=this->m_pData->getSliceStart()+startIdx;
                dst.size=length;
                return dst;
            }

            void getValueAt(size_t index, char* buf, size_t nChars) const {
                size_t startIdx = this->m_pOffsetArray->getUint32(index*sizeof(uint32_t));
                size_t endIdx = this->m_pOffsetArray->getUint32((index+1)*sizeof(uint32_t));
                size_t length = endIdx - startIdx;
                size_t copyChars=0;
                assert(length >= 0);
                copyChars=nChars<=length?nChars:length;
                memcpy(buf, this->m_pData->getSliceStart()+startIdx, copyChars);
                return;
            }

            const ByteBuf_t getRaw(size_t index) const {
                size_t startIdx = this->m_pOffsetArray->getUint32(index*sizeof(uint32_t));
                size_t endIdx = this->m_pOffsetArray->getUint32((index+1)*sizeof(uint32_t));
                size_t length = endIdx - startIdx;
                assert(length >= 0);
                return this->m_pData->getSliceStart()+startIdx;
            }

            uint32_t getSize(size_t index) const {
                size_t startIdx = this->m_pOffsetArray->getUint32(index*sizeof(uint32_t));
                size_t endIdx = this->m_pOffsetArray->getUint32((index+1)*sizeof(uint32_t));
                size_t length = endIdx - startIdx;
                assert(length >= 0);
                return length;
            }
        private:
            SlicedByteBuf* m_pOffsetArray;
            SlicedByteBuf* m_pData;
    };

    class ValueVectorVarChar:public ValueVectorVarWidth{
        public:
            ValueVectorVarChar(SlicedByteBuf *b, size_t rowCount):ValueVectorVarWidth(b, rowCount){
            }
            VarWidthWrapper get(size_t index) const {
                return ValueVectorVarWidth::get(index);
            }
    };

    class ValueVectorVarBinary:public ValueVectorVarWidth{
        public:
            ValueVectorVarBinary(SlicedByteBuf *b, size_t rowCount):ValueVectorVarWidth(b, rowCount){
            }
    };

    class FieldBatch{
        public:
            FieldBatch(const FieldMetadata& fmd, const ByteBuf_t data, size_t start, size_t length):
                m_fieldMetadata(fmd){
                    m_pValueVector=NULL;
                    m_pFieldData=new SlicedByteBuf(data, start, length);
                }

            ~FieldBatch(){
                if(m_pFieldData!=NULL){
                    delete m_pFieldData; m_pFieldData=NULL;
                }
                if(m_pValueVector!=NULL){
                    delete m_pValueVector; m_pValueVector=NULL;
                }
            }

            // Loads the data into a Value Vector ofappropriate type
            int load();

            const ValueVectorBase * getVector(){
                return m_pValueVector; 
            }

        private:
            const FieldMetadata& m_fieldMetadata;
            ValueVectorBase * m_pValueVector;
            SlicedByteBuf   * m_pFieldData;

    };

    class ValueVectorFactory{
        public:
            static ValueVectorBase* allocateValueVector(const FieldMetadata & fmd, SlicedByteBuf *b);
    };

    class RecordBatch{
        public:
            RecordBatch(QueryResult* pResult, ByteBuf_t b){
                m_pQueryResult=pResult;      
                m_pRecordBatchDef=&pResult->def();
                m_numRecords=pResult->row_count();
                m_buffer=b;
                m_numFields=pResult->def().field_size();
                m_bHasSchemaChanged=false;
            }

            ~RecordBatch(){
                m_buffer=NULL;
                //free memory allocated for FieldBatch objects saved in m_fields;
                for(std::vector<FieldBatch*>::iterator it = m_fields.begin(); it != m_fields.end(); ++it){
                    delete *it;    
                }
                m_fields.clear();
                // we don't free memory for FieldMetadata objects saved in m_fieldDefs since the memory is 
                // owned by the QueryResult object; (see the build() function). Note that the vector contains 
                // a const FieldMetadata* anyway, so it cannot be deleted.
                //for(std::vector<FieldMetadata*>::iterator it = m_fieldDefs.begin(); it != m_fieldDefs.end(); ++it){
                //    delete *it;    
                //}
                m_fieldDefs.clear();
                delete m_pQueryResult;
            }

            // get the ith field metadata
            const FieldMetadata& getFieldMetadata(size_t index){
                return this->m_pRecordBatchDef->field(index);
            }

            size_t getNumRecords(){ return m_numRecords;}
            std::vector<FieldBatch*>& getFields(){ return m_fields;}

            std::vector<const FieldMetadata*>& getColumnDefs(){ return m_fieldDefs;}

            // 
            // build the record batch: i.e. fill up the value vectors from the buffer.
            // On fetching the data from the server, the caller creates a RecordBatch 
            // object then calls build() to build the value vectors.The caller saves the 
            // Record Batch and is responsible for freeing both the RecordBatch and the 
            // raw buffer memory
            //
            int build();

            void print(size_t num);

            const ValueVectorBase * getVector(size_t index){
                return m_fields[index]->getVector(); 
            }

            void schemaChanged(bool b){
                this->m_bHasSchemaChanged=b;
            }

            bool hasSchemaChanged(){ return m_bHasSchemaChanged;}
        private:
            const QueryResult* m_pQueryResult;
            const RecordBatchDef* m_pRecordBatchDef;
            ByteBuf_t m_buffer;
            //build the current schema out of the field metadata
            std::vector<const FieldMetadata*> m_fieldDefs;
            std::vector<FieldBatch*> m_fields;
            size_t m_numFields;
            size_t m_numRecords;
            bool m_bHasSchemaChanged;

    }; // RecordBatch

} // namespace

#endif
