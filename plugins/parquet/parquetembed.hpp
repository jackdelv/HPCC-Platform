/*##############################################################################
    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef _PARQUETEMBED_INCL
#define _PARQUETEMBED_INCL

#ifdef PARQUETEMBED_PLUGIN_EXPORTS
#define PARQUETEMBED_PLUGIN_API DECL_EXPORT
#else
#define PARQUETEMBED_PLUGIN_API DECL_IMPORT
#endif

#define RAPIDJSON_HAS_STDSTRING 1

#include "arrow/api.h"
#include "arrow/io/file.h"

#include "parquet/arrow/reader.h"
#include "parquet/exception.h"
#include "parquet/stream_reader.h"
#include "parquet/stream_writer.h"

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"

// Platform includes
#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "eclhelper.hpp"
#include "tokenserialization.hpp"
#include "rtlfield.hpp"
#include "roxiemem.hpp"

namespace parquetembed
{
    extern void UNSUPPORTED(const char *feature) __attribute__((noreturn));
    extern void failx(const char *msg, ...) __attribute__((noreturn))  __attribute__((format(printf, 1, 2)));
    extern void fail(const char *msg) __attribute__((noreturn));

    static void typeError(const char *expected, const char * fieldname)
    {
        VStringBuffer msg("MongoDBembed: type mismatch - %s expected", expected);
        if (!isEmptyString(fieldname))
            msg.appendf(" for field %s", fieldname);
        rtlFail(0, msg.str());
    }

    static void typeError(const char *expected, const RtlFieldInfo *field)
    {
        typeError(expected, field ? field->name : nullptr);
    }

    static int getNumFields(const RtlTypeInfo *record)
    {
        int count = 0;
        const RtlFieldInfo * const *fields = record->queryFields();
        assertex(fields);
        while (*fields++)
            count++;
        return count;
    }

    static void handleDeserializeOutcome(DeserializationResult resultcode, const char * targetype, const char * culpritvalue)
    {
        switch (resultcode)
        {
        case Deserialization_SUCCESS:
            break;
        case Deserialization_BAD_TYPE:
            failx("Deserialization error (%s): value cannot be const", targetype);
            break;
        case Deserialization_UNSUPPORTED:
            failx("Deserialization error (%s): encountered value type not supported", targetype);
            break;
        case Deserialization_INVALID_TOKEN:
            failx("Deserialization error (%s): token cannot be NULL, empty, or all whitespace", targetype);
            break;
        case Deserialization_NOT_A_NUMBER:
            failx("Deserialization error (%s): non-numeric characters found in numeric conversion: '%s'", targetype, culpritvalue);
            break;
        case Deserialization_OVERFLOW:
            failx("Deserialization error (%s): number too large to be represented by receiving value", targetype);
            break;
        case Deserialization_UNDERFLOW:
            failx("Deserialization error (%s): number too small to be represented by receiving value", targetype);
            break;
        default:
            typeError(targetype, culpritvalue);
            break;
        }
    }

    enum PathNodeType {CPNTScalar, CPNTDataset, CPNTSet};

    struct PathTracker
    {
        StringBuffer nodeName;
        PathNodeType nodeType;
        unsigned int currentChildIndex;
        unsigned int childCount;
        unsigned int childrenProcessed;

        // Constructor given node name and dataset bool
        PathTracker(const StringBuffer& _nodeName, PathNodeType _nodeType)
            : nodeName(_nodeName), nodeType(_nodeType), currentChildIndex(0), childCount(0), childrenProcessed(0)
        {
        }

        // Copy constructor
        PathTracker(const PathTracker& other)
            : nodeName(other.nodeName), nodeType(other.nodeType), currentChildIndex(other.currentChildIndex), childCount(other.childCount), childrenProcessed(other.childrenProcessed)
        {
        }
    };

    class RowBatchBuilder
    {
    public:
        explicit RowBatchBuilder(int64_t num_rows) : field_(nullptr)
        {
            // Reserve all of the space required up-front to avoid unnecessary resizing
            rows_.reserve(num_rows);

            for (int64_t i = 0; i < num_rows; ++i)
            {
                rows_.push_back(rapidjson::Document());
                rows_[i].SetObject();
            }
        }

        // Set which field to convert.
        void SetField(const arrow::Field *field) { field_ = field; }

        // Retrieve converted rows from builder.
        std::vector<rapidjson::Document> Rows() && { return std::move(rows_); }

        // Default implementation
        arrow::Status Visit(const arrow::Array &array)
        {
            return arrow::Status::NotImplemented("Can not convert to json document for array of type ", array.type()->ToString());
        }

        // Handles booleans, integers, floats
        template <typename ArrayType, typename DataClass = typename ArrayType::TypeClass>
        arrow::enable_if_primitive_ctype<DataClass, arrow::Status> Visit(const ArrayType &array)
        {
            assert(static_cast<int64_t>(rows_.size()) == array.length());
            for (int64_t i = 0; i < array.length(); ++i)
            {
                if (!array.IsNull(i))
                {
                    rapidjson::GenericValue<rapidjson::UTF8<>> str_key(field_->name(), rows_[i].GetAllocator());
                    rows_[i].AddMember(str_key, array.Value(i), rows_[i].GetAllocator());
                }
            }
            return arrow::Status::OK();
        }

        arrow::Status Visit(const arrow::StringArray &array)
        {
            assert(static_cast<int64_t>(rows_.size()) == array.length());
            for (int64_t i = 0; i < array.length(); ++i)
            {
                if (!array.IsNull(i))
                {
                    rapidjson::Value str_key(field_->name(), rows_[i].GetAllocator());
                    std::string_view value_view = array.Value(i);
                    rapidjson::Value value;
                    value.SetString(value_view.data(), static_cast<rapidjson::SizeType>(value_view.size()), rows_[i].GetAllocator());
                    rows_[i].AddMember(str_key, value, rows_[i].GetAllocator());
                }
            }
            return arrow::Status::OK();
        }

        arrow::Status Visit(const arrow::StructArray &array)
        {
            const arrow::StructType *type = array.struct_type();

            assert(static_cast<int64_t>(rows_.size()) == array.length());

            RowBatchBuilder child_builder(rows_.size());
            for (int i = 0; i < type->num_fields(); ++i)
            {
                const arrow::Field *child_field = type->field(i).get();
                child_builder.SetField(child_field);
                ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array.field(i).get(), &child_builder));
            }
            std::vector<rapidjson::Document> rows = std::move(child_builder).Rows();

            for (int64_t i = 0; i < array.length(); ++i)
            {
                if (!array.IsNull(i))
                {
                    rapidjson::Value str_key(field_->name(), rows_[i].GetAllocator());
                    // Must copy value to new allocator
                    rapidjson::Value row_val;
                    row_val.CopyFrom(rows[i], rows_[i].GetAllocator());
                    rows_[i].AddMember(str_key, row_val, rows_[i].GetAllocator());
                }
            }
            return arrow::Status::OK();
        }

        arrow::Status Visit(const arrow::ListArray &array)
        {
            assert(static_cast<int64_t>(rows_.size()) == array.length());
            // First create rows from values
            std::shared_ptr<arrow::Array> values = array.values();
            RowBatchBuilder child_builder(values->length());
            const arrow::Field *value_field = array.list_type()->value_field().get();
            std::string value_field_name = value_field->name();
            child_builder.SetField(value_field);
            ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*values.get(), &child_builder));

            std::vector<rapidjson::Document> rows = std::move(child_builder).Rows();

            int64_t values_i = 0;
            for (int64_t i = 0; i < array.length(); ++i)
            {
                if (array.IsNull(i))
                    continue;

                rapidjson::Document::AllocatorType &allocator = rows_[i].GetAllocator();
                auto array_len = array.value_length(i);

                rapidjson::Value value;
                value.SetArray();
                value.Reserve(array_len, allocator);

                for (int64_t j = 0; j < array_len; ++j)
                {
                    rapidjson::Value row_val;
                    // Must copy value to new allocator
                    row_val.CopyFrom(rows[values_i][value_field_name], allocator);
                    value.PushBack(row_val, allocator);
                    ++values_i;
                }

                rapidjson::Value str_key(field_->name(), allocator);
                rows_[i].AddMember(str_key, value, allocator);
            }

            return arrow::Status::OK();
        }

    private:
        const arrow::Field *field_;
        std::vector<rapidjson::Document> rows_;
    };

    /**
     * @brief ParquetHelper holds the inputs from the user, the file stream objects, function for setting the schema, and functions 
     * for opening parquet files.
     */
    class ParquetHelper
    {
        public:
            /**
             * @brief Simple constructor that stores the inputs from the user.
             * 
             * @param option The read or write option.
             * 
             * @param location The location to read a parquet file.
             * 
             * @param destination The destination to write a parquet file.
             * 
             * @param rowsize The max row group size when reading parquet files.
             * 
             * @param batchSize The size of the batches when converting parquet columns to rows.
             */
            ParquetHelper(const char * option, const char * location, const char * destination, int rowsize, int _batchSize)
            {
                p_option = option;
                p_location = location;
                p_destination = destination;
                maxRowSize = rowsize;
                batchSize = _batchSize;
            }

            /**
             * @brief Adds a new field to the schema for writing a parquet file.
             * 
             * @param name Name of the field to be written.
             * @param repetition Repetition setting of the field.
             * @param type Data type of the field.
             * @param ctype Converted Data Type of the field.
             */
            void addField(const char *name, enum parquet::Repetition::type repetition, parquet::Type::type type, enum parquet::ConvertedType::type ctype, int length)
            {
                fields.push_back(parquet::schema::PrimitiveNode::Make(name, repetition, type, ctype, length));
            }

            /**
             * @brief Get the Schema object 
             * 
             * @return std::shared_ptr<parquet::schema::GroupNode> Shared_ptr of schema object for building the write stream.
             */
            std::shared_ptr<parquet::schema::GroupNode> getSchema()
            {
                return std::static_pointer_cast<parquet::schema::GroupNode>(
                    parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
            }

            /**
             * @brief Opens the write stream with the schema and destination.
             * 
             */
            void openWriteFile()
            {
                PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(p_destination));

                parquet::WriterProperties::Builder builder;

                std::shared_ptr<parquet::StreamWriter> os(new parquet::StreamWriter(parquet::ParquetFileWriter::Open(outfile, getSchema(), builder.build())));

                os->SetMaxRowGroupSize(maxRowSize);

                parquet_write = os;
            }

            /**
             * @brief Opens the read stream with the schema and location.
             * 
             */
            void openReadFile()
            {
                PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(p_location));

                PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &parquet_read));
            }

            /**
             * @brief Returns a pointer to the stream writer for writing to the destination.
             * 
             * @return std::shared_ptr<parquet::StreamWriter> 
             */
            std::shared_ptr<parquet::StreamWriter> write()
            {
                return parquet_write;
            }

            /**
             * @brief Sets the parquet_table member to the output of reading from the given
             * parquet file.
             */
            void read()
            {
                std::shared_ptr<arrow::Table> out;
                PARQUET_THROW_NOT_OK(parquet_read->ReadTable(&out));
                parquet_table = out;
            }

            /**
             * @brief Returns the maximum size of the row group set by the user. Default is 1000.
             * 
             * @return int Maximum size of the row group.
             */
            int maxRoWSize()
            {
                return maxRowSize;
            }

            char options()
            {
                if (p_option[0] == 'W' || p_option[0] == 'w')
                {
                    return 'w';
                } 
                else if (p_option[0] == 'R' || p_option[0] == 'r')
                {
                    return 'r';
                }
            }

            // Convert a single batch of Arrow data into Documents
            arrow::Result<std::vector<rapidjson::Document>> ConvertToVector(std::shared_ptr<arrow::RecordBatch> batch) 
            {
                RowBatchBuilder builder{batch->num_rows()};

            for (int i = 0; i < batch->num_columns(); ++i) 
            {
                builder.SetField(batch->schema()->field(i).get());
                ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*batch->column(i).get(), &builder));
            }

            return std::move(builder).Rows();
            }

            arrow::Iterator<rapidjson::Document> ConvertToIterator(std::shared_ptr<arrow::Table> table, size_t batch_size)
            {
                // Use TableBatchReader to divide table into smaller batches. The batches
                // created are zero-copy slices with *at most* `batch_size` rows.
                auto batch_reader = std::make_shared<arrow::TableBatchReader>(*table);
                batch_reader->set_chunksize(batch_size);

                auto read_batch = [this](const std::shared_ptr<arrow::RecordBatch>& batch)->arrow::Result<arrow::Iterator<rapidjson::Document>>
                {
                    ARROW_ASSIGN_OR_RAISE(auto rows, ConvertToVector(batch));
                    return arrow::MakeVectorIterator(std::move(rows));
                };

                auto nested_iter = arrow::MakeMaybeMapIterator(read_batch, arrow::MakeIteratorFromReader(std::move(batch_reader)));

                return arrow::MakeFlattenIterator(std::move(nested_iter));
            }

            void setIterator()
            {
                output = ConvertToIterator(parquet_table, batchSize);
            }

            arrow::Result<rapidjson::Document> next()
            {
                return output.Next();
            }

        private:
            int maxRowSize;                                                     //! The maximum size of each parquet row group.
            size_t batchSize;                                                   //! BatchSize for converting Parquet Columns to ECL rows. It is more efficient to break
                                                                                //  the data into small batches for converting to rows than to convert all at once. 
            std::string p_option;                                               //! Read, r, Write, w, option for specifying parquet operation.
            std::string p_location;                                             //! Location to read parquet file from.
            std::string p_destination;                                          //! Destination to write parquet file to.
            parquet::schema::NodeVector fields;                                 //! Schema vector for appending the information of each field.
            std::shared_ptr<parquet::StreamWriter> parquet_write = nullptr;     //! Output stream for writing to parquet files.
            std::shared_ptr<arrow::io::FileOutputStream> outfile = nullptr;     //! Shared pointer to FileOutputStream object.
            std::unique_ptr<parquet::arrow::FileReader> parquet_read = nullptr; //! Input stream for reading from parquet files.
            std::shared_ptr<arrow::Table> parquet_table = nullptr;              //! Table for creating the iterator for outputing result rows.
            std::shared_ptr<arrow::io::ReadableFile> infile = nullptr;          //! Shared pointer to ReadableFile object.
            arrow::Iterator<rapidjson::Document> output;              //! Arrow iterator to rows read from parquet file.
    };

    /**
     * @brief Builds ECL Records from Parquet result rows.
     *
     */
    class ParquetRowStream : public RtlCInterface, implements IRowStream
    {
    public:
        ParquetRowStream(IEngineRowAllocator* _resultAllocator, std::shared_ptr<ParquetHelper> _parquet);
        virtual ~ParquetRowStream();

        RTLIMPLEMENT_IINTERFACE
        virtual const void* nextRow();
        virtual void stop();

    private:
        Linked<IEngineRowAllocator> m_resultAllocator;  //!< Pointer to allocator used when building result rows.
        bool m_shouldRead;                              //!< If true, we should continue trying to read more messages.
        __int64 m_currentRow;                           //!< Current result row.
        std::shared_ptr<ParquetHelper> s_parquet;       // Shared pointer to ParquetHelper class for the stream class.
    };

    /**
     * @brief Builds ECL records for ParquetRowStream.
     *
     */
    class ParquetRowBuilder : public CInterfaceOf<IFieldSource>
    {
    public:
        ParquetRowBuilder(IPropertyTree * resultrow)
        {
            m_oResultRow.set(resultrow);
            if (!m_oResultRow)
                failx("Missing result row data");
            m_pathStack.reserve(10);
        }

        virtual bool getBooleanResult(const RtlFieldInfo *field);
        virtual void getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result);
        virtual double getRealResult(const RtlFieldInfo *field);
        virtual __int64 getSignedResult(const RtlFieldInfo *field);
        virtual unsigned __int64 getUnsignedResult(const RtlFieldInfo *field);
        virtual void getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result);
        virtual void getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result);
        virtual void getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result);
        virtual void getDecimalResult(const RtlFieldInfo *field, Decimal &value);
        virtual void processBeginSet(const RtlFieldInfo * field, bool &isAll);
        virtual bool processNextSet(const RtlFieldInfo * field);
        virtual void processBeginDataset(const RtlFieldInfo * field);
        virtual void processBeginRow(const RtlFieldInfo * field);
        virtual bool processNextRow(const RtlFieldInfo * field);
        virtual void processEndSet(const RtlFieldInfo * field);
        virtual void processEndDataset(const RtlFieldInfo * field);
        virtual void processEndRow(const RtlFieldInfo * field);

    protected:
        const char * nextField(const RtlFieldInfo * field);
        void xpathOrName(StringBuffer & outXPath, const RtlFieldInfo * field) const;
        void constructNewXPath(StringBuffer& outXPath, const char * nextNode) const;
    private:
        TokenDeserializer m_tokenDeserializer;
        Owned<IPropertyTree> m_oResultRow;
        std::vector<PathTracker> m_pathStack;
    };

    /**
     * @brief Binds ECL records to parquet objects
     *
     */
    class ParquetRecordBinder : public CInterfaceOf<IFieldProcessor>
    {
    public:
        ParquetRecordBinder(const IContextLogger &_logctx, const RtlTypeInfo *_typeInfo, int _firstParam, std::shared_ptr<ParquetHelper> _parquet)
            : logctx(_logctx), typeInfo(_typeInfo), firstParam(_firstParam), dummyField("<row>", NULL, typeInfo), thisParam(_firstParam)
        {
            r_parquet = _parquet;
        }

        int numFields();
        void processRow(const byte *row);
        virtual void processString(unsigned len, const char *value, const RtlFieldInfo * field);
        virtual void processBool(bool value, const RtlFieldInfo * field);
        virtual void processData(unsigned len, const void *value, const RtlFieldInfo * field);
        virtual void processInt(__int64 value, const RtlFieldInfo * field);
        virtual void processUInt(unsigned __int64 value, const RtlFieldInfo * field);
        virtual void processReal(double value, const RtlFieldInfo * field);
        virtual void processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field);
        virtual void processUDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
        {
            UNSUPPORTED("UNSIGNED decimals");
        }

        virtual void processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo * field);
        virtual void processQString(unsigned len, const char *value, const RtlFieldInfo * field);
        virtual void processUtf8(unsigned chars, const char *value, const RtlFieldInfo * field);
        virtual bool processBeginSet(const RtlFieldInfo * field, unsigned numElements, bool isAll, const byte *data)
        {
            UNSUPPORTED("SET");
            return false;
        }
        virtual bool processBeginDataset(const RtlFieldInfo * field, unsigned numRows)
        {
            return false;
        }
        virtual bool processBeginRow(const RtlFieldInfo * field)
        {
            return true;
        }
        virtual void processEndSet(const RtlFieldInfo * field)
        {
            UNSUPPORTED("SET");
        }
        virtual void processEndDataset(const RtlFieldInfo * field)
        {
            UNSUPPORTED("DATASET");
        }
        virtual void processEndRow(const RtlFieldInfo * field)
        {
        }

    protected:
        inline unsigned checkNextParam(const RtlFieldInfo * field);

        const RtlTypeInfo *typeInfo = nullptr;
        const IContextLogger &logctx;
        int firstParam;
        RtlFieldStrInfo dummyField;
        int thisParam;
        TokenSerializer m_tokenSerializer;

        std::shared_ptr<ParquetHelper> r_parquet;
    };

    /**
     * @brief Binds an ECL dataset to a vector of parquet objects.
     *
     */
    class ParquetDatasetBinder : public ParquetRecordBinder
    {
    public:
        /**
         * @brief Construct a new ParquetDataset Binder object
         *
         * @param _logctx logger for building the dataset.
         * @param _input Stream of input of dataset.
         * @param _typeInfo Field type info.
         * @param _query Holds the builder object for creating the documents.
         * @param _firstParam Index of the first param.
         */
        ParquetDatasetBinder(const IContextLogger &_logctx, IRowStream * _input, const RtlTypeInfo *_typeInfo, std::shared_ptr<ParquetHelper> _parquet, int _firstParam)
            : input(_input), ParquetRecordBinder(_logctx, _typeInfo, _firstParam, _parquet)
        {
            d_parquet = _parquet;

            getFieldTypes(_typeInfo);
        }
        void getFieldTypes(const RtlTypeInfo *typeInfo);

        /**
         * @brief Gets the next ECL row.
         *
         * @return true If there is a row to process.
         * @return false If there are no rows left.
         */
        bool bindNext()
        {
            roxiemem::OwnedConstRoxieRow nextRow = (const byte *) input->ungroupedNextRow();
            if (!nextRow)
                return false;
            processRow((const byte *) nextRow.get());   // Bind the variables for the current row
            return true;
        }

        /**
         * @brief Binds all the rows of the dataset and executes the function.
         */
        void executeAll()
        {
            d_parquet->openWriteFile();

            for(int i = 1; bindNext(); i++)
            {
                // After all the fields have been written end the row
                *d_parquet->write() << parquet::EndRow;
                
                if (i % d_parquet->maxRoWSize() == 0) 
                {
                    // At the end of each row group send EndRowGroup.
                    // If EndRowGroup is not explicitly passed in then the 
                    // groups are created automatically.
                    *d_parquet->write() << parquet::EndRowGroup;
                }
            }   
        }

    protected:
        Owned<IRowStream> input;
        std::shared_ptr<ParquetHelper> d_parquet;       //! Helper object for keeping track of read and write options, schema, and file names.
    };

    /**
     * @brief Main interface for the engine to interact with the plugin. The get functions return results to the engine and the Rowstream and
     *
     */
    class ParquetEmbedFunctionContext : public CInterfaceOf<IEmbedFunctionContext>
    {
    public:
        ParquetEmbedFunctionContext(const IContextLogger &_logctx, const char *options, unsigned _flags);
        virtual ~ParquetEmbedFunctionContext();
        virtual bool getBooleanResult();
        virtual void getDataResult(size32_t &len, void * &result);
        virtual double getRealResult();
        virtual __int64 getSignedResult();
        virtual unsigned __int64 getUnsignedResult();
        virtual void getStringResult(size32_t &chars, char * &result);
        virtual void getUTF8Result(size32_t &chars, char * &result);
        virtual void getUnicodeResult(size32_t &chars, UChar * &result);
        virtual void getDecimalResult(Decimal &value);
           virtual void getSetResult(bool & __isAllResult, size32_t & __resultBytes, void * & __result, int elemType, size32_t elemSize)
        {
            UNSUPPORTED("SET results");
        }
        virtual IRowStream * getDatasetResult(IEngineRowAllocator * _resultAllocator);
        virtual byte * getRowResult(IEngineRowAllocator * _resultAllocator);
        virtual size32_t getTransformResult(ARowBuilder & rowBuilder);
        virtual void bindRowParam(const char *name, IOutputMetaData & metaVal, const byte *val) override;
        virtual void bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val);
        virtual void bindBooleanParam(const char *name, bool val);
        virtual void bindDataParam(const char *name, size32_t len, const void *val);
        virtual void bindFloatParam(const char *name, float val);
        virtual void bindRealParam(const char *name, double val);
        virtual void bindSignedSizeParam(const char *name, int size, __int64 val);
        virtual void bindSignedParam(const char *name, __int64 val);
        virtual void bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val);
        virtual void bindUnsignedParam(const char *name, unsigned __int64 val);
        virtual void bindStringParam(const char *name, size32_t len, const char *val);
        virtual void bindVStringParam(const char *name, const char *val);
        virtual void bindUTF8Param(const char *name, size32_t chars, const char *val);
        virtual void bindUnicodeParam(const char *name, size32_t chars, const UChar *val);
        virtual void bindSetParam(const char *name, int elemType, size32_t elemSize, bool isAll, size32_t totalBytes, const void *setData)
        {
            UNSUPPORTED("SET parameters");
        }
        virtual IInterface *bindParamWriter(IInterface *esdl, const char *esdlservice, const char *esdltype, const char *name)
        {
            return NULL;
        }
        virtual void paramWriterCommit(IInterface *writer)
        {
            UNSUPPORTED("paramWriterCommit");
        }
        virtual void writeResult(IInterface *esdl, const char *esdlservice, const char *esdltype, IInterface *writer)
        {
            UNSUPPORTED("writeResult");
        }
        virtual void importFunction(size32_t lenChars, const char *text)
        {
            UNSUPPORTED("importFunction");
        }
        virtual void compileEmbeddedScript(size32_t chars, const char *script);
        virtual void callFunction();
        virtual void loadCompiledScript(size32_t chars, const void *_script) override
        {
            UNSUPPORTED("loadCompiledScript");
        }
        virtual void enter() override {}
        virtual void reenter(ICodeContext *codeCtx) override {}
        virtual void exit() override {}

    protected:
        void execute();
        unsigned checkNextParam(const char *name);
        const IContextLogger &logctx;
        Owned<IPropertyTreeIterator> m_resultrow;

        int m_NextRow;                                  //! Next Row to process.
        Owned<ParquetDatasetBinder> m_oInputStream;     //! Input Stream used for building a dataset.

        TokenDeserializer m_tokenDeserializer;
        TokenSerializer m_tokenSerializer;
        unsigned m_nextParam;                           //! Index of the next parameter to process.
        unsigned m_numParams;                           //! Number of parameters in the function definition.
        unsigned m_scriptFlags;                         //! Count of flags raised by embedded script.

        std::shared_ptr<ParquetHelper> m_parquet;       //! Helper object for keeping track of read and write options, schema, and file names.
    };
}
#endif
