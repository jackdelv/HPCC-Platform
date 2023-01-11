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
// #include "arrow/dataset/api.h"
// #include "arrow/filesystem/api.h"
#include "arrow/io/file.h"
#include "arrow/util/logging.h"
#include "arrow/ipc/api.h"
#include "parquet/arrow/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/document.h"

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"

// Platform includes
#include "hqlplugins.hpp"
#include "eclrtl_imp.hpp"
#include "eclhelper.hpp"
#include "tokenserialization.hpp"
#include "rtlfield.hpp"
#include "roxiemem.hpp"

#include <iostream>

namespace parquetembed
{
    static rapidjson::MemoryPoolAllocator<> jsonAlloc;

    extern void UNSUPPORTED(const char *feature) __attribute__((noreturn));
    extern void failx(const char *msg, ...) __attribute__((noreturn))  __attribute__((format(printf, 1, 2)));
    extern void fail(const char *msg) __attribute__((noreturn));

    void reportIfFailure(arrow::Status st)
    {
        if (!st.ok())
        {
            failx("%s: %s.", st.CodeAsString().c_str(), st.message().c_str());
        }
    }

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

    const rapidjson::Value kNullJsonSingleton = rapidjson::Value();

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
            return arrow::Status::NotImplemented("Cannot convert to json document for array of type ", array.type()->ToString());
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

    class DocValuesIterator {
    public:
    /// \param rows vector of rows
    /// \param path field names to enter
    /// \param array_levels number of arrays to enter
    DocValuesIterator(const std::vector<rapidjson::Document>& rows,
                        std::vector<std::string> path, int64_t array_levels)
        : rows(rows), path(std::move(path)), array_levels(array_levels) {}

    const rapidjson::Value* NextArrayOrRow(const rapidjson::Value* value, size_t* path_i,
                                            int64_t* arr_i) {
        while (array_stack.size() > 0) {
        ArrayPosition& pos = array_stack.back();
        // Try to get next position in Array
        if (pos.index + 1 < pos.array_node->Size()) {
            ++pos.index;
            value = &(*pos.array_node)[pos.index];
            *path_i = pos.path_index;
            *arr_i = array_stack.size();
            return value;
        } else {
            array_stack.pop_back();
        }
        }
        ++row_i;
        if (row_i < rows.size()) {
        value = static_cast<const rapidjson::Value*>(&rows[row_i]);
        } else {
        value = nullptr;
        }
        *path_i = 0;
        *arr_i = 0;
        return value;
    }

    arrow::Result<const rapidjson::Value*> Next() {
        const rapidjson::Value* value = nullptr;
        size_t path_i;
        int64_t arr_i;
        // Can either start at document or at last array level
        if (array_stack.size() > 0) {
        auto pos = array_stack.back();
        value = pos.array_node;
        path_i = pos.path_index;
        arr_i = array_stack.size() - 1;
        }

        value = NextArrayOrRow(value, &path_i, &arr_i);

        // Traverse to desired level (with possible backtracking as needed)
        while (path_i < path.size() || arr_i < array_levels) {
        if (value == nullptr) {
            return value;
        } else if (value->IsArray() && value->Size() > 0) {
            ArrayPosition pos;
            pos.array_node = value;
            pos.path_index = path_i;
            pos.index = 0;
            array_stack.push_back(pos);

            value = &(*value)[0];
            ++arr_i;
        } else if (value->IsArray()) {
            // Empty array means we need to backtrack and go to next array or row
            value = NextArrayOrRow(value, &path_i, &arr_i);
        } else if (value->HasMember(path[path_i])) {
            value = &(*value)[path[path_i]];
            ++path_i;
        } else {
            return &kNullJsonSingleton;
        }
        }

        // Return value
        return value;
    }

    private:
    const std::vector<rapidjson::Document>& rows;
    std::vector<std::string> path;
    int64_t array_levels;
    size_t row_i = -1;  // index of current row

    // Info about array position for one array level in array stack
    struct ArrayPosition {
        const rapidjson::Value* array_node;
        int64_t path_index;
        rapidjson::SizeType index;
    };
    std::vector<ArrayPosition> array_stack;
    };

    class JsonValueConverter {
    public:
    explicit JsonValueConverter(const std::vector<rapidjson::Document>& rows)
        : rows_(rows), array_levels_(0) {}

    JsonValueConverter(const std::vector<rapidjson::Document>& rows,
                        const std::vector<std::string>& root_path, int64_t array_levels)
        : rows_(rows), root_path_(root_path), array_levels_(array_levels) {}

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field& field, arrow::ArrayBuilder* builder) {
        return Convert(field, field.name(), builder);
    }

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field& field, const std::string& field_name,
                            arrow::ArrayBuilder* builder) {
        field_name_ = field_name;
        builder_ = builder;
        ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field.type().get(), this));
        return arrow::Status::OK();
    }

    // Default implementation
    arrow::Status Visit(const arrow::DataType& type) {
        return arrow::Status::NotImplemented(
            "Can not convert json value to Arrow array of type ", type.ToString());
    }

    arrow::Status Visit(const arrow::Int64Type& type) {
        arrow::Int64Builder* builder = static_cast<arrow::Int64Builder*>(builder_);
        for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            if (value->IsUint()) {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetUint()));
            } else if (value->IsInt()) {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetInt()));
            } else if (value->IsUint64()) {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetUint64()));
            } else if (value->IsInt64()) {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetInt64()));
            } else {
            return arrow::Status::Invalid("Value is not an integer");
            }
        }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::FloatType& type) 
    {
        arrow::FloatBuilder* builder = static_cast<arrow::FloatBuilder*>(builder_);
        for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetFloat()));
        }
        }
        return arrow::Status::OK();
  }

    arrow::Status Visit(const arrow::DoubleType& type) {
        arrow::DoubleBuilder* builder = static_cast<arrow::DoubleBuilder*>(builder_);
        for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetDouble()));
        }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StringType& type) {
        arrow::StringBuilder* builder = static_cast<arrow::StringBuilder*>(builder_);
        for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetString()));
        }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::BooleanType& type) {
        arrow::BooleanBuilder* builder = static_cast<arrow::BooleanBuilder*>(builder_);
        for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetBool()));
        }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StructType& type) {
        arrow::StructBuilder* builder = static_cast<arrow::StructBuilder*>(builder_);

        std::vector<std::string> child_path(root_path_);
        if (field_name_.size() > 0) {
        child_path.push_back(field_name_);
        }
        auto child_converter = JsonValueConverter(rows_, child_path, array_levels_);

        for (int i = 0; i < type.num_fields(); ++i) {
        std::shared_ptr<arrow::Field> child_field = type.field(i);
        std::shared_ptr<arrow::ArrayBuilder> child_builder = builder->child_builder(i);

        ARROW_RETURN_NOT_OK(
            child_converter.Convert(*child_field.get(), child_builder.get()));
        }

        // Make null bitmap
        for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
        }

        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::ListType& type) {
        arrow::ListBuilder* builder = static_cast<arrow::ListBuilder*>(builder_);

        // Values and offsets needs to be interleaved in ListBuilder, so first collect the
        // values
        std::unique_ptr<arrow::ArrayBuilder> tmp_value_builder;
        ARROW_ASSIGN_OR_RAISE(tmp_value_builder,
                            arrow::MakeBuilder(builder->value_builder()->type()));
        std::vector<std::string> child_path(root_path_);
        child_path.push_back(field_name_);
        auto child_converter = JsonValueConverter(rows_, child_path, array_levels_ + 1);
        ARROW_RETURN_NOT_OK(
            child_converter.Convert(*type.value_field().get(), "", tmp_value_builder.get()));

        std::shared_ptr<arrow::Array> values_array;
        ARROW_RETURN_NOT_OK(tmp_value_builder->Finish(&values_array));
        std::shared_ptr<arrow::ArrayData> values_data = values_array->data();

        arrow::ArrayBuilder* value_builder = builder->value_builder();
        int64_t offset = 0;
        for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
        if (!value->IsNull() && value->Size() > 0) {
            ARROW_RETURN_NOT_OK(
                value_builder->AppendArraySlice(*values_data.get(), offset, value->Size()));
            offset += value->Size();
        }
        }

        return arrow::Status::OK();
    }

    private:
    std::string field_name_;
    arrow::ArrayBuilder* builder_;
    const std::vector<rapidjson::Document>& rows_;
    std::vector<std::string> root_path_;
    int64_t array_levels_;

    /// Return a flattened iterator over values at nested location
    arrow::Iterator<const rapidjson::Value*> FieldValues() {
        std::vector<std::string> path(root_path_);
        if (field_name_.size() > 0) 
        {
            path.push_back(field_name_);
        }

        auto iter = DocValuesIterator(rows_, std::move(path), array_levels_);
        auto fn = [iter]() mutable -> arrow::Result<const rapidjson::Value*> 
        {
            return iter.Next();
        };

        return arrow::MakeFunctionIterator(fn);
    }
    };

    /**
     * @brief ParquetHelper holds the inputs from the user, the file stream objects, function for setting the schema, and functions 
     * for opening parquet files.
     */
    class ParquetHelper
    {
        public:

            ParquetHelper(const char * option, const char * location, const char * destination, const char * partDir, int rowsize, int _batchSize);
            std::shared_ptr<arrow::Schema> getSchema();
            arrow::Status openWriteFile();
            void openReadFile();
            arrow::Status writePartition(std::shared_ptr<arrow::Table> table);
            std::unique_ptr<parquet::arrow::FileWriter> * write();
            void read();
            rapidjson::Value * doc();
            void update_row();
            std::vector<rapidjson::Document> * record_batch();
            bool partSetting();
            int getMaxRowSize();
            char options();
            bool shouldRead();
            arrow::Result<std::vector<rapidjson::Document>> ConvertToVector(std::shared_ptr<arrow::RecordBatch> batch);
            arrow::Iterator<rapidjson::Document> ConvertToIterator(std::shared_ptr<arrow::Table> table, size_t batch_size);
            arrow::Result<std::shared_ptr<arrow::RecordBatch>> ConvertToRecordBatch(const std::vector<rapidjson::Document>& rows, std::shared_ptr<arrow::Schema> schema);
            void setIterator();
            arrow::Result<rapidjson::Document> next();
            int64_t num_rows();
            std::shared_ptr<arrow::StructType> makeChildRecord(const RtlFieldInfo *field);
            arrow::Status FieldToNode(const std::string& name, const RtlFieldInfo *field, std::vector<std::shared_ptr<arrow::Field>> &arrow_fields);
            int countFields(const RtlTypeInfo *typeInfo);
            arrow::Status fieldsToSchema(const RtlTypeInfo *typeInfo);
            void begin_row();
            void end_row(const char * name);
        private:
            int current_row;
            int row_size;                                                       // The maximum size of each parquet row group.
            int current_row_group;                                                // Current RowGroup that has been read from the input file.
            int current_read_row;                                               // Current Row that has been read from the RowGroup
            int num_row_groups;                                                   // The number of row groups in the file that was opened for reading.
            size_t batch_size;                                                  // batch_size for converting Parquet Columns to ECL rows. It is more efficient to break the data into small batches for converting to rows than to convert all at once.
            int64_t numRows;                                                    // The number of result rows that are read from the parquet file. 
            bool partition;                                                     // Boolean variable to track whether we are writing partitioned files or not.
            std::string p_option;                                               // Read, r, Write, w, option for specifying parquet operation.
            std::string p_location;                                             // Location to read parquet file from.
            std::string p_destination;                                          // Destination to write parquet file to.
            std::string p_partDir;                                              // Directory to create for writing partitioned files.
            std::shared_ptr<arrow::Schema> schema;                              // Schema object th
            std::unique_ptr<parquet::arrow::FileWriter> writer;                 // FileWriter for writing to parquet files.
            std::vector<rapidjson::Document> parquet_doc;                       // Document vector for converting rows to columns for writing to parquet files.
            std::vector<rapidjson::Value> row_stack;                            // Stack for keeping track of the context when building a nested row.
            std::shared_ptr<arrow::dataset::Dataset> dataset = nullptr;         // Dataset for holding information of partitioned files. PARTITION
            arrow::dataset::FileSystemDatasetWriteOptions write_options;        // Write options for writing partitioned files. PARTITION
            std::unique_ptr<parquet::arrow::FileReader> parquet_read = nullptr; // Input stream for reading from parquet files.
            std::shared_ptr<arrow::Table> parquet_table = nullptr;              // Table for creating the iterator for outputing result rows.
            arrow::Iterator<rapidjson::Document> output;                        // Arrow iterator to rows read from parquet file.
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
            std::shared_ptr<parquet::schema::GroupNode> getSchema(parquet::schema::NodeVector nodes)
            {
                return std::static_pointer_cast<parquet::schema::GroupNode>(
                    parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, nodes));
            }

            /**
             * @brief Opens the write stream with the schema and destination.
             * 
             */
            void openWriteFile()
            {
                PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(p_destination));

                parquet::WriterProperties::Builder builder;
                // schema = std::static_pointer_cast<parquet::schema::GroupNode>(fieldInfo->schema_root());
                // auto field1 = schema->field(2);          

                // std::shared_ptr<parquet::ParquetFileWriter fw = parquet::ParquetFileWriter::Open(outfile, schema, builder.build());

                // fw->SetMaxRowGroupSize(maxRowSize);

                parquet_write = std::make_shared<parquet::StreamWriter>(parquet::ParquetFileWriter::Open(outfile, schema, builder.build()));
            }

            /**
             * @brief Opens the read stream with the schema and location.
             * 
             */
            void openReadFile()
            {
                // if(partition())
                // {
                //     // Create a filesystem
                //     std::shared_ptr<arrow::fs::LocalFileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();
                //     arrow::fs::FileSelector selector;
                //     selector.base_dir = p_location; // The base directory to be searched is provided by the user in the location option.
                //     selector.recursive = true; // Selector will search the base path recursively for partitioned files.

                //     // Create a file format
                //     std::shared_ptr<arrow::dataset::ParquetFileFormat> format = std::make_shared<arrow::dataset::ParquetFileFormat>();

                //     // Create the partitioning factory.
                //     // TO DO look into other partioning types.
                //     std::shared_ptr<arrow::dataset::PartitioningFactory> partitioning_factory = arrow::dataset::HivePartitioning::MakeFactory();

                //     arrow::dataset::FileSystemFactoryOptions options;
                //     options.partitioning = partitioning_factory;

                //     // Create the dataset factory
                //     PARQUET_ASSIGN_OR_THROW(std::shared_ptr<arrow::dataset::DatasetFactory> dataset_factory, arrow::dataset::FileSystemDatasetFactory::Make(fs, selector, format, options));

                //     // Get dataset
                //     PARQUET_ASSIGN_OR_THROW(dataset, dataset_factory->Finish());
                // }
                // else
                // {
                    PARQUET_ASSIGN_OR_THROW(infile, arrow::io::ReadableFile::Open(p_location));

                    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &parquet_read));
                // }
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
             * @brief Sets the parquet_table member to the output of what is read from the given
             * parquet file.
             */
            void read()
            {
                // if(partition())
                // {
                //     // Create a scanner 
                //     arrow::dataset::ScannerBuilder scanner_builder(dataset);
                //     PARQUET_ASSIGN_OR_THROW(std::shared_ptr<arrow::dataset::Scanner> scanner, scanner_builder.Finish());

                //     // Scan the dataset
                //     PARQUET_ASSIGN_OR_THROW(std::shared_ptr<arrow::Table> table, scanner->ToTable());
                //     numRows = table->num_rows();
                //     parquet_table = table;
                // }
                // else
                // {
                    std::shared_ptr<arrow::Table> out;
                    PARQUET_THROW_NOT_OK(parquet_read->ReadTable(&out));
                    numRows = out->num_rows();
                    parquet_table = out;
                // }
            }

            /**
             * @brief Returns the maximum size of the row group set by the user. Default is 1000.
             * 
             * @return int Maximum size of the row group.
             */
            int getMaxRowSize()
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

            bool partition()
            {
                if (p_option[1])
                    return (p_option[1] == 'M' || p_option[1] == 'm');
                
                return false;
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

            int64_t num_rows()
            {
                return numRows;
            }

            arrow::Status FieldToNode(const std::string& name, const RtlFieldInfo *field, parquet::schema::NodePtr* out) 
            {
                enum parquet::Type::type type;
                enum parquet::ConvertedType::type ctype;
                parquet::Repetition::type repetition = parquet::Repetition::REQUIRED;
                int length = -1;
                unsigned len = field->type->length;

                switch (field->type->getType()) 
                {   
                    case type_boolean:
                        type = parquet::Type::BOOLEAN;
                        break;
                    case type_int:
                        if(len > 4)
                        {
                            type = parquet::Type::INT64;
                            ctype = parquet::ConvertedType::INT_64;
                        }
                        else if(len > 2)
                        {
                            type = parquet::Type::INT32;
                            ctype = parquet::ConvertedType::INT_32;
                        }
                        else if(len > 1)
                        {
                            type = parquet::Type::INT32;
                            ctype = parquet::ConvertedType::INT_16;
                        }
                        else
                        {
                            type = parquet::Type::INT32;
                            ctype = parquet::ConvertedType::INT_8;
                        } 
                        break;
                    case type_unsigned:
                        if(len > 4)
                        {
                            type = parquet::Type::INT64;
                            ctype = parquet::ConvertedType::UINT_64;
                        }
                        else if(len > 2)
                        {
                            type = parquet::Type::INT32;
                            ctype = parquet::ConvertedType::UINT_32;
                        }
                        else if(len > 1)
                        {
                            type = parquet::Type::INT32;
                            ctype = parquet::ConvertedType::UINT_16;
                        }
                        else
                        {
                            type = parquet::Type::INT32;
                            ctype = parquet::ConvertedType::UINT_8;
                        } 
                        break;
                    case type_real:
                        type = parquet::Type::DOUBLE;
                        ctype = parquet::ConvertedType::NONE;
                        break;
                    case type_string:
                        type = parquet::Type::BYTE_ARRAY;
                        ctype = parquet::ConvertedType::UTF8;
                        break;
                    case type_char:
                        type = parquet::Type::FIXED_LEN_BYTE_ARRAY;
                        ctype = parquet::ConvertedType::NONE;
                        length = field->type->length;
                        break;
                    case type_varstring:
                        type = parquet::Type::BYTE_ARRAY;
                        ctype = parquet::ConvertedType::UTF8;
                        break;
                    case type_qstring:
                        type = parquet::Type::BYTE_ARRAY;
                        ctype = parquet::ConvertedType::UTF8;
                        break;
                    case type_unicode:
                        UNSUPPORTED("UNICODE datatype");
                        break;
                    case type_utf8:
                        type = parquet::Type::BYTE_ARRAY;
                        ctype = parquet::ConvertedType::UTF8;
                        break;
                    case type_decimal: 
                        type = parquet::Type::BYTE_ARRAY;
                        ctype = parquet::ConvertedType::DECIMAL;
                        break;    
                    case type_record:      
                    case type_row:
                        // return rowToNodes(field->name, field->type, out);    
                    case type_set: 
                        return listToNodes(field->name, field->type, out);
                    default: 
                        failx("Datatype %i is not compatible with this plugin.", field->type->getType());
                }

                PARQUET_CATCH_NOT_OK(*out = parquet::schema::PrimitiveNode::Make(name, repetition, type, ctype, length));
                return arrow::Status::OK();
            }            

            // arrow::Status MapToNode(const std::string& name, const RtlFieldInfo *field, parquet::schema::NodePtr* out) 
            // {
            //     const RtlFieldInfo *field = field->type->queryFields();
            //     parquet::schema::NodePtr key_node;
            //     RETURN_NOT_OK(FieldToNode("key", ++field, &key_node));

            //     parquet::schema::NodePtr value_node;
            //     RETURN_NOT_OK(FieldToNode("value", ++field, &value_node));

            //     parquet::schema::NodePtr key_value = parquet::schema::GroupNode::Make("key_value", parquet::Repetition::REPEATED, {key_node, value_node});
            //     *out = parquet::schema::GroupNode::Make(name, parquet::Repetition::REPEATED, {key_value}, parquet::LogicalType::Map());
            //     return arrow::Status::OK();
            // }

            int countFields(const RtlTypeInfo *typeInfo)
            {
                const RtlFieldInfo * const *fields = typeInfo->queryFields();
                int count = 0;
                assertex(fields);
                while (*fields++) 
                    count++;

                return count;
            }

            arrow::Status rowToNodes(const char * name, const RtlTypeInfo *typeInfo, parquet::schema::NodePtr* out)
            {
                const RtlFieldInfo * const *fields = typeInfo->queryFields();
                int count = countFields(typeInfo);

                parquet::schema::NodeVector nodes(count);

                for (int i = 0; i < count; i++, fields++) 
                {
                    RETURN_NOT_OK(FieldToNode((*fields)->name, *fields, &nodes[i]));
                }

                auto element = parquet::schema::GroupNode::Make("schema", parquet::Repetition::REPEATED, {nodes});

                *out = parquet::schema::GroupNode::Make(name, parquet::Repetition::OPTIONAL, {element}, parquet::LogicalType::Map());

                return arrow::Status::OK();
            }

            arrow::Status listToNodes(const char * name, const RtlTypeInfo *typeInfo, parquet::schema::NodePtr* out)
            {
                const RtlFieldInfo * const *fields = typeInfo->queryFields();
                int count = countFields(typeInfo);

                parquet::schema::NodeVector nodes(count);

                for (int i = 0; i < count; i++, fields++) 
                {
                    RETURN_NOT_OK(FieldToNode((*fields)->name, *fields, &nodes[i]));
                }

                auto element = parquet::schema::GroupNode::Make("schema", parquet::Repetition::REPEATED, {nodes});

                *out = parquet::schema::GroupNode::Make(name, parquet::Repetition::OPTIONAL, {element}, parquet::LogicalType::List());

                return arrow::Status::OK();
            }

            arrow::Status fieldsToSchema(const RtlTypeInfo *typeInfo)
            {
                const RtlFieldInfo * const *fields = typeInfo->queryFields();
                int count = countFields(typeInfo);

                std::vector<parquet::schema::NodePtr> nodes(count);

                for (int i = 0; i < count; i++, fields++) 
                {
                    RETURN_NOT_OK(FieldToNode((*fields)->name, *fields, &nodes[i]));
                }

                // parquet::schema::NodePtr schema = parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, nodes);
                // fieldInfo = std::make_shared<::parquet::SchemaDescriptor>();
                // PARQUET_CATCH_NOT_OK(fieldInfo->Init(schema));
                schema = getSchema(nodes);
                return arrow::Status::OK();
            }

        private:
            int maxRowSize;                                                     //! The maximum size of each parquet row group.
            size_t batchSize;                                                   //! BatchSize for converting Parquet Columns to ECL rows. It is more efficient to break the data into small batches for converting to rows than to convert all at once.
            int64_t numRows;                                                    //! The number of result rows that are read from the parquet file. 
            std::string p_option;                                               //! Read, r, Write, w, option for specifying parquet operation.
            std::string p_location;                                             //! Location to read parquet file from.
            std::string p_destination;                                          //! Destination to write parquet file to.
            parquet::schema::NodeVector fields;                                 //! Schema vector for appending the information of each field.
            std::shared_ptr<parquet::schema::GroupNode> schema;                 //! GroupNode for utilizing the SchemaDescriptor in opening a file to write to.
            std::shared_ptr<::parquet::SchemaDescriptor> fieldInfo = nullptr;   //! SchemaDescriptor holding field information.
            std::shared_ptr<parquet::StreamWriter> parquet_write = nullptr;     //! Output stream for writing to parquet files.
            // std::shared_ptr<arrow::dataset::Dataset> dataset = nullptr;         //! Dataset for holding information of partitioned files.
            std::shared_ptr<arrow::io::FileOutputStream> outfile = nullptr;     //! Shared pointer to FileOutputStream object.
            std::unique_ptr<parquet::arrow::FileReader> parquet_read = nullptr; //! Input stream for reading from parquet files.
            std::shared_ptr<arrow::Table> parquet_table = nullptr;              //! Table for creating the iterator for outputing result rows.
            std::shared_ptr<arrow::io::ReadableFile> infile = nullptr;          //! Shared pointer to ReadableFile object.
            arrow::Iterator<rapidjson::Document> output;                        //! Arrow iterator to rows read from parquet file.
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
        Linked<IEngineRowAllocator> m_resultAllocator;  //! Pointer to allocator used when building result rows.
        bool m_shouldRead;                              //! If true, we should continue trying to read more messages.
        __int64 m_currentRow;                           //! Current result row.
        int64_t numRows;                                //! Number of result rows read from parquet file.
        std::shared_ptr<ParquetHelper> s_parquet;       //! Shared pointer to ParquetHelper class for the stream class.
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
            partition = _parquet->partSetting();
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
            // There is a better way to do this than creating a stack and having to iterate back through to
            // copy over the members of the rapidjson value. 
            // TO DO
            // Create a json string of all the fields which will be much more performant.
            r_parquet->begin_row();
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
            r_parquet->end_row(field->name);
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
        bool partition;                                 //! Local copy of a boolean so we can know if we are writing partitioned files or not.
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
            // getFieldTypes(_typeInfo);

            reportIfFailure(d_parquet->fieldsToSchema(_typeInfo));
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

        void writeRecordBatch(std::vector<rapidjson::Document> *batch)
        {
            // convert row_batch vector to RecordBatch and write to file.
            arrow::Result<std::shared_ptr<arrow::RecordBatch>> result = d_parquet->ConvertToRecordBatch(*batch, d_parquet->getSchema());
            if(!result.ok())
            {
                failx("Error writing RecordBatch, %s", result.status().message().c_str());
            }
            else
            {
                // Convert to record batch
                std::shared_ptr<arrow::RecordBatch> record_batch = result.ValueOrDie();

                // Write each batch as a row_groups
                arrow::Result<std::shared_ptr<arrow::Table>> table = arrow::Table::FromRecordBatches(d_parquet->getSchema(), {record_batch});

                arrow::Status st = d_parquet->write()->get()->WriteTable(*table.ValueOrDie().get(), record_batch->num_rows());

                if(!st.ok())
                    failx("error writing table to file, %s", st.message().c_str());
            }
        }

        /**
         * @brief Binds all the rows of the dataset and executes the function.
         */
        void executeAll()
        {
            arrow::Status state = d_parquet->openWriteFile();
            if(state != arrow::Status::OK())
                failx("Error opening file to write parquet data. %s", state.message().c_str());

            if(partition)
            {
                while(bindNext())
                {
                    // Add rows to arrow table
                }

                // write table to partition

            }
            else
            {
                int i = 1;
                int row_size = d_parquet->getMaxRowSize();
                for(; bindNext(); d_parquet->update_row(), i++)
                {   
                    if (i % row_size == 0) 
                    {
                        writeRecordBatch(d_parquet->record_batch());
                        parquetembed::jsonAlloc.Clear();
                    }
                }

                if(--i % row_size != 0)
                {
                    d_parquet->record_batch()->resize(i % row_size);
                    writeRecordBatch(d_parquet->record_batch());
                    parquetembed::jsonAlloc.Clear();
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
