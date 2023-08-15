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

#include "parquetembed.hpp"
#include "arrow/result.h"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/schema.h"
#include "arrow/io/api.h"
#include <cmath>

#include "rtlembed.hpp"
#include "rtlds_imp.hpp"

static constexpr const char *MODULE_NAME = "parquet";
static constexpr const char *MODULE_DESCRIPTION = "Parquet Embed Helper";
static constexpr const char *VERSION = "Parquet Embed Helper 1.0.0";
static const char *COMPATIBLE_VERSIONS[] = {VERSION, nullptr};
static const NullFieldProcessor NULLFIELD(NULL);

/**
 * @brief Takes a pointer to an ECLPluginDefinitionBlock and passes in all the important info
 * about the plugin.
 */
extern "C" PARQUETEMBED_PLUGIN_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx))
    {
        ECLPluginDefinitionBlockEx *pbx = (ECLPluginDefinitionBlockEx *)pb;
        pbx->compatibleVersions = COMPATIBLE_VERSIONS;
    }
    else if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = VERSION;
    pb->moduleName = MODULE_NAME;
    pb->ECL = nullptr;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = MODULE_DESCRIPTION;
    return true;
}

namespace parquetembed
{
// //--------------------------------------------------------------------------
// Plugin Classes
//--------------------------------------------------------------------------

/**
 * @brief Throws an exception and gets called when an operation that is unsupported is attempted.
 *
 * @param feature Name of the feature that is currently unsupported.
 */
extern void UNSUPPORTED(const char *feature)
{
    throw MakeStringException(-1, "%s UNSUPPORTED feature: %s not supported in %s", MODULE_NAME, feature, VERSION);
}

/**
 * @brief Exits the program with a failure code and a message to display.
 *
 * @param message Message to display.
 * @param ... Takes any number of arguments that can be inserted into the string using %.
 */
extern void failx(const char *message, ...)
{
    va_list args;
    va_start(args, message);
    StringBuffer msg;
    msg.appendf("%s: ", MODULE_NAME).valist_appendf(message, args);
    va_end(args);
    rtlFail(0, msg.str());
}

/**
 * @brief Exits the program with a failure code and a message to display.
 *
 * @param message Message to display.
 */
extern void fail(const char *message)
{
    StringBuffer msg;
    msg.appendf("%s: ", MODULE_NAME).append(message);
    rtlFail(0, msg.str());
}

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
 * @param _batchSize The size of the batches when converting parquet columns to rows.
 */
ParquetHelper::ParquetHelper(const char *option, const char *_location, const char *destination,
                                int rowsize, int _batchSize, const IThorActivityContext *_activityCtx)
    : p_option(option), location(_location), destination(destination)
{
    row_size = rowsize;
    batch_size = _batchSize;
    activityCtx = _activityCtx;
    rowsProcessed = 0;
    tablesProcessed = 0;

    pool = arrow::default_memory_pool();

    parquet_doc = std::vector<rapidjson::Document>(rowsize);
    current_row = 0;

    partition = strlen(option) > 5;
}

ParquetHelper::~ParquetHelper()
{
    pool->ReleaseUnused();
    jsonAlloc.Clear();
}

/**
 * @brief Get the Schema shared pointer
 *
 * @return std::shared_ptr<arrow::Schema> Shared_ptr of schema object for building the write stream.
 */
std::shared_ptr<arrow::Schema> ParquetHelper::getSchema()
{
    return schema;
}

/**
 * @brief Opens the write stream with the schema and destination. T
 *
 */
arrow::Status ParquetHelper::openWriteFile()
{
    if (destination == "")
        failx("Invalid option: The destination was not supplied.");

    if (partition)
    {
        ARROW_ASSIGN_OR_RAISE(auto filesystem, arrow::fs::FileSystemFromUriOrPath(destination));
        reportIfFailure(filesystem->DeleteDirContents(destination));
        auto partition_schema = arrow::schema({schema->field(5)});

        auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
        auto partitioning = std::make_shared<arrow::dataset::HivePartitioning>(partition_schema);

        write_options.file_write_options = format->DefaultWriteOptions();
        write_options.filesystem = filesystem;
        write_options.base_dir = destination;
        write_options.partitioning = partitioning;
        write_options.existing_data_behavior = arrow::dataset::ExistingDataBehavior::kOverwriteOrIgnore;
    }
    else
    {
        // Currently under the assumption that all channels and workers are given a worker id and no matter
        // the configuration will show up in activityCtx->numSlaves()
        if (activityCtx->numSlaves() > 1)
        {
            destination.insert(destination.find(".parquet"), std::to_string(activityCtx->querySlave()));
        }

        std::shared_ptr<arrow::io::FileOutputStream> outfile;

        PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(destination));

        // Choose compression
        // TO DO let the user choose a compression
        std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder().compression(arrow::Compression::UNCOMPRESSED)->build();

        // Create a writer
        reportIfFailure(parquet::arrow::FileWriter::Open(*schema.get(), pool, outfile, props, &writer));
    }
    return arrow::Status::OK();
}

/**
 * @brief Opens the read stream with the schema and location.
 *
 */
arrow::Status ParquetHelper::openReadFile()
{
    if (partition)
    {
        // Create a filesystem
        std::shared_ptr<arrow::fs::FileSystem> fs;
        ARROW_ASSIGN_OR_RAISE(fs, arrow::fs::FileSystemFromUriOrPath(location));

        // FileSelector allows traversal of multi-file dataset
        arrow::fs::FileSelector selector;
        selector.base_dir = location; // The base directory to be searched is provided by the user in the location option.
        selector.recursive = true;    // Selector will search the base path recursively for partitioned files.

        // Create a file format
        std::shared_ptr<arrow::dataset::ParquetFileFormat> format = std::make_shared<arrow::dataset::ParquetFileFormat>();

        arrow::dataset::FileSystemFactoryOptions options;
        options.partitioning = arrow::dataset::HivePartitioning::MakeFactory(); // TODO set other partitioning types

        // Create the dataset factory
        PARQUET_ASSIGN_OR_THROW(auto dataset_factory, arrow::dataset::FileSystemDatasetFactory::Make(fs, selector, format, options));

        // Get scanner
        PARQUET_ASSIGN_OR_THROW(auto dataset, dataset_factory->Finish());
        ARROW_ASSIGN_OR_RAISE(auto scan_builder, dataset->NewScan());
        reportIfFailure(scan_builder->Pool(pool));
        ARROW_ASSIGN_OR_RAISE(scanner, scan_builder->Finish());
    }
    else
    {
        // Currently under the assumption that all channels and workers are given a worker id and no matter
        // the configuration will show up in activityCtx->numSlaves()
        if (activityCtx->numSlaves() > 1)
        {
            location.insert(location.find(".parquet"), std::to_string(activityCtx->querySlave()));
        }
        ARROW_ASSIGN_OR_RAISE(auto input, arrow::io::ReadableFile::Open(location));
        reportIfFailure(parquet::arrow::OpenFile(input, pool, &parquet_read));
    //     auto reader_properties = parquet::ReaderProperties(pool);
    //     auto arrow_reader_props = parquet::ArrowReaderProperties();
    //     parquet::arrow::FileReaderBuilder reader_builder;
    //     reportIfFailure(reader_builder.OpenFile(location, false, reader_properties));
    //     reader_builder.memory_pool(pool);
    //     reader_builder.properties(arrow_reader_props);
    //     ARROW_ASSIGN_OR_RAISE(parquet_read, reader_builder.Build());
    }
    return arrow::Status::OK();
}

arrow::Status ParquetHelper::writePartition(std::shared_ptr<arrow::Table> table)
{
    // Create dataset for writing partitioned files.
    auto dataset = std::make_shared<arrow::dataset::InMemoryDataset>(table);

    StringBuffer basename_template;
    basename_template.appendf("part{i}_%lld.parquet", tablesProcessed++);
    write_options.basename_template = basename_template.str();

    {
        CriticalBlock block(fileLock);

        ARROW_ASSIGN_OR_RAISE(auto scanner_builder, dataset->NewScan());
        reportIfFailure(scanner_builder->Pool(pool));
        ARROW_ASSIGN_OR_RAISE(auto scanner, scanner_builder->Finish());

        // Write partitioned files.
        reportIfFailure(arrow::dataset::FileSystemDataset::Write(write_options, scanner));
    }

    return arrow::Status::OK();
}

/**
 * @brief Returns a pointer to the stream writer for writing to the destination.
 *
 * @return
 */
std::unique_ptr<parquet::arrow::FileWriter> *ParquetHelper::write()
{
    return &writer;
}

/**
 * @brief Returns a pointer to the top of the stack for the current row being built.
 *
 * @return A rapidjson::Value containing the row
 */
rapidjson::Value *ParquetHelper::doc()
{
    return &row_stack[row_stack.size() - 1];
}

/**
 * @brief A helper method for updating the current row on writes and keeping
 * it within the boundary of the row_size set by the user when creating RowGroups.
 */
void ParquetHelper::update_row()
{
    if (++current_row == row_size)
        current_row = 0;
}

std::vector<rapidjson::Document> *ParquetHelper::record_batch()
{
    return &parquet_doc;
}

/**
 * @brief Divide row groups being read from a parquet file among any number of thor workers. If running hthor all row groups are assigned to it. This function
 * will handle all cases where the number of groups is greater than, less than or divisible by the number of thor workers.
 */
void divide_row_groups(const IThorActivityContext *activityCtx, __int64 total_row_groups, __int64 &num_row_groups, __int64 &start_row_group)
{
    int workers = activityCtx->numSlaves();
    int strands = activityCtx->numStrands();
    int worker_id = activityCtx->querySlave();

    // Currently under the assumption that all channels and workers are given a worker id and no matter
    // the configuration will show up in activityCtx->numSlaves()
    if (workers > 1)
    {
        // If the number of workers goes into total_row_groups evenly then every worker gets the same amount
        // of rows to read
        if (total_row_groups % workers == 0)
        {
            num_row_groups = total_row_groups / workers;
            start_row_group = num_row_groups * worker_id;
        }
        // If the total_row_groups is not evenly divisible by the number of workers then we divide them up
        // with the first n-1 workers getting slightly more and the nth worker gets the remainder
        else if (total_row_groups > workers)
        {
            if (worker_id == (workers - 1))
            {
                num_row_groups = total_row_groups - (std::ceil((float)total_row_groups / workers) * (workers - 1));
                start_row_group = std::ceil((float)total_row_groups / workers) * worker_id;
            }
            else
            {
                num_row_groups = std::ceil((float)total_row_groups / workers);
                start_row_group = num_row_groups * worker_id;
            }
        }
        // If the number of total_row_groups is less than the number of workers we give as many as possible
        // a single row group to read.
        else
        {
            if (worker_id < total_row_groups)
            {
                num_row_groups = 1;
                start_row_group = worker_id;
            }
            else
            {
                num_row_groups = 0;
                start_row_group = 0;
            }
        }
    }
    else
    {
        // There is only one worker
        num_row_groups = total_row_groups;
        start_row_group = 0;
    }
}

void ParquetHelper::chunkTable(std::shared_ptr<arrow::Table> &table)
{
    auto columns = table->columns();
    parquet_table.clear();
    for (int i = 0; i < columns.size(); i++)
    {
        parquet_table.insert(std::make_pair(table->field(i)->name(), columns[i]->chunk(0)));
    }
}

/**
 * @brief Sets the parquet_table member to the output of what is read from the given
 * parquet file.
 */
void ParquetHelper::read()
{
    if (partition)
    {
        // rowsProcessed starts at zero and we read in batches until it is equal to rowsCount
        rowsProcessed = 0;
        PARQUET_ASSIGN_OR_THROW(rbatch_reader, scanner->ToRecordBatchReader());
        rbatch_itr = arrow::RecordBatchReader::RecordBatchReaderIterator(rbatch_reader.get());
        // Divide the work among any number of workers
        PARQUET_ASSIGN_OR_THROW(auto batch, *rbatch_itr);
        PARQUET_ASSIGN_OR_THROW(float total_rows, scanner->CountRows());
        batch_size = batch->num_rows();
        divide_row_groups(activityCtx, std::ceil(total_rows / batch_size), tableCount, start_row_group);
        if (tableCount != 0)
        {
            std::shared_ptr<arrow::Table> table;
            PARQUET_ASSIGN_OR_THROW(table, queryRows());
            rowsCount = table->num_rows();
            chunkTable(table);
            tablesProcessed++;
        }
        else
        {
            rowsCount = 0;
        }
    }
    else
    {
        // int total_row_groups = parquet_read->num_row_groups();
        // divide_row_groups(activityCtx, total_row_groups, tableCount, start_row_group);
        tableCount = parquet_read->num_row_groups();
        start_row_group = 0;
        rowsProcessed = 0;
        if (tableCount != 0)
        {
            std::shared_ptr<arrow::Table> table;
            reportIfFailure(parquet_read->RowGroup(tablesProcessed + start_row_group)->ReadTable(&table));
            rowsCount = table->num_rows();
            chunkTable(table);
            tablesProcessed++;
        }
        else
        {
            rowsCount = 0;
        }
    }
}

/**
 * @brief Returns a boolean so we know if we are writing partitioned files.
 *
 * @return true If we are partitioning.
 * @return false If we are writing a single file.
 */
bool ParquetHelper::partSetting()
{
    return partition;
}

/**
 * @brief Returns the maximum size of the row group set by the user. Default is 1000.
 *
 * @return int Maximum size of the row group.
 */
__int64 ParquetHelper::getMaxRowSize()
{
    return row_size;
}

char ParquetHelper::options()
{
    if (p_option[0] == 'W' || p_option[0] == 'w')
    {
        return 'w';
    }
    else if (p_option[0] == 'R' || p_option[0] == 'r')
    {
        return 'r';
    }
    else
    {
        failx("Invalid options parameter.");
    }
}

/**
 * @brief Checks if all the rows have been read and if reading a single file all of the
 * RowGroups as well.
 *
 * @return True if there are more rows to be read and false if else.
 */
bool ParquetHelper::shouldRead()
{
    return !(tablesProcessed >= tableCount && rowsProcessed >= rowsCount);
}

__int64 &ParquetHelper::getRowsProcessed()
{
    return rowsProcessed;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ParquetHelper::ConvertToRecordBatch(
    const std::vector<rapidjson::Document> &rows, std::shared_ptr<arrow::Schema> schema)
{
    // RecordBatchBuilder will create array builders for us for each field in our
    // schema. By passing the number of output rows (`rows.size()`) we can
    // pre-allocate the correct size of arrays, except of course in the case of
    // string, byte, and list arrays, which have dynamic lengths.
    std::unique_ptr<arrow::RecordBatchBuilder> batch_builder;
    ARROW_ASSIGN_OR_RAISE(
        batch_builder,
        arrow::RecordBatchBuilder::Make(schema, pool, rows.size()));

    // Inner converter will take rows and be responsible for appending values
    // to provided array builders.
    JsonValueConverter converter(rows);
    for (int i = 0; i < batch_builder->num_fields(); ++i)
    {
        std::shared_ptr<arrow::Field> field = schema->field(i);
        arrow::ArrayBuilder *builder = batch_builder->GetField(i);
        ARROW_RETURN_NOT_OK(converter.Convert(*field.get(), builder));
    }

    std::shared_ptr<arrow::RecordBatch> batch;
    ARROW_ASSIGN_OR_RAISE(batch, batch_builder->Flush());

    // Use RecordBatch::ValidateFull() to make sure arrays were correctly constructed.
    reportIfFailure(batch->ValidateFull());
    return batch;
}

arrow::Result<std::shared_ptr<arrow::Table>> ParquetHelper::queryRows()
{
    if (tablesProcessed == 0)
    {
        int offset = 0;
        while (offset < start_row_group)
        {
            rbatch_itr++;
            offset++;
        }
    }
    PARQUET_ASSIGN_OR_THROW(auto batch, *rbatch_itr);
    rbatch_itr++;
    std::vector<std::shared_ptr<arrow::RecordBatch>> to_table = {batch};
    return std::move(arrow::Table::FromRecordBatches(std::move(to_table)));
}

std::unordered_map<std::string, std::shared_ptr<arrow::Array>> *ParquetHelper::next()
{
    if (rowsProcessed == rowsCount)
    {
        if (partition)
        {
            // rowsProcessed starts at zero and we read in batches until it is equal to rowsCount
            rowsProcessed = 0;
            tablesProcessed++;
            std::shared_ptr<arrow::Table> table;
            PARQUET_ASSIGN_OR_THROW(table, queryRows());
            rowsCount = table->num_rows();
            chunkTable(table);
        }
        else
        {
            std::shared_ptr<arrow::Table> table;
            reportIfFailure(parquet_read->RowGroup(tablesProcessed + start_row_group)->ReadTable(&table));
            rowsProcessed = 0;
            tablesProcessed++;
            rowsCount = table->num_rows();
            chunkTable(table);
        }
    }
    return &parquet_table;
}

__int64 ParquetHelper::num_rows()
{
    return rowsCount;
}

/**
 * @brief Creates the child record for an array or dataset type. This method is used for converting
 * the ECL RtlFieldInfo object into arrow::Fields for creating a rapidjson document object.
 *
 * @param field The field containing metadata for the record.
 *
 * @returns An arrow::Structype holding the schema and fields of the child records.
 */
std::shared_ptr<arrow::NestedType> ParquetHelper::makeChildRecord(const RtlFieldInfo *field)
{
    const RtlTypeInfo *typeInfo = field->type;
    const RtlFieldInfo *const *fields = typeInfo->queryFields();
    // Create child fields
    if (fields)
    {
        int count = countFields(typeInfo);

        std::vector<std::shared_ptr<arrow::Field>> child_fields;

        for (int i = 0; i < count; i++, fields++)
        {
            reportIfFailure(FieldToNode((*fields)->name, *fields, child_fields));
        }

        return std::make_shared<arrow::StructType>(child_fields);
    }
    else
    {
        // Create set
        const RtlTypeInfo *child = typeInfo->queryChildType();
        const RtlFieldInfo childField = RtlFieldInfo("", "", child);
        std::vector<std::shared_ptr<arrow::Field>> child_field;
        reportIfFailure(FieldToNode(childField.name, &childField, child_field));
        return std::make_shared<arrow::ListType>(child_field[0]);
    }
}

/**
 * @brief Converts an RtlFieldInfo object into an arrow field and adds it to the output vector.
 *
 * @param name The name of the field
 *
 * @param field The field containing metadata for the record.
 *
 * @param arrow_fields Output vector for pushing new nodes to.
 *
 * @return Status of the operation
 */
arrow::Status ParquetHelper::FieldToNode(const std::string &name, const RtlFieldInfo *field, std::vector<std::shared_ptr<arrow::Field>> &arrow_fields)
{
    unsigned len = field->type->length;

    switch (field->type->getType())
    {
    case type_boolean:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::boolean()));
        break;
    case type_int:
        if (field->type->isSigned())
        {
            if (len > 4)
            {
                arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::int64()));
            }
            else
            {
                arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::int32()));
            }
        }
        else
        {
            if (len > 4)
            {
                arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::uint64()));
            }
            else
            {
                arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::uint32()));
            }
        }
        break;
    case type_real:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::float64()));
        break;
    case type_string:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::utf8()));
        break;
    case type_char:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::utf8()));
        break;
    case type_varstring:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::utf8()));
        break;
    case type_qstring:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::utf8()));
        break;
    case type_unicode:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::utf8()));
        break;
    case type_utf8:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::utf8()));
        break;
    case type_decimal:
        // The second parameter, scale, is the number of digits after the decimal point.
        // I am not sure if the eclhelper function getDecimalDigits() returns the digits after the decimal point or the total digits.
        // TO DO
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::decimal128(field->type->getDecimalPrecision(), field->type->getDecimalDigits())));
        break;
    case type_data:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, arrow::large_binary()));
        break;
    case type_record:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, makeChildRecord(field)));
        break;
    case type_set:
        arrow_fields.push_back(std::make_shared<arrow::Field>(name, makeChildRecord(field)));
        break;
    default:
        failx("Datatype %i is not compatible with this plugin.", field->type->getType());
    }

    return arrow::Status::OK();
}

/**
 * @brief counts the number of fields in the input dataset
 */
int ParquetHelper::countFields(const RtlTypeInfo *typeInfo)
{
    const RtlFieldInfo *const *fields = typeInfo->queryFields();
    int count = 0;
    assertex(fields);
    while (*fields++)
        count++;

    return count;
}

/**
 * @brief Creates an arrow::Schema from the field info of the row.
 * @param typeInfo An RtlTypeInfo object that we iterate through to get all
 * the information for the row.
 */
arrow::Status ParquetHelper::fieldsToSchema(const RtlTypeInfo *typeInfo)
{
    const RtlFieldInfo *const *fields = typeInfo->queryFields();
    int count = countFields(typeInfo);

    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;

    for (int i = 0; i < count; i++, fields++)
    {
        RETURN_NOT_OK(FieldToNode((*fields)->name, *fields, arrow_fields));
    }

    schema = std::make_shared<arrow::Schema>(arrow_fields);
    return arrow::Status::OK();
}

/**
 * @brief Creates a rapidjson::Value and adds it to the stack
 */
void ParquetHelper::begin_set()
{
    rapidjson::Value row(rapidjson::kArrayType);
    row_stack.push_back(std::move(row));
}

/**
 * @brief Creates a rapidjson::Value and adds it to the stack
 */
void ParquetHelper::begin_row()
{
    rapidjson::Value row(rapidjson::kObjectType);
    row_stack.push_back(std::move(row));
}

/**
 * @brief Removes the value from the top of the stack and adds it the parent row.
 * If there is only one value on the stack then it converts it to a rapidjson::Document.
 */
void ParquetHelper::end_row(const char *name)
{
    if (row_stack.size() > 1)
    {
        rapidjson::Value child = std::move(row_stack[row_stack.size() - 1]);
        row_stack.pop_back();
        row_stack[row_stack.size() - 1].AddMember(rapidjson::StringRef(name), child, jsonAlloc);
    }
    else
    {
        parquet_doc[current_row].SetObject();

        rapidjson::Value parent = std::move(row_stack[row_stack.size() - 1]);
        row_stack.pop_back();

        for (auto itr = parent.MemberBegin(); itr != parent.MemberEnd(); ++itr)
        {
            parquet_doc[current_row].AddMember(itr->name, itr->value, jsonAlloc);
        }
    }
}

ParquetRowStream::ParquetRowStream(IEngineRowAllocator *_resultAllocator, std::shared_ptr<ParquetHelper> _parquet)
    : m_resultAllocator(_resultAllocator), s_parquet(_parquet)
{
    m_currentRow = 0;
    m_shouldRead = true;
    rowsCount = _parquet->num_rows();
    array_visitor = std::make_shared<ParquetArrayVisitor>();
}

const void *ParquetRowStream::nextRow()
{
    if (m_shouldRead && s_parquet->shouldRead())
    {
        auto table = s_parquet->next();
        m_currentRow++;

        if (table)
        {
            ParquetRowBuilder pRowBuilder(table, s_parquet->getRowsProcessed()++, &array_visitor);

            RtlDynamicRowBuilder rowBuilder(m_resultAllocator);
            const RtlTypeInfo *typeInfo = m_resultAllocator->queryOutputMeta()->queryTypeInfo();
            assertex(typeInfo);
            RtlFieldStrInfo dummyField("<row>", NULL, typeInfo);
            size32_t len = typeInfo->build(rowBuilder, 0, &dummyField, pRowBuilder);
            return rowBuilder.finalizeRowClear(len);
        }
        else
            failx("Error processing result row");
    }
    return nullptr;
}

void ParquetRowStream::stop()
{
    m_resultAllocator.clear();
    m_shouldRead = false;
}

void ParquetRowBuilder::xpathOrName(StringBuffer &outXPath, const RtlFieldInfo *field) const
{
    outXPath.clear();

    if (field->xpath)
    {
        if (field->xpath[0] == xpathCompoundSeparatorChar)
        {
            outXPath.append(field->xpath + 1);
        }
        else
        {
            const char *sep = strchr(field->xpath, xpathCompoundSeparatorChar);

            if (!sep)
            {
                outXPath.append(field->xpath);
            }
            else
            {
                outXPath.append(field->xpath, 0, static_cast<size32_t>(sep - field->xpath));
            }
        }
    }
    else
    {
        outXPath.append(field->name);
    }
}

/**
 * @brief Gets a Boolean result for an ECL Row
 *
 * @param field Holds the value of the field.
 * @return bool Returns the boolean value from the result row.
 */
bool ParquetRowBuilder::getBooleanResult(const RtlFieldInfo *field)
{
    nextField(field);

    if ((*array_visitor)->type == 0)
    {
        NullFieldProcessor p(field);
        return p.boolResult;
    }
    if ((*array_visitor)->type != 1)
    {
        failx("Incorrect type for field %s.", field->name);
    }
    auto i = !m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet ? m_pathStack.back().childrenProcessed++ : currentRow;
    return (*array_visitor)->bool_arr->Value(i);
}

/**
 * @brief Gets a data result from the result row and passes it back to engine through result.
 *
 * @param field Holds the value of the field.
 * @param len Length of the Data value.
 * @param result Used for returning the result to the caller.
 */
void ParquetRowBuilder::getDataResult(const RtlFieldInfo *field, size32_t &len, void *&result)
{
    nextField(field);

    if ((*array_visitor)->type == 0)
    {
        NullFieldProcessor p(field);
        rtlUtf8ToDataX(len, result, p.resultChars, p.stringResult);
        return;
    }
    if ((*array_visitor)->type == 6)
    {
        auto i = !m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet ? m_pathStack.back().childrenProcessed++ : currentRow;
        auto view = (*array_visitor)->large_bin_arr->GetView(i);
        rtlUtf8ToDataX(len, result, rtlUtf8Length(view.size(), view.data()), view.data());
        return;
    }
    else
    {
        failx("Incorrect type for field %s.", field->name);
    }
}

/**
 * @brief Gets a real result from the result row.
 *
 * @param field Holds the value of the field.
 * @return double Double value to return.
 */
double ParquetRowBuilder::getRealResult(const RtlFieldInfo *field)
{
    nextField(field);

    if ((*array_visitor)->type == 0)
    {
        NullFieldProcessor p(field);
        return p.doubleResult;
    }
    if ((*array_visitor)->type != 10)
    {
        failx("Incorrect type for field %s.", field->name);
    }
    auto i = !m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet ? m_pathStack.back().childrenProcessed++ : currentRow;
    return (*array_visitor)->double_arr->Value(i);
}

__int64 getSigned(std::shared_ptr<ParquetArrayVisitor> *array_visitor, int index)
{
    switch ((*array_visitor)->size)
    {
        case 8:
            return (*array_visitor)->int8_arr->Value(index);
        case 16:
            return (*array_visitor)->int16_arr->Value(index);
        case 32:
            return (*array_visitor)->int32_arr->Value(index);
        case 64:
            return (*array_visitor)->int64_arr->Value(index);
        default:
            failx("getSigned: Invalid size %i", (*array_visitor)->size);
    }
}

unsigned __int64 getUnsigned(std::shared_ptr<ParquetArrayVisitor> *array_visitor, int index)
{
    switch ((*array_visitor)->size)
    {
        case 8:
            return (*array_visitor)->uint8_arr->Value(index);
        case 16:
            return (*array_visitor)->uint16_arr->Value(index);
        case 32:
            return (*array_visitor)->uint32_arr->Value(index);
        case 64:
            return (*array_visitor)->uint64_arr->Value(index);
        default:
            failx("getUnsigned: Invalid size %i", (*array_visitor)->size);
    }
}

/**
 * @brief Gets the Signed Integer result from the result row.
 *
 * @param field Holds the value of the field.
 * @return __int64 Value to return.
 */
__int64 ParquetRowBuilder::getSignedResult(const RtlFieldInfo *field)
{
    nextField(field);

    if ((*array_visitor)->type == 0)
    {
        NullFieldProcessor p(field);
        return p.uintResult;
    }
    if ((*array_visitor)->type != 2)
    {
        failx("Incorrect type for field %s.", field->name);
    }
    auto i = !m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet ? m_pathStack.back().childrenProcessed++ : currentRow;
    return getSigned(array_visitor, i);
}

/**
 * @brief Gets the Unsigned Integer result from the result row.
 *
 * @param field Holds the value of the field.
 * @return unsigned Value to return.
 */
unsigned __int64 ParquetRowBuilder::getUnsignedResult(const RtlFieldInfo *field)
{
    nextField(field);

    if ((*array_visitor)->type == 0)
    {

        NullFieldProcessor p(field);
        return p.uintResult;
    }
    if ((*array_visitor)->type != 3)
    {
        failx("Incorrect type for field %s.", field->name);
    }
    auto i = !m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet ? m_pathStack.back().childrenProcessed++ : currentRow;
    return getUnsigned(array_visitor, i);
}

/**
 * @brief Gets a String from the result row.
 *
 * @param field Holds the value of the field.
 * @param chars Number of chars in the String.
 * @param result Variable used for returning string back to the caller.
 */
void ParquetRowBuilder::getStringResult(const RtlFieldInfo *field, size32_t &chars, char *&result)
{
    nextField(field);

    if ((*array_visitor)->type == 0)
    {
        NullFieldProcessor p(field);
        rtlUtf8ToStrX(chars, result, p.resultChars, p.stringResult);
        return;
    }
    if ((*array_visitor)->type == 5)
    {
        auto i = !m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet ? m_pathStack.back().childrenProcessed++ : currentRow;
        auto view = (*array_visitor)->string_arr->GetView(i);
        rtlStrToStrX(chars, result, view.size(), view.data());
        return;
    }
    else
    {
        failx("Incorrect type for field %s.", field->name);
    }
}

/**
 * @brief Gets a UTF8 from the result row.
 *
 * @param field Holds the value of the field.
 * @param chars Number of chars in the UTF8.
 * @param result Variable used for returning UTF8 back to the caller.
 */
void ParquetRowBuilder::getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char *&result)
{
    nextField(field);

    if ((*array_visitor)->type == 0)
    {
        NullFieldProcessor p(field);
        rtlUtf8ToUtf8X(chars, result, p.resultChars, p.stringResult);
        return;
    }
    if ((*array_visitor)->type == 5)
    {
        auto i = !m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet ? m_pathStack.back().childrenProcessed++ : currentRow;
        auto view = (*array_visitor)->string_arr->GetView(i);
        unsigned numchars = rtlUtf8Length(view.size(), view.data());
        rtlUtf8ToUtf8X(chars, result, numchars, view.data());
        return;
    }
    else
    {
        failx("Incorrect type for field %s.", field->name);
    }
}

/**
 * @brief Gets a Unicode from the result row.
 *
 * @param field Holds the value of the field.
 * @param chars Number of chars in the Unicode.
 * @param result Variable used for returning Unicode back to the caller.
 */
void ParquetRowBuilder::getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar *&result)
{
    nextField(field);

    if ((*array_visitor)->type == 0)
    {
        NullFieldProcessor p(field);
        rtlUnicodeToUnicodeX(chars, result, p.resultChars, p.unicodeResult);
        return;
    }
    if ((*array_visitor)->type == 5)
    {
        auto i = !m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet ? m_pathStack.back().childrenProcessed++ : currentRow;
        auto view = (*array_visitor)->string_arr->GetView(i);
        unsigned numchars = rtlUtf8Length(view.size(), view.data());
        rtlUtf8ToUnicodeX(chars, result, numchars, view.data());
        return;
    }
    else
    {
        failx("Incorrect type for field %s.", field->name);
    }
}

/**
 * @brief Gets a decimal from the result row.
 *
 * @param field Holds the value of the field.
 * @param value Variable used for returning decimal to caller.
 */
void ParquetRowBuilder::getDecimalResult(const RtlFieldInfo *field, Decimal &value)
{
    nextField(field);

    if ((*array_visitor)->type == 0)
    {
        NullFieldProcessor p(field);
        value.set(p.decimalResult);
        return;
    }
    if ((*array_visitor)->type == 7)
    {
        auto i = !m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet ? m_pathStack.back().childrenProcessed++ : currentRow;
        auto dvalue = (*array_visitor)->dec_arr->GetView(i);
        value.setString(dvalue.size(), dvalue.data());
        RtlDecimalTypeInfo *dtype = (RtlDecimalTypeInfo *)field->type;
        value.setPrecision(dtype->getDecimalDigits(), dtype->getDecimalPrecision());
        return;
    }
    else
    {
        failx("Incorrect type for field %s.", field->name);
    }
}

/**
 * @brief Starts a new Set.
 *
 * @param field Field with information about the context of the set.
 * @param isAll Not Supported.
 */
void ParquetRowBuilder::processBeginSet(const RtlFieldInfo *field, bool &isAll)
{
    isAll = false; // ALL not supported
    nextField(field);

    if ((*array_visitor)->type == 8)
    {
        PathTracker newPathNode(field->name, (*array_visitor)->list_arr, CPNTSet);
        newPathNode.childCount = (*array_visitor)->list_arr->value_slice(currentRow)->length();
        m_pathStack.push_back(newPathNode);
    }
    else
    {
        failx("Incorrect type for field %s.", field->name);
    }
}

/**
 * @brief Checks if we should process another set.
 *
 * @param field Context information about the set.
 * @return true If the children that we have process is less than the total child count.
 * @return false If all the children sets have been processed.
 */
bool ParquetRowBuilder::processNextSet(const RtlFieldInfo *field)
{
    return m_pathStack.back().childrenProcessed < m_pathStack.back().childCount;
}

/**
 * @brief Starts a new Dataset.
 *
 * @param field Information about the context of the dataset.
 */
void ParquetRowBuilder::processBeginDataset(const RtlFieldInfo *field)
{
    UNSUPPORTED("Nested Dataset type is unsupported.");
}

/**
 * @brief Starts a new Row.
 *
 * @param field Information about the context of the row.
 */
void ParquetRowBuilder::processBeginRow(const RtlFieldInfo *field)
{
    StringBuffer xpath;
    xpathOrName(xpath, field);

    if (!xpath.isEmpty())
    {
        if (strncmp(xpath, "<row>", 5) != 0)
        {
            nextField(field);
            if ((*array_visitor)->type == 9)
            {
                m_pathStack.push_back(PathTracker(field->name, (*array_visitor)->struct_arr, CPNTScalar));
            }
            else
            {
                failx("proccessBeginRow: Incorrect type for row.");
            }
        }
    }
    else
    {
        failx("processBeginRow: Field name or xpath missing");
    }
}

/**
 * @brief Checks whether we should process the next row.
 *
 * @param field Information about the context of the row.
 * @return true If the number of child rows process is less than the total count of children.
 * @return false If all of the child rows have been processed.
 */
bool ParquetRowBuilder::processNextRow(const RtlFieldInfo *field)
{
    return m_pathStack.back().childrenProcessed < m_pathStack.back().childCount;
}

/**
 * @brief Ends a set.
 *
 * @param field Information about the context of the set.
 */
void ParquetRowBuilder::processEndSet(const RtlFieldInfo *field)
{
    StringBuffer xpath;
    xpathOrName(xpath, field);

    if (!xpath.isEmpty() && !m_pathStack.empty() && strcmp(xpath.str(), m_pathStack.back().nodeName) == 0)
    {
        m_pathStack.pop_back();
    }
}

/**
 * @brief Ends a dataset.
 *
 * @param field Information about the context of the dataset.
 */
void ParquetRowBuilder::processEndDataset(const RtlFieldInfo *field)
{
    UNSUPPORTED("Nested Dataset type is unsupported.");
}

/**
 * @brief Ends a row.
 *
 * @param field Information about the context of the row.
 */
void ParquetRowBuilder::processEndRow(const RtlFieldInfo *field)
{
    StringBuffer xpath;
    xpathOrName(xpath, field);

    if (!xpath.isEmpty())
    {
        if (!m_pathStack.empty())
        {
            if (m_pathStack.back().nodeType == CPNTDataset)
            {
                m_pathStack.back().childrenProcessed++;
            }
            else if (strcmp(xpath.str(), m_pathStack.back().nodeName) == 0)
            {
                m_pathStack.pop_back();
            }
        }
    }
    else
    {
        failx("processEndRow: Field name or xpath missing");
    }
}

void ParquetRowBuilder::nextFromStruct(const RtlFieldInfo *field)
{
    auto structPtr = m_pathStack.back().structPtr;
    reportIfFailure(structPtr->Accept((*array_visitor).get()));
    if (m_pathStack.back().nodeType == CPNTScalar)
    {
        auto child = (*array_visitor)->struct_arr->GetFieldByName(field->name);
        reportIfFailure(child->Accept((*array_visitor).get()));
    }
    else if (m_pathStack.back().nodeType == CPNTSet)
    {
        auto child = (*array_visitor)->list_arr->value_slice(currentRow);
        reportIfFailure(child->Accept((*array_visitor).get()));
    }
}

/**
 * @brief Gets the next field and processes it.
 *
 * @param field Information about the context of the next field.
 * @return const char* Result of building field.
 */
void ParquetRowBuilder::nextField(const RtlFieldInfo *field)
{
    if (!field->name)
    {
        failx("Field name is empty.");
    }
    if (m_pathStack.size() > 0)
    {
        nextFromStruct(field);
        return;
    }
    auto column = result_rows->find(field->name);
    if (column != result_rows->end())
    {
        reportIfFailure(column->second->Accept((*array_visitor).get()));
        return;
    }
}

unsigned ParquetRecordBinder::checkNextParam(const RtlFieldInfo *field)
{
    if (logctx.queryTraceLevel() > 4)
        logctx.CTXLOG("Binding %s to %d", field->name, thisParam);
    return thisParam++;
}

int ParquetRecordBinder::numFields()
{
    int count = 0;
    const RtlFieldInfo *const *fields = typeInfo->queryFields();
    assertex(fields);
    while (*fields++)
        count++;
    return count;
}

void addMember(std::shared_ptr<ParquetHelper> r_parquet, rapidjson::Value &key, rapidjson::Value &value)
{
    rapidjson::Value *row = r_parquet->doc();
    if (row->GetType() == rapidjson::kObjectType)
        row->AddMember(key, value, jsonAlloc);
    else
        row->PushBack(value, jsonAlloc);
}

/**
 * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
 *
 * @param len Number of chars in value.
 * @param value pointer to value of parameter.
 * @param field RtlFieldInfo holds meta information about the embed context.
 * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
 */
void bindStringParam(unsigned len, const char *value, const RtlFieldInfo *field, std::shared_ptr<ParquetHelper> r_parquet)
{
    size32_t utf8chars;
    rtlDataAttr utf8;
    rtlStrToUtf8X(utf8chars, utf8.refstr(), len, value);

    rapidjson::Value key = rapidjson::Value(field->name, jsonAlloc);
    rapidjson::Value val = rapidjson::Value(std::string(utf8.getstr(), rtlUtf8Size(utf8chars, utf8.getdata())), jsonAlloc);

    addMember(r_parquet, key, val);
}

/**
 * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
 *
 * @param value pointer to value of parameter.
 * @param field RtlFieldInfo holds meta information about the embed context.
 * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
 */
void bindBoolParam(bool value, const RtlFieldInfo *field, std::shared_ptr<ParquetHelper> r_parquet)
{
    r_parquet->doc()->AddMember(rapidjson::Value(field->name, jsonAlloc).Move(), value, jsonAlloc);
}

/**
 * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
 *
 * @param len Number of chars in value.
 * @param value pointer to value of parameter.
 * @param field RtlFieldInfo holds meta information about the embed context.
 * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
 */
void bindDataParam(unsigned len, const char *value, const RtlFieldInfo *field, std::shared_ptr<ParquetHelper> r_parquet)
{
    size32_t utf8chars;
    rtlDataAttr data;
    rtlStrToUtf8X(utf8chars, data.refstr(), len, value);
    size32_t utf8len = rtlUtf8Size(utf8chars, data.getstr());

    rapidjson::Value key;
    key.SetString(field->name, jsonAlloc);
    rapidjson::Value val;
    val.SetString(data.getstr(), utf8len, jsonAlloc);

    r_parquet->doc()->AddMember(key, val, jsonAlloc);
}

/**
 * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
 *
 * @param value pointer to value of parameter.
 * @param field RtlFieldInfo holds meta information about the embed context.
 * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
 */
void bindIntParam(__int64 value, const RtlFieldInfo *field, std::shared_ptr<ParquetHelper> r_parquet)
{
    int64_t val = value;

    rapidjson::Value key = rapidjson::Value(field->name, jsonAlloc);
    rapidjson::Value num(val);

    r_parquet->doc()->AddMember(key, num, jsonAlloc);
}

/**
 * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
 *
 * @param value pointer to value of parameter.
 * @param field RtlFieldInfo holds meta information about the embed context.
 * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
 */
void bindUIntParam(unsigned __int64 value, const RtlFieldInfo *field, std::shared_ptr<ParquetHelper> r_parquet)
{
    uint64_t val = value;

    rapidjson::Value key = rapidjson::Value(field->name, jsonAlloc);
    rapidjson::Value num(val);

    r_parquet->doc()->AddMember(key, num, jsonAlloc);
}

/**
 * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
 *
 * @param value pointer to value of parameter.
 * @param field RtlFieldInfo holds meta information about the embed context.
 * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
 */
void bindRealParam(double value, const RtlFieldInfo *field, std::shared_ptr<ParquetHelper> r_parquet)
{
    r_parquet->doc()->AddMember(rapidjson::Value(field->name, jsonAlloc).Move(), value, jsonAlloc);
}

/**
 * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
 *
 * @param chars Number of chars in value.
 * @param value pointer to value of parameter.
 * @param field RtlFieldInfo holds meta information about the embed context.
 * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
 */
void bindUnicodeParam(unsigned chars, const UChar *value, const RtlFieldInfo *field, std::shared_ptr<ParquetHelper> r_parquet)
{
    size32_t utf8chars;
    char *utf8;
    rtlUnicodeToUtf8X(utf8chars, utf8, chars, value);

    r_parquet->doc()->AddMember(rapidjson::Value(field->name, jsonAlloc).Move(), rapidjson::Value(utf8, jsonAlloc).Move(), jsonAlloc);
}

/**
 * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
 *
 * @param value Decimal value represented as a string.
 * @param field RtlFieldInfo holds meta information about the embed context.
 * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
 */
void bindDecimalParam(std::string value, const RtlFieldInfo *field, std::shared_ptr<ParquetHelper> r_parquet)
{
    r_parquet->doc()->AddMember(rapidjson::Value(field->name, jsonAlloc).Move(), value, jsonAlloc);
}

/**
 * @brief Calls the typeInfo member function process to write an ECL row to parquet.
 *
 * @param row Pointer to ECL row.
 */
void ParquetRecordBinder::processRow(const byte *row)
{
    thisParam = firstParam;
    typeInfo->process(row, row, &dummyField, *this);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param len Number of chars in value.
 * @param value Data to be written to the parquet file.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processString(unsigned len, const char *value, const RtlFieldInfo *field)
{
    checkNextParam(field);

    bindStringParam(len, value, field, r_parquet);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param value Data to be written to the parquet file.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processBool(bool value, const RtlFieldInfo *field)
{
    bindBoolParam(value, field, r_parquet);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param len Number of chars in value.
 * @param value Data to be written to the parquet file.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processData(unsigned len, const void *value, const RtlFieldInfo *field)
{
    size32_t bytes;
    rtlDataAttr data;
    rtlStrToDataX(bytes, data.refdata(), len, value);

    bindDataParam(bytes, data.getstr(), field, r_parquet);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param value Data to be written to the parquet file.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processInt(__int64 value, const RtlFieldInfo *field)
{
    bindIntParam(value, field, r_parquet);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param value Data to be written to the parquet file.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processUInt(unsigned __int64 value, const RtlFieldInfo *field)
{
    bindUIntParam(value, field, r_parquet);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param value Data to be written to the parquet file.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processReal(double value, const RtlFieldInfo *field)
{
    bindRealParam(value, field, r_parquet);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param value Data to be written to the parquet file.
 * @param digits Number of digits in decimal.
 * @param precision Number of digits of precision.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo *field)
{
    Decimal val;
    size32_t bytes;
    rtlDataAttr decText;
    val.setDecimal(digits, precision, value);
    val.getStringX(bytes, decText.refstr());

    bindDecimalParam(decText.getstr(), field, r_parquet);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param chars Number of chars in the value.
 * @param value Data to be written to the parquet file.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo *field)
{
    bindUnicodeParam(chars, value, field, r_parquet);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param len Length of QString
 * @param value Data to be written to the parquet file.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processQString(unsigned len, const char *value, const RtlFieldInfo *field)
{
    size32_t charCount;
    rtlDataAttr text;
    rtlQStrToStrX(charCount, text.refstr(), len, value);

    bindStringParam(charCount, text.getstr(), field, r_parquet);
}

/**
 * @brief Calls the bind function for the data type of the value.
 *
 * @param chars Number of chars in the value.
 * @param value Data to be written to the parquet file.
 * @param field Object with information about the current field.
 */
void ParquetRecordBinder::processUtf8(unsigned chars, const char *value, const RtlFieldInfo *field)
{
    bindStringParam(chars, value, field, r_parquet);
}

/**
 * @brief Construct a new ParquetEmbedFunctionContext object
 *
 * @param _logctx Context logger for use with the ParquetRecordBinder ParquetDatasetBinder classes.
 * @param options Pointer to the list of options that are passed into the Embed function.
 * @param _flags Should be zero if the embedded script is ok.
 */
ParquetEmbedFunctionContext::ParquetEmbedFunctionContext(const IContextLogger &_logctx, const IThorActivityContext *activityCtx, const char *options, unsigned _flags)
    : logctx(_logctx), m_NextRow(), m_nextParam(0), m_numParams(0), m_scriptFlags(_flags)
{
    // Option Variables
    const char *option = "";      // Read(read), Read Parition(readpartition), Write(write), Write Partition(writepartition)
    const char *location = "";    // file name and location of where to write parquet file
    const char *destination = ""; // file name and location of where to read parquet file from
    __int64 rowsize = 2000000;    // Size of the row groups when writing to parquet files
    __int64 batchSize = 2000000;  // Size of the batches when converting parquet columns to rows
    // Iterate through user options and save them
    StringArray inputOptions;
    inputOptions.appendList(options, ",");
    ForEachItemIn(idx, inputOptions)
    {
        const char *opt = inputOptions.item(idx);
        const char *val = strchr(opt, '=');
        if (val)
        {
            StringBuffer optName(val - opt, opt);
            val++;
            if (stricmp(optName, "option") == 0)
                option = val;
            else if (stricmp(optName, "location") == 0)
                location = val;
            else if (stricmp(optName, "destination") == 0)
                destination = val;
            else if (stricmp(optName, "MaxRowSize") == 0)
                rowsize = atoi(val);
            else if (stricmp(optName, "BatchSize") == 0)
                batchSize = atoi(val);
            else
                failx("Unknown option %s", optName.str());
        }
    }
    if (option == "" || (location == "" && destination == ""))
    {
        failx("Invalid options must specify read or write settings and a location to perform such actions.");
    }
    else
    {
        m_parquet = std::make_shared<ParquetHelper>(option, location, destination, rowsize, batchSize, activityCtx);
    }
}

bool ParquetEmbedFunctionContext::getBooleanResult()
{
    UNIMPLEMENTED_X("Parquet Scalar Return Type BOOLEAN");
    return false;
}

void ParquetEmbedFunctionContext::getDataResult(size32_t &len, void *&result)
{
    UNIMPLEMENTED_X("Parquet Scalar Return Type DATA");
}

double ParquetEmbedFunctionContext::getRealResult()
{
    UNIMPLEMENTED_X("Parquet Scalar Return Type REAL");
    return 0.0;
}

__int64 ParquetEmbedFunctionContext::getSignedResult()
{
    UNIMPLEMENTED_X("Parquet Scalar Return Type SIGNED");
    return 0;
}

unsigned __int64 ParquetEmbedFunctionContext::getUnsignedResult()
{
    UNIMPLEMENTED_X("Parquet Scalar Return Type UNSIGNED");
    return 0;
}

void ParquetEmbedFunctionContext::getStringResult(size32_t &chars, char *&result)
{
    UNIMPLEMENTED_X("Parquet Scalar Return Type STRING");
}

void ParquetEmbedFunctionContext::getUTF8Result(size32_t &chars, char *&result)
{
    UNIMPLEMENTED_X("Parquet Scalar Return Type UTF8");
}

void ParquetEmbedFunctionContext::getUnicodeResult(size32_t &chars, UChar *&result)
{
    UNIMPLEMENTED_X("Parquet Scalar Return Type UNICODE");
}

void ParquetEmbedFunctionContext::getDecimalResult(Decimal &value)
{
    UNIMPLEMENTED_X("Parquet Scalar Return Type DECIMAL");
}

IRowStream *ParquetEmbedFunctionContext::getDatasetResult(IEngineRowAllocator *_resultAllocator)
{
    Owned<ParquetRowStream> parquetRowStream;
    parquetRowStream.setown(new ParquetRowStream(_resultAllocator, m_parquet));
    return parquetRowStream.getLink();
}

byte *ParquetEmbedFunctionContext::getRowResult(IEngineRowAllocator *_resultAllocator)
{
    Owned<ParquetRowStream> parquetRowStream;
    parquetRowStream.setown(new ParquetRowStream(_resultAllocator, m_parquet));
    return (byte *)parquetRowStream->nextRow();
}

size32_t ParquetEmbedFunctionContext::getTransformResult(ARowBuilder &rowBuilder)
{
    UNIMPLEMENTED_X("Parquet Transform Result");
    return 0;
}

void ParquetEmbedFunctionContext::bindRowParam(const char *name, IOutputMetaData &metaVal, const byte *val)
{
    ParquetRecordBinder binder(logctx, metaVal.queryTypeInfo(), m_nextParam, m_parquet);
    binder.processRow(val);
    m_nextParam += binder.numFields();
}

void ParquetEmbedFunctionContext::bindDatasetParam(const char *name, IOutputMetaData &metaVal, IRowStream *val)
{
    if (m_oInputStream)
    {
        fail("At most one dataset parameter supported");
    }
    m_oInputStream.setown(new ParquetDatasetBinder(logctx, LINK(val), metaVal.queryTypeInfo(), m_parquet, m_nextParam));
    m_nextParam += m_oInputStream->numFields();
}

void ParquetEmbedFunctionContext::bindBooleanParam(const char *name, bool val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type BOOLEAN");
}

void ParquetEmbedFunctionContext::bindDataParam(const char *name, size32_t len, const void *val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type DATA");
}

void ParquetEmbedFunctionContext::bindFloatParam(const char *name, float val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type FLOAT");
}

void ParquetEmbedFunctionContext::bindRealParam(const char *name, double val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type REAL");
}

void ParquetEmbedFunctionContext::bindSignedSizeParam(const char *name, int size, __int64 val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type SIGNED SIZE");
}

void ParquetEmbedFunctionContext::bindSignedParam(const char *name, __int64 val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type SIGNED");
}

void ParquetEmbedFunctionContext::bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type UNSIGNED SIZE");
}

void ParquetEmbedFunctionContext::bindUnsignedParam(const char *name, unsigned __int64 val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type UNSIGNED");
}

void ParquetEmbedFunctionContext::bindStringParam(const char *name, size32_t len, const char *val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type STRING");
}

void ParquetEmbedFunctionContext::bindVStringParam(const char *name, const char *val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type VSTRING");
}

void ParquetEmbedFunctionContext::bindUTF8Param(const char *name, size32_t chars, const char *val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type UTF8");
}

void ParquetEmbedFunctionContext::bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
{
    UNIMPLEMENTED_X("Parquet Scalar Parameter type UNICODE");
}

/**
 * @brief Compiles the embedded script passed in by the user. The script is placed inside the EMBED
 * and ENDEMBED block.
 *
 * @param chars The number of chars in the script.
 *
 * @param script The embedded script for compilation.
 */
void ParquetEmbedFunctionContext::compileEmbeddedScript(size32_t chars, const char *script)
{
}

void ParquetEmbedFunctionContext::execute()
{
    if (m_oInputStream)
    {
        m_oInputStream->executeAll();
    }
    else
    {
        if (m_parquet->options() == 'r')
        {
            reportIfFailure(m_parquet->openReadFile());
            m_parquet->read();
        }
        else
        {
            failx("Invalid read/write option.");
        }
    }
}

void ParquetEmbedFunctionContext::callFunction()
{
    execute();
}

unsigned ParquetEmbedFunctionContext::checkNextParam(const char *name)
{
    if (m_nextParam == m_numParams)
        failx("Too many parameters supplied: No matching $<name> placeholder for parameter %s", name);
    return m_nextParam++;
}

/**
 * @brief Serves as the entry point for the HPCC Engine into the plugin and is how it obtains a
 * ParquetEmbedFunctionContext object for creating the query and executing it.
 *
 */
class ParquetEmbedContext : public CInterfaceOf<IEmbedContext>
{
public:
    virtual IEmbedFunctionContext *createFunctionContext(unsigned flags, const char *options) override
    {
        return createFunctionContextEx(nullptr, nullptr, flags, options);
    }

    virtual IEmbedFunctionContext *createFunctionContextEx(ICodeContext *ctx, const IThorActivityContext *activityCtx, unsigned flags, const char *options) override
    {
        if (flags & EFimport)
        {
            UNSUPPORTED("IMPORT");
            return nullptr;
        }
        else
            return new ParquetEmbedFunctionContext(ctx ? ctx->queryContextLogger() : queryDummyContextLogger(), activityCtx, options, flags);
    }

    virtual IEmbedServiceContext *createServiceContext(const char *service, unsigned flags, const char *options) override
    {
        throwUnexpected();
        return nullptr;
    }
};

extern DECL_EXPORT IEmbedContext *getEmbedContext()
{
    return new ParquetEmbedContext();
}

extern DECL_EXPORT bool syntaxCheck(const char *script)
{
    return true;
}
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
}
