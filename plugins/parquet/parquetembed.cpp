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

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

// #include <map>
// #include <mutex>
// #include <thread>
// #include <cstdlib>
// #include <iostream>
// #include <string>
// #include <memory>
// #include <cstdint>

// #include "platform.h"
// #include "jthread.hpp"
#include "rtlembed.hpp"
// #include "jptree.hpp"
#include "rtlds_imp.hpp"
// #include <time.h>
// #include <vector>

static constexpr const char *MODULE_NAME = "parquet";
static constexpr const char *MODULE_DESCRIPTION = "Parquet Embed Helper";
static constexpr const char *VERSION = "Parquet Embed Helper 1.0.0";
static const char *COMPATIBLE_VERSIONS[] = { VERSION, nullptr };
static const NullFieldProcessor NULLFIELD(NULL);

/**
 * @brief Takes a pointer to an ECLPluginDefinitionBlock and passes in all the important info
 * about the plugin. 
 */
extern "C" PARQUETEMBED_PLUGIN_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    if (pb->size == sizeof(ECLPluginDefinitionBlockEx)) 
    {
        ECLPluginDefinitionBlockEx * pbx = (ECLPluginDefinitionBlockEx *) pb;
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
    //--------------------------------------------------------------------------
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
        va_start(args,message);
        StringBuffer msg;
        msg.appendf("%s: ", MODULE_NAME).valist_appendf(message,args);
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

    ParquetRowStream::ParquetRowStream(IEngineRowAllocator* _resultAllocator, std::shared_ptr<ParquetHelper> _parquet)
        : m_resultAllocator(_resultAllocator)
    {
        s_parquet = _parquet;
        m_currentRow = 0;
        m_shouldRead = true;
        numRows = _parquet->num_rows();
    }
    
    ParquetRowStream::~ParquetRowStream()
    {
    }

    const void* ParquetRowStream::nextRow()
    {
        arrow::Result<rapidjson::Document> row = s_parquet->next();
        if (m_shouldRead && m_currentRow < numRows)
        {
            rapidjson::Document doc = std::move(row).ValueUnsafe();
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            doc.Accept(writer);

            auto json = buffer.GetString();
            Owned<IPropertyTree> contentTree = createPTreeFromJSONString(json, ipt_caseInsensitive);
            m_currentRow++;

            if (contentTree)
            {
                ParquetRowBuilder pRowBuilder(contentTree);
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

    /**
     * @brief Gets a Boolean result for an ECL Row
     * 
     * @param field Holds the value of the field.
     * @return bool Returns the boolean value from the result row. 
     */
    bool ParquetRowBuilder::getBooleanResult(const RtlFieldInfo *field)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            return p.boolResult;
        }

        bool mybool;
        parquetembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, mybool), "bool", value);
        return mybool;
    }

    /**
     * @brief Gets a data result from the result row and passes it back to engine through result.
     * 
     * @param field Holds the value of the field.
     * @param len Length of the Data value.
     * @param result Used for returning the result to the caller.
     */
    void ParquetRowBuilder::getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            rtlStrToDataX(len, result, p.resultChars, p.stringResult);
            return;
        }
        rtlStrToDataX(len, result, strlen(value), value); // This feels like it may not work to me - will preallocate rather larger than we want
    }

    /**
     * @brief Gets a real result from the result row.
     * 
     * @param field Holds the value of the field.
     * @return double Double value to return.
     */
    double ParquetRowBuilder::getRealResult(const RtlFieldInfo *field)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            return p.doubleResult;
        }

        double mydouble = 0.0;
        parquetembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, mydouble), "real", value);
        return mydouble;
    }

    /**
     * @brief Gets the Signed Integer result from the result row.
     * 
     * @param field Holds the value of the field.
     * @return __int64 Value to return.
     */
    __int64 ParquetRowBuilder::getSignedResult(const RtlFieldInfo *field)
    {
        const char * value = nextField(field);
        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            return p.uintResult;
        }

        __int64 myint64 = 0;
        parquetembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, myint64), "signed", value);
        return myint64;
    }

    /**
     * @brief Gets the Unsigned Integer result from the result row.
     * 
     * @param field Holds the value of the field.
     * @return unsigned Value to return.
     */
    unsigned __int64 ParquetRowBuilder::getUnsignedResult(const RtlFieldInfo *field)
    {
        const char * value = nextField(field);
        if (!value || !*value) 
        {

            NullFieldProcessor p(field);
            return p.uintResult;
        }

        unsigned __int64 myuint64 = 0;
        parquetembed::handleDeserializeOutcome(m_tokenDeserializer.deserialize(value, myuint64), "unsigned", value);
        return myuint64;
    }

    /**
     * @brief Gets a String from the result row.
     * 
     * @param field Holds the value of the field.
     * @param chars Number of chars in the String.
     * @param result Variable used for returning string back to the caller.
     */
    void ParquetRowBuilder::getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            rtlUtf8ToStrX(chars, result, p.resultChars, p.stringResult);
            return;
        }

        unsigned numchars = rtlUtf8Length(strlen(value), value);
        rtlUtf8ToStrX(chars, result, numchars, value);
        return;
    }

    /**
     * @brief Gets a UTF8 from the result row.
     * 
     * @param field Holds the value of the field.
     * @param chars Number of chars in the UTF8.
     * @param result Variable used for returning UTF8 back to the caller.
     */
    void ParquetRowBuilder::getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            rtlUtf8ToUtf8X(chars, result, p.resultChars, p.stringResult);
            return;
        }

        unsigned numchars = rtlUtf8Length(strlen(value), value);
        rtlUtf8ToUtf8X(chars, result, numchars, value);
        return;
    }

    /**
     * @brief Gets a Unicode from the result row.
     * 
     * @param field Holds the value of the field.
     * @param chars Number of chars in the Unicode.
     * @param result Variable used for returning Unicode back to the caller.
     */
    void ParquetRowBuilder::getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        const char * value = nextField(field);

        if (!value || !*value) 
        {
            NullFieldProcessor p(field);
            rtlUnicodeToUnicodeX(chars, result, p.resultChars, p.unicodeResult);
            return;
        }

        unsigned numchars = rtlUtf8Length(strlen(value), value); // MORE - is it a good assumption that it is utf8 ? Depends how the database is configured I think
        rtlUtf8ToUnicodeX(chars, result, numchars, value);
        return;
    }

    /**
     * @brief Gets a decimal from the result row.
     * 
     * @param field Holds the value of the field.
     * @param value Variable used for returning decimal to caller.
     */
    void ParquetRowBuilder::getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        const char * dvalue = nextField(field);
        if (!dvalue || !*dvalue) 
        {
            NullFieldProcessor p(field);
            value.set(p.decimalResult);
            return;
        }

        size32_t chars;
        rtlDataAttr result;
        value.setString(strlen(dvalue), dvalue);
        RtlDecimalTypeInfo *dtype = (RtlDecimalTypeInfo *) field->type;
        value.setPrecision(dtype->getDecimalDigits(), dtype->getDecimalPrecision());
    }

    /**
     * @brief Starts a new Set.
     * 
     * @param field Field with information about the context of the set.
     * @param isAll Not Supported.
     */
    void ParquetRowBuilder::processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        isAll = false; // ALL not supported

        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty()) 
        {
            PathTracker newPathNode(xpath, CPNTSet);
            StringBuffer newXPath;

            constructNewXPath(newXPath, xpath.str());

            newPathNode.childCount = m_oResultRow->getCount(newXPath);
            m_pathStack.push_back(newPathNode);
        } 
        else 
        {
            failx("processBeginSet: Field name or xpath missing");
        }
    }

    /**
     * @brief Checks if we should process another set.
     * 
     * @param field Context information about the set.
     * @return true If the children that we have process is less than the total child count.
     * @return false If all the children sets have been processed.
     */
    bool ParquetRowBuilder::processNextSet(const RtlFieldInfo * field)
    {
        return m_pathStack.back().childrenProcessed < m_pathStack.back().childCount;
    }

    /**
     * @brief Starts a new Dataset.
     * 
     * @param field Information about the context of the dataset.
     */
    void ParquetRowBuilder::processBeginDataset(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty()) 
        {
            PathTracker newPathNode(xpath, CPNTDataset);
            StringBuffer newXPath;

            constructNewXPath(newXPath, xpath.str());

            newPathNode.childCount = m_oResultRow->getCount(newXPath);
            m_pathStack.push_back(newPathNode);
        } 
        else 
        {
            failx("processBeginDataset: Field name or xpath missing");
        }
    }

    /**
     * @brief Starts a new Row.
     * 
     * @param field Information about the context of the row.
     */
    void ParquetRowBuilder::processBeginRow(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty()) 
        {
            if (strncmp(xpath.str(), "<nested row>", 12) == 0) 
            {
                // Row within child dataset
                if (m_pathStack.back().nodeType == CPNTDataset) 
                {
                    m_pathStack.back().currentChildIndex++;
                } 
                else 
                {
                    failx("<nested row> received with no outer dataset designated");
                }
            } 
            else 
            {
                m_pathStack.push_back(PathTracker(xpath, CPNTScalar));
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
    bool ParquetRowBuilder::processNextRow(const RtlFieldInfo * field)
    {
        return m_pathStack.back().childrenProcessed < m_pathStack.back().childCount;
    }

    /**
     * @brief Ends a set.
     * 
     * @param field Information about the context of the set.
     */
    void ParquetRowBuilder::processEndSet(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty() && !m_pathStack.empty() && strcmp(xpath.str(), m_pathStack.back().nodeName.str()) == 0) 
        {
            m_pathStack.pop_back();
        }
    }

    /**
     * @brief Ends a dataset.
     * 
     * @param field Information about the context of the dataset.
     */
    void ParquetRowBuilder::processEndDataset(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (!xpath.isEmpty()) 
        {
            if (!m_pathStack.empty() && strcmp(xpath.str(), m_pathStack.back().nodeName.str()) == 0) 
            {
                m_pathStack.pop_back();
            }
        } 
        else 
        {
            failx("processEndDataset: Field name or xpath missing");
        }
    }

    /**
     * @brief Ends a row.
     * 
     * @param field Information about the context of the row.
     */
    void ParquetRowBuilder::processEndRow(const RtlFieldInfo * field)
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
                else if (strcmp(xpath.str(), m_pathStack.back().nodeName.str()) == 0) 
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

    /**
     * @brief Gets the next field and processes it.
     * 
     * @param field Information about the context of the next field.
     * @return const char* Result of building field.
     */
    const char * ParquetRowBuilder::nextField(const RtlFieldInfo * field)
    {
        StringBuffer xpath;
        xpathOrName(xpath, field);

        if (xpath.isEmpty()) 
        {
            failx("nextField: Field name or xpath missing");
        }
        StringBuffer fullXPath;

        if (!m_pathStack.empty() && m_pathStack.back().nodeType == CPNTSet && strncmp(xpath.str(), "<set element>", 13) == 0) 
        {
            m_pathStack.back().currentChildIndex++;
            constructNewXPath(fullXPath, NULL);
            m_pathStack.back().childrenProcessed++;
        } 
        else 
        {
            constructNewXPath(fullXPath, xpath.str());
        }

        return m_oResultRow->queryProp(fullXPath.str());
    }

    void ParquetRowBuilder::xpathOrName(StringBuffer & outXPath, const RtlFieldInfo * field) const
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
                const char * sep = strchr(field->xpath, xpathCompoundSeparatorChar);

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

    void ParquetRowBuilder::constructNewXPath(StringBuffer& outXPath, const char * nextNode) const
    {
        bool nextNodeIsFromRoot = (nextNode && *nextNode == '/');

        outXPath.clear();

        if (!nextNodeIsFromRoot) 
        {
            // Build up full parent xpath using our previous components
            for (std::vector<PathTracker>::const_iterator iter = m_pathStack.begin(); iter != m_pathStack.end(); iter++) 
            {
                if (strncmp(iter->nodeName, "<row>", 5) != 0) 
                {
                    if (!outXPath.isEmpty()) 
                    {
                        outXPath.append("/");
                    }
                    outXPath.append(iter->nodeName);
                    if (iter->nodeType == CPNTDataset || iter->nodeType == CPNTSet) 
                    {
                        outXPath.appendf("[%d]", iter->currentChildIndex);
                    }
                }
            }
        }

        if (nextNode && *nextNode) 
        {
            if (!outXPath.isEmpty()) 
            {
                outXPath.append("/");
            }
            outXPath.append(nextNode);
        }
    }

    unsigned ParquetRecordBinder::checkNextParam(const RtlFieldInfo * field)
    {
        if (logctx.queryTraceLevel() > 4) 
            logctx.CTXLOG("Binding %s to %d", field->name, thisParam);
        return thisParam++;       
    }    

    int ParquetRecordBinder::numFields()
    {
        int count = 0;
        const RtlFieldInfo * const *fields = typeInfo->queryFields();
        assertex(fields);
        while (*fields++) 
            count++;
        return count;
    }

    /**
     * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
     * 
     * @param len Number of chars in value.
     * @param value pointer to value of parameter.
     * @param field RtlFieldInfo holds meta information about the embed context.
     * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
     */
    void bindUtf8Param(unsigned len, const char *value, const RtlFieldInfo * field, std::shared_ptr<ParquetHelper> r_parquet)
    {
        size32_t utf8chars;
        rtlDataAttr utf8;
        rtlUtf8ToUtf8X(utf8chars, utf8.refstr(), len, value);
        *r_parquet->write() << std::string(utf8.getstr(), rtlUtf8Size(utf8chars, utf8.getdata()));
    }

    /**
     * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
     * 
     * @param len Number of chars in value.
     * @param value pointer to value of parameter.
     * @param field RtlFieldInfo holds meta information about the embed context.
     * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
     */
    void bindStringParam(unsigned len, const char *value, const RtlFieldInfo * field, std::shared_ptr<ParquetHelper> r_parquet)
    {
        *r_parquet->write() << std::string(value, len);
    }

    /**
     * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
     * 
     * @param value pointer to value of parameter.
     * @param field RtlFieldInfo holds meta information about the embed context. 
     * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
     */
    void bindBoolParam(bool value, const RtlFieldInfo * field, std::shared_ptr<ParquetHelper> r_parquet)
    {
        *r_parquet->write() << value;
    }

    /**
     * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
     * 
     * @param len Number of chars in value.
     * @param value pointer to value of parameter.
     * @param field RtlFieldInfo holds meta information about the embed context. 
     * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
     */
    void bindDataParam(unsigned len, const void *value, const RtlFieldInfo * field, std::shared_ptr<ParquetHelper> r_parquet)
    {
        size32_t bytes;
        rtlDataAttr data;
        rtlStrToDataX(bytes, data.refdata(), len, value);

        *r_parquet->write() << std::string(data.getstr(), bytes);
    }

    /**
     * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
     * 
     * @param value pointer to value of parameter.
     * @param field RtlFieldInfo holds meta information about the embed context. 
     * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
     */
    void bindIntParam(__int64 value, const RtlFieldInfo * field, std::shared_ptr<ParquetHelper> r_parquet)
    {
        int64_t val = value;
        *r_parquet->write() << val;
    }

    /**
     * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
     * 
     * @param value pointer to value of parameter.
     * @param field RtlFieldInfo holds meta information about the embed context.
     * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
     */
    void bindUIntParam(unsigned __int64 value, const RtlFieldInfo * field, std::shared_ptr<ParquetHelper> r_parquet)
    {
        uint64_t val = value;
        *r_parquet->write() << val;
    }

    /**
     * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
     * 
     * @param value pointer to value of parameter.
     * @param field RtlFieldInfo holds meta information about the embed context.
     * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
     */
    void bindRealParam(double value, const RtlFieldInfo * field, std::shared_ptr<ParquetHelper> r_parquet)
    {
        *r_parquet->write() << value;
    }

    /**
     * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
     * 
     * @param chars Number of chars in value.
     * @param value pointer to value of parameter.
     * @param field RtlFieldInfo holds meta information about the embed context.
     * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
     */
    void bindUnicodeParam(unsigned chars, const UChar *value, const RtlFieldInfo * field, std::shared_ptr<ParquetHelper> r_parquet)
    {
        size32_t utf8chars;
        char *utf8;
        rtlUnicodeToUtf8X(utf8chars, utf8, chars, value);
        *r_parquet->write() << utf8;
    }

    /**
     * @brief Writes the value to the parquet file using the StreamWriter from the ParquetHelper class.
     * 
     * @param value Decimal value represented as a string.
     * @param field RtlFieldInfo holds meta information about the embed context.
     * @param r_parquet Shared pointer to helper class that operates the parquet functions for us.
     */
    void bindDecimalParam(std::string value, const RtlFieldInfo * field, std::shared_ptr<ParquetHelper> r_parquet)
    {
        *r_parquet->write() << value;
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
    void ParquetRecordBinder::processString(unsigned len, const char *value, const RtlFieldInfo * field)
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
    void ParquetRecordBinder::processBool(bool value, const RtlFieldInfo * field)
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
    void ParquetRecordBinder::processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        bindDataParam(len, value, field, r_parquet);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param value Data to be written to the parquet file.
     * @param field Object with information about the current field.
     */
    void ParquetRecordBinder::processInt(__int64 value, const RtlFieldInfo * field)
    {
        bindIntParam(value, field, r_parquet);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param value Data to be written to the parquet file.
     * @param field Object with information about the current field.
     */
    void ParquetRecordBinder::processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        bindUIntParam(value, field, r_parquet);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param value Data to be written to the parquet file.
     * @param field Object with information about the current field.
     */
    void ParquetRecordBinder::processReal(double value, const RtlFieldInfo * field)
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
    void ParquetRecordBinder::processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
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
    void ParquetRecordBinder::processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo * field)
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
    void ParquetRecordBinder::processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        size32_t charCount;
        rtlDataAttr text;
        rtlQStrToStrX(charCount, text.refstr(), len, value);
        processUtf8(charCount, text.getstr(), field);
    }

    /**
     * @brief Calls the bind function for the data type of the value.
     * 
     * @param chars Number of chars in the value.
     * @param value Data to be written to the parquet file.
     * @param field Object with information about the current field.
     */
    void ParquetRecordBinder::processUtf8(unsigned chars, const char *value, const RtlFieldInfo * field)
    {
        bindUtf8Param(chars, value, field, r_parquet);
    }

    /**
     * @brief Gets all the field types from the RtlTypeInfo object and adds them to the schema.
     * 
     * @param typeInfo Object holding meta information about the record.
     */
    void ParquetDatasetBinder::getFieldTypes(const RtlTypeInfo *typeInfo)
    {
        const RtlFieldInfo * const *fields = typeInfo->queryFields();
        assertex(fields);
        while (*fields){
            const char * name = (*fields)->name;
            enum parquet::Type::type type;
            enum parquet::ConvertedType::type ctype;
            int wlength = -1;  // Writing length that gets passed to schema
            unsigned len = (*fields)->type->length;
            switch((*fields)->type->getType())
            {
                case type_boolean:
                    type = parquet::Type::BOOLEAN;
                    ctype = parquet::ConvertedType::NONE;
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
                case type_decimal:
                    type = parquet::Type::BYTE_ARRAY;
                    ctype = parquet::ConvertedType::DECIMAL;
                    break;
                case type_string:
                    type = parquet::Type::BYTE_ARRAY;
                    ctype = parquet::ConvertedType::UTF8;
                    break;
                case type_char:
                    type = parquet::Type::FIXED_LEN_BYTE_ARRAY;
                    ctype = parquet::ConvertedType::NONE;
                    wlength = (*fields)->type->length;
                    break;
                case type_varstring:
                    type = parquet::Type::BYTE_ARRAY;
                    ctype = parquet::ConvertedType::UTF8;
                    break;
                case type_set:
                    // Do something with arrow::ListType
                    break;
                case type_record:
                case type_row:
                    // Do something with arrow::MapType
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
                default:
                    failx("Datatype %i is not compatible with this plugin.", (*fields)->type->getType());
            }
            d_parquet->addField(name, parquet::Repetition::REQUIRED, type, ctype, wlength);
            fields++;
        }
    }

    /**
     * @brief Construct a new ParquetEmbedFunctionContext object
     * 
     * @param _logctx Context logger for use with the ParquetRecordBinder ParquetDatasetBinder classes.
     * @param options Pointer to the list of options that are passed into the Embed function.
     * @param _flags Should be zero if the embedded script is ok.
     */
    ParquetEmbedFunctionContext::ParquetEmbedFunctionContext(const IContextLogger &_logctx, const char *options, unsigned _flags)
    : logctx(_logctx), m_NextRow(), m_nextParam(0), m_numParams(0), m_scriptFlags(_flags)
    {
        // Option Variables
        const char *option = ""; // Read(r), Write(w)
        const char *location = ""; // file name and location of where to write parquet file
        const char *destination = ""; // file name and location of where to read parquet file from
        int rowsize = 1000; // Size of the row groups when writing to parquet files
        int batchSize = 100; // Size of the batches when converting parquet columns to rows
        // Iterate through user options and save them
        StringArray inputOptions;
        inputOptions.appendList(options, ",");
        ForEachItemIn(idx, inputOptions) 
        {
            const char *opt = inputOptions.item(idx);
            const char *val = strchr(opt, '=');
            if (val)
            {
                StringBuffer optName(val-opt, opt);
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
        std::shared_ptr<ParquetHelper> ptr(new ParquetHelper(option, location, destination, rowsize, batchSize));
        m_parquet = ptr;
    }

    /**
     * @brief Destroy the ParquetEmbedFunctionContext object.
     */
    ParquetEmbedFunctionContext::~ParquetEmbedFunctionContext()
    {
    }

    bool ParquetEmbedFunctionContext::getBooleanResult()
    {
        // TO DO
        return true;
    }

    void ParquetEmbedFunctionContext::getDataResult(size32_t &len, void * &result)
    {
        // TO DO
    }

    double ParquetEmbedFunctionContext::getRealResult()
    {
        // TO DO
        return 1.2;
    }

    __int64 ParquetEmbedFunctionContext::getSignedResult()
    {
        // TO DO
        return 2;
    }

    unsigned __int64 ParquetEmbedFunctionContext::getUnsignedResult()
    {
        // TO DO
        return 3;
    }

    void ParquetEmbedFunctionContext::getStringResult(size32_t &chars, char * &result)
    {
        // TO DO
    }

    void ParquetEmbedFunctionContext::getUTF8Result(size32_t &chars, char * &result)
    {
        // TO DO
    }

    void ParquetEmbedFunctionContext::getUnicodeResult(size32_t &chars, UChar * &result)
    {
        // TO DO
    }

    void ParquetEmbedFunctionContext::getDecimalResult(Decimal &value)
    {
        // TO DO
    }

    IRowStream * ParquetEmbedFunctionContext::getDatasetResult(IEngineRowAllocator * _resultAllocator)
    {
        Owned<ParquetRowStream> parquetRowStream;
        parquetRowStream.setown(new ParquetRowStream(_resultAllocator, m_parquet));
        return parquetRowStream.getLink();
    }

    byte * ParquetEmbedFunctionContext::getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        Owned<ParquetRowStream> parquetRowStream;
        parquetRowStream.setown(new ParquetRowStream(_resultAllocator, m_parquet));
        return (byte *)parquetRowStream->nextRow();  
    }

    size32_t ParquetEmbedFunctionContext::getTransformResult(ARowBuilder & rowBuilder)
    {
        UNIMPLEMENTED_X("Parquet Transform Result");
        return 0;
    }

    void ParquetEmbedFunctionContext::bindRowParam(const char *name, IOutputMetaData & metaVal, const byte *val)
    {
        ParquetRecordBinder binder(logctx, metaVal.queryTypeInfo(), m_nextParam, m_parquet);
        binder.processRow(val);
        m_nextParam += binder.numFields();    
    }

    void ParquetEmbedFunctionContext::bindDatasetParam(const char *name, IOutputMetaData & metaVal, IRowStream * val)
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
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindDataParam(const char *name, size32_t len, const void *val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindFloatParam(const char *name, float val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindRealParam(const char *name, double val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindSignedSizeParam(const char *name, int size, __int64 val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindSignedParam(const char *name, __int64 val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindUnsignedSizeParam(const char *name, int size, unsigned __int64 val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindUnsignedParam(const char *name, unsigned __int64 val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindStringParam(const char *name, size32_t len, const char *val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindVStringParam(const char *name, const char *val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindUTF8Param(const char *name, size32_t chars, const char *val)
    {
        // TO DO    
    }

    void ParquetEmbedFunctionContext::bindUnicodeParam(const char *name, size32_t chars, const UChar *val)
    {
        // TO DO    
    }

    /**
     * @brief Compiles the embedded script passed in by the user. The script is placed inside the EMBED
     * and ENDEMBED block.
     * 
     * @param chars THe number of chars in the script.
     * 
     * @param script The embedded script for compilation.
     */
    void ParquetEmbedFunctionContext::compileEmbeddedScript(size32_t chars, const char *script)
    {
        // Not sure if there will be an embedded script.
        // if (script && *script) 
        // {
        //     // Incoming script is not necessarily null terminated. Note that the chars refers to utf8 characters and not bytes.
        //     size32_t size = rtlUtf8Size(chars, script);

        //     if (size > 0) 
        //     {
        //         StringAttr queryScript;
        //         queryScript.set(script, size);
        //         // Do something with the script now that is is done processing
        //         // queryScript.get()
        //     }
        //     else
        //         failx("Empty query detected");
        // }
        // else
        //     failx("Empty query detected");
    }
    
    void ParquetEmbedFunctionContext::execute()
    {
        if (m_oInputStream)
            m_oInputStream->executeAll();
        else
        {
            if(m_parquet->options() == 'w')
            {

            }
            else if(m_parquet->options() == 'r')
            {
                m_parquet->openReadFile();

                m_parquet->read();

                m_parquet->setIterator();
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
        virtual IEmbedFunctionContext * createFunctionContext(unsigned flags, const char *options) override
        {
            return createFunctionContextEx(nullptr, nullptr, flags, options);
        }

        virtual IEmbedFunctionContext * createFunctionContextEx(ICodeContext * ctx, const IThorActivityContext *activityCtx, unsigned flags, const char *options) override
        {
            if (flags & EFimport) 
            {
                UNSUPPORTED("IMPORT");
                return nullptr;
            } 
            else 
                return new ParquetEmbedFunctionContext(ctx ? ctx->queryContextLogger() : queryDummyContextLogger(), options, flags);
        }

        virtual IEmbedServiceContext * createServiceContext(const char *service, unsigned flags, const char *options) override
        {
            throwUnexpected();
            return nullptr;
        }
    };


    extern DECL_EXPORT IEmbedContext* getEmbedContext()
    {
        return new ParquetEmbedContext();
    }

    extern DECL_EXPORT bool syntaxCheck(const char *script)
    {
        return true; // TO-DO
    }
}

MODULE_INIT(INIT_PRIORITY_STANDARD)
{
    return true;
}

MODULE_EXIT()
{
}
