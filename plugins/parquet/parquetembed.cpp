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
// #include "rtlds_imp.hpp"
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

    ParquetRowStream::ParquetRowStream(IEngineRowAllocator* _resultAllocator)
    {
        // TODO
    }
    ParquetRowStream::~ParquetRowStream()
    {
        // TODO
    }
    const void* ParquetRowStream::nextRow()
    {
        // TODO
        return nullptr;
    }
    void ParquetRowStream::stop()
    {
        // TODO
    }

    bool ParquetRowBuilder::getBooleanResult(const RtlFieldInfo *field)
    {
        // TODO
        return true;
    }

    void ParquetRowBuilder::getDataResult(const RtlFieldInfo *field, size32_t &len, void * &result)
    {
        // TODO
    }

    double ParquetRowBuilder::getRealResult(const RtlFieldInfo *field)
    {
        // TODO
        return 1.1;
    }

    __int64 ParquetRowBuilder::getSignedResult(const RtlFieldInfo *field)
    {
        // TODO
        return 1;
    }

    unsigned __int64 ParquetRowBuilder::getUnsignedResult(const RtlFieldInfo *field)
    {
        // TODO
        return 1;
    }

    void ParquetRowBuilder::getStringResult(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        // TODO
    }

    void ParquetRowBuilder::getUTF8Result(const RtlFieldInfo *field, size32_t &chars, char * &result)
    {
        // TODO
    }

    void ParquetRowBuilder::getUnicodeResult(const RtlFieldInfo *field, size32_t &chars, UChar * &result)
    {
        // TODO
    }

    void ParquetRowBuilder::getDecimalResult(const RtlFieldInfo *field, Decimal &value)
    {
        // TODO
    }

    void ParquetRowBuilder::processBeginSet(const RtlFieldInfo * field, bool &isAll)
    {
        // TODO
    }

    bool ParquetRowBuilder::processNextSet(const RtlFieldInfo * field)
    {
        // TODO
        return true;
    }

    void ParquetRowBuilder::processBeginDataset(const RtlFieldInfo * field)
    {
        // TODO
    }

    void ParquetRowBuilder::processBeginRow(const RtlFieldInfo * field)
    {
        // TODO
    }

    bool ParquetRowBuilder::processNextRow(const RtlFieldInfo * field)
    {
        // TODO
        return true;
    }

    void ParquetRowBuilder::processEndSet(const RtlFieldInfo * field)
    {
        // TODO
    }

    void ParquetRowBuilder::processEndDataset(const RtlFieldInfo * field)
    {
        // TODO
    }

    void ParquetRowBuilder::processEndRow(const RtlFieldInfo * field)
    {
        // TODO
    }    


    const char * ParquetRowBuilder::nextField(const RtlFieldInfo * field)
    {
        // TO DO
        return nullptr;
    }

    void ParquetRowBuilder::xpathOrName(StringBuffer & outXPath, const RtlFieldInfo * field) const
    {
        // TO DO
    }

    void ParquetRowBuilder::constructNewXPath(StringBuffer& outXPath, const char * nextNode) const
    {
        // TO DO
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

    void ParquetRecordBinder::processRow(const byte *row)
    {
        // TO DO
    }

    void ParquetRecordBinder::processString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        // TO DO
    }

    void ParquetRecordBinder::processBool(bool value, const RtlFieldInfo * field)
    {
        // TO DO
    }

    void ParquetRecordBinder::processData(unsigned len, const void *value, const RtlFieldInfo * field)
    {
        // TO DO
    }

    void ParquetRecordBinder::processInt(__int64 value, const RtlFieldInfo * field)
    {
        // TO DO
    }

    void ParquetRecordBinder::processUInt(unsigned __int64 value, const RtlFieldInfo * field)
    {
        // TO DO
    }

    void ParquetRecordBinder::processReal(double value, const RtlFieldInfo * field)
    {
        // TO DO
    }

    void ParquetRecordBinder::processDecimal(const void *value, unsigned digits, unsigned precision, const RtlFieldInfo * field)
    {
        // TO DO
    }


    void ParquetRecordBinder::processUnicode(unsigned chars, const UChar *value, const RtlFieldInfo * field)
    {
        // TO DO
    }

    void ParquetRecordBinder::processQString(unsigned len, const char *value, const RtlFieldInfo * field)
    {
        // TO DO
    }

    void ParquetRecordBinder::processUtf8(unsigned chars, const char *value, const RtlFieldInfo * field)
    {
        // TO DO
    }

    void ParquetDatasetBinder::getFieldTypes(const RtlTypeInfo *typeInfo)
    {
        const RtlFieldInfo * const *fields = typeInfo->queryFields();
        assertex(fields);
        while (*fields){
            const char * name = fields->name;
            enum parquet::Type::type type;
            enum parquet::ConvertedType::type ctype;
            int length = -1;
            switch(fields->type->getType())
            {
                case type_boolean:
                    type = BOOLEAN;
                    ctype = NONE;
                    break;
                case type_int:
                    if(fields->type->length > 4)
                    {
                        type = INT64;
                        ctype = INT_64;
                    }
                    else if(fields->type->length > 2)
                    {
                        type = INT32;
                        ctype = INT_32;
                    }
                    else if(fields->type->length > 1)
                    {
                        type = INT32;
                        ctype = INT_16;
                    }
                    else
                    {
                        type = INT32;
                        ctype = INT_8;
                    } 
                    break;
                case type_unsigned:
                    if(fields->type->length > 4)
                    {
                        type = INT64;
                        ctype = UINT_64;
                    }
                    else if(fields->type->length > 2)
                    {
                        type = INT32;
                        ctype = UINT_32;
                    }
                    else if(fields->type->length > 1)
                    {
                        type = INT32;
                        ctype = UINT_16;
                    }
                    else
                    {
                        type = INT32;
                        ctype = UINT_8;
                    } 
                    break;
                case type_real:
                    type = FLOAT;
                    ctype = NONE;
                    break;
                case type_decimal:
                    type = FLOAT;
                    ctype = DECIMAL;
                    break;
                case type_string:
                    type = BYTE_ARRAY;
                    ctype = UTF8;
                    break;
                case type_char:
                    type = FIXED_LEN_BYTE_ARRAY;
                    ctype = NONE;
                    length = fields->type->length;
                    break;
                case type_record:
                    
                    break;
                case type_varstring:
                    type = BYTE_ARRAY;
                    ctype = UTF8;
                    break;
                case type_set:
                    
                    break;
                case type_row:
                    
                    break;
                case type_qstring:
                    type = BYTE_ARRAY;
                    ctype = UTF8;
                    break;
                case type_unicode:
                    UNSUPPORTED("UNICODE datatype");
                    break;
                case type_utf8:
                    type = BYTE_ARRAY;
                    ctype = UTF8;
                    break;
                default:
                    failx("Datatype %i is not compatible with this plugin.", fields->type->getType());
            }
            d_parquet->addField(name, REQUIRED, type, ctype, length);
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
        int rowsize = 1000;
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
                else
                    failx("Unknown option %s", optName.str());
            }
        }
        std::shared_ptr<ParquetHelper> ptr(new ParquetHelper(option, location, destination, rowsize));
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
        // TO DO    
        return nullptr;
    }

    byte * ParquetEmbedFunctionContext::getRowResult(IEngineRowAllocator * _resultAllocator)
    {
        // TO DO
        return nullptr;    
    }

    size32_t ParquetEmbedFunctionContext::getTransformResult(ARowBuilder & rowBuilder)
    {
        // TO DO  
        return 0;  
    }

    void ParquetEmbedFunctionContext::bindRowParam(const char *name, IOutputMetaData & metaVal, const byte *val)
    {
        // TO DO    
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
            // TODO
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
