/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* accelerated_io_tools is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* accelerated_io_tools is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* accelerated_io_tools is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with accelerated_io_tools.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>

#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/Sysinfo.h>

#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/Operator.h>
#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>
#include <query/Operator.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <util/Platform.h>
#include <util/Network.h>
#include <array/SinglePassArray.h>
#include <array/SynchableArray.h>
#include <array/PinBuffer.h>

#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>


namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.de_rle"));

using namespace scidb;
using std::shared_ptr;
using std::vector;

static void EXCEPTION_ASSERT(bool cond)
{
    if (! cond)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
    }
}

class MemChunkBuilder
{
private:
    size_t      _allocSize;
    char*       _chunkStartPointer;
    char*       _dataStartPointer;
    char*       _writePointer;
    uint32_t*   _sizePointer;
    uint64_t*   _dataSizePointer;
    MemChunk    _chunk;

public:
    static const size_t s_startingSize = 8*1024*1024 + 512;

    static size_t chunkDataOffset()
    {
        return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 5);
    }

    static size_t chunkSizeOffset()
    {
        return (sizeof(ConstRLEPayload::Header) + 2 * sizeof(ConstRLEPayload::Segment) + sizeof(varpart_offset_t) + 1);
    }


    MemChunkBuilder():
        _allocSize(s_startingSize)
    {
        _chunk.allocate(_allocSize);
        _chunkStartPointer = (char*) _chunk.getData();
        ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) _chunkStartPointer;
        hdr->_magic = RLE_PAYLOAD_MAGIC;
        hdr->_nSegs = 1;
        hdr->_elemSize = 0;
        hdr->_dataSize = 0;
        _dataSizePointer = &(hdr->_dataSize);
        hdr->_varOffs = sizeof(varpart_offset_t);
        hdr->_isBoolean = 0;
        ConstRLEPayload::Segment* seg = (ConstRLEPayload::Segment*) (hdr+1);
        *seg =  ConstRLEPayload::Segment(0,0,false,false);
        ++seg;
        *seg =  ConstRLEPayload::Segment(1,0,false,false);
        varpart_offset_t* vp =  (varpart_offset_t*) (seg+1);
        *vp = 0;
        uint8_t* sizeFlag = (uint8_t*) (vp+1);
        *sizeFlag =0;
        _sizePointer = (uint32_t*) (sizeFlag + 1);
        _dataStartPointer = (char*) (_sizePointer+1);
        _writePointer = _dataStartPointer;
    }

    ~MemChunkBuilder()
    {}

    inline size_t getTotalSize() const
    {
        return (_writePointer - _chunkStartPointer);
    }

    inline void addData(char const* data, size_t const size)
    {
        if( getTotalSize() + size > _allocSize)
        {
            size_t const mySize = getTotalSize();
            while (mySize + size > _allocSize)
            {
                _allocSize = _allocSize * 2;
            }
            vector<char> buf(_allocSize);
            memcpy(&(buf[0]), _chunk.getData(), mySize);
            _chunk.allocate(_allocSize);
            _chunkStartPointer = (char*) _chunk.getData();
            memcpy(_chunkStartPointer, &(buf[0]), mySize);
            _dataStartPointer = _chunkStartPointer + chunkDataOffset();
            _sizePointer = (uint32_t*) (_chunkStartPointer + chunkSizeOffset());
            _writePointer = _chunkStartPointer + mySize;
            ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) _chunkStartPointer;
            _dataSizePointer = &(hdr->_dataSize);
        }
        memcpy(_writePointer, data, size);
        _writePointer += size;
    }

    inline MemChunk& getChunk()
    {
        *_sizePointer = (_writePointer - _dataStartPointer);
        *_dataSizePointer = (_writePointer - _dataStartPointer) + 5 + sizeof(varpart_offset_t);
        return _chunk;
    }

    inline void reset()
    {
        _writePointer = _dataStartPointer;
    }
};

//Modeled after MaterializeArray
class DeRLEArray : public DelegateArray
{
public:
    //Caching materialized chunks: maybe later
    //std::vector< std::map<Coordinates, std::shared_ptr<MemChunk>, CoordinatesLess > > _chunkCache;
    //std::map<Coordinates, std::shared_ptr<ConstRLEEmptyBitmap>, CoordinatesLess > _bitmapCache;
    //Mutex _mutex;
    //size_t _cacheSize;

    static void materialize(const std::shared_ptr<Query>& query, MemChunk& materializedChunk, 
                            ConstChunk const& chunk, size_t const attrSize);

    //std::shared_ptr<MemChunk> getMaterializedChunk(ConstChunk const& inputChunk);

    class ArrayIterator : public DelegateArrayIterator
    {
        DeRLEArray& _array;
        ConstChunk const* _chunkToReturn;
        std::shared_ptr<MemChunk> _materializedChunk;

    public:
        ConstChunk const& getChunk() override;
        void operator ++() override;
        bool setPosition(Coordinates const& pos) override;
        void restart() override;

        ArrayIterator(DeRLEArray& arr, AttributeID attrID, std::shared_ptr<ConstArrayIterator> input);

    private: 
        bool _isEmptyTag;
        size_t _attrSize;
    };

    DeRLEArray(std::shared_ptr<Array> input, std::shared_ptr<Query>const& query);

    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
};

DeRLEArray::ArrayIterator::ArrayIterator(DeRLEArray& arr, AttributeID attrID, std::shared_ptr<ConstArrayIterator> input):
    DelegateArrayIterator(arr, attrID, input),
    _array(arr),
    _chunkToReturn(0),
    _isEmptyTag(false),
    _attrSize(0)
{
    ArrayDesc const& desc = arr.getArrayDesc();
    if(attrID == desc.getAttributes().size()-1)
    { 
        _isEmptyTag=true;
    }
    _attrSize = desc.getAttributes()[attrID].getSize();

}

ConstChunk const& DeRLEArray::ArrayIterator::getChunk()
{
    if(_chunkToReturn)
    {
        return *_chunkToReturn;
    }
    ConstChunk const& chunk = inputIterator->getChunk();
    if(_isEmptyTag)
    {
        //LOG4CXX_INFO(logger, "Forwarding empty tag");       
        return chunk;
    }
    if (!_materializedChunk) 
    {
         _materializedChunk = std::shared_ptr<MemChunk>(new MemChunk());
    }
    std::shared_ptr<Query> query(Query::getValidQueryPtr(_array._query));
    DeRLEArray::materialize(query, *_materializedChunk, chunk, _attrSize);
    _chunkToReturn = _materializedChunk.get();
    return *_chunkToReturn;
}

void DeRLEArray::ArrayIterator::operator ++()
{
    _chunkToReturn = 0;
    DelegateArrayIterator::operator ++();
}

bool DeRLEArray::ArrayIterator::setPosition(Coordinates const& pos)
{
     _chunkToReturn = 0;
     return DelegateArrayIterator::setPosition(pos);
}

void DeRLEArray::ArrayIterator::restart()
{
     _chunkToReturn = 0;;
     DelegateArrayIterator::restart();
}

DeRLEArray::DeRLEArray(std::shared_ptr<Array> input,
                       std::shared_ptr<Query>const& query): 
    DelegateArray(input->getArrayDesc(), input, true)
{
    _query = query;
}

/*
void DeRLEArray::materialize(const std::shared_ptr<Query>& query,
                                    MemChunk& materializedChunk,
                                    ConstChunk const& chunk)
{
    LOG4CXX_INFO(logger, "In Materialize");
    materializedChunk.initialize(chunk);
    materializedChunk.setBitmapChunk((Chunk*)chunk.getBitmapChunk());
    std::shared_ptr<ConstChunkIterator> src = chunk.getConstIterator();
    std::shared_ptr<ChunkIterator> dst  = materializedChunk.getIterator(query,
                                            ChunkIterator::NO_EMPTY_CHECK|ChunkIterator::SEQUENTIAL_WRITE);
    size_t count = 0;
    while (!src->end()) 
    {
        if (!dst->setPosition(src->getPosition()))
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_MERGE, SCIDB_LE_OPERATION_FAILED) << "setPosition";
        }
        dst->writeItem(src->getItem());
        count += 1;
        ++(*src);
    }
    dst->flush();
}
*/

void DeRLEArray::materialize(const std::shared_ptr<Query>& query,
                                    MemChunk& materializedChunk,
                                    ConstChunk const& chunk, 
                                    size_t const attrSize)
{
    size_t const inputCount = chunk.count();
    size_t const dataSize = attrSize * inputCount;
    size_t const chunkOverheadSize = sizeof(ConstRLEPayload::Header) +
                                     2 * sizeof(ConstRLEPayload::Segment);
    //LOG4CXX_INFO(logger, "In Materialize count " <<inputCount << " size " << attrSize );
    materializedChunk.initialize(chunk);
    materializedChunk.setBitmapChunk((Chunk*)chunk.getBitmapChunk());
    materializedChunk.allocate( chunkOverheadSize + dataSize );
    char * bufPointer = (char*) materializedChunk.getData();
    ConstRLEPayload::Header* hdr = (ConstRLEPayload::Header*) bufPointer;
    hdr->_magic = RLE_PAYLOAD_MAGIC;
    hdr->_nSegs = 1;
    hdr->_elemSize = attrSize;
    hdr->_dataSize = dataSize + 5;
    hdr->_varOffs = 0;
    hdr->_isBoolean = 0;
    ::memset(&hdr->_pad[0], 0, sizeof(hdr->_pad));
    ConstRLEPayload::Segment* seg = (ConstRLEPayload::Segment*) (hdr+1);
    *seg =  ConstRLEPayload::Segment(0,0,false,false);
    ++seg;
    *seg =  ConstRLEPayload::Segment(inputCount, inputCount,false,false);
    ++seg;
    char* dataPtr = reinterpret_cast<char*>(seg);
    std::shared_ptr<ConstChunkIterator> src = chunk.getConstIterator();
    while (!src->end()) 
    {
        Value const& v = src->getItem();
        char const* d = reinterpret_cast<char*> (v.data());
        ::memcpy(dataPtr, d, attrSize);
        dataPtr = dataPtr + attrSize;
        ++(*src);
    }
}


DelegateArrayIterator* DeRLEArray::createArrayIterator(AttributeID id) const
{
    return new DeRLEArray::ArrayIterator(*(DeRLEArray*)this, id, inputArray->getConstIterator(id));
}


class PhysicalDeRLE : public PhysicalOperator
{
public:
    PhysicalDeRLE(std::string const& logicalName,
                    std::string const& physicalName,
                    Parameters const& parameters,
                    ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        shared_ptr<Array>& input = inputArrays[0];
        return shared_ptr<Array>(new DeRLEArray(input, query));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalDeRLE, "de_rle", "PhysicalDeRLE");

} // end namespace scidb