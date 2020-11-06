/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* de_rle is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* de_rle is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* de_rle is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with de_rle.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
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
#include <query/PhysicalOperator.h>
#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <util/Platform.h>
#include <network/Network.h>
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

        ArrayIterator(DeRLEArray& arr, const AttributeDesc& attrID, const std::shared_ptr<ConstArrayIterator> input);

    private:
        bool _isEmptyTag;
        size_t _attrSize;
    };

    DeRLEArray(std::shared_ptr<Array> input, std::shared_ptr<Query>const& query);

    DelegateArrayIterator* createArrayIterator(const AttributeDesc& id) const override;
};

DeRLEArray::ArrayIterator::ArrayIterator(DeRLEArray& arr, const AttributeDesc& attrID, const std::shared_ptr<ConstArrayIterator> input):
    DelegateArrayIterator(arr, attrID, input),
    _array(arr),
    _chunkToReturn(0),
    _isEmptyTag(false),
    _attrSize(0)
{
    ArrayDesc const& desc = arr.getArrayDesc();
    if(attrID.getId() == desc.getEmptyBitmapAttribute()->getId())
    {
        _isEmptyTag=true;
    }
    _attrSize = attrID.getSize();

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
         _materializedChunk = std::shared_ptr<MemChunk>(new MemChunk(SCIDB_CODE_LOC));
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

/* OLD Method from materialize() operator
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
    size_t const chunkOverheadSize = sizeof(ConstRLEPayload::PayloadHeader) +
                                     2 * sizeof(PayloadSegment);
    //LOG4CXX_INFO(logger, "In Materialize count " <<inputCount << " size " << attrSize );
    materializedChunk.initialize(chunk);
    materializedChunk.setBitmapChunk((Chunk*)chunk.getBitmapChunk());
    materializedChunk.allocate( chunkOverheadSize + dataSize, AllocType::memChunk_data, SCIDB_CODE_LOC );
    char * bufPointer = (char*) materializedChunk.getWriteData();
    ConstRLEPayload::PayloadHeader* hdr = (ConstRLEPayload::PayloadHeader*) bufPointer;
    hdr->_magic = RLE_PAYLOAD_MAGIC;
    hdr->_nSegs = 1;
    hdr->_elemSize = attrSize;
    hdr->_dataSize = dataSize;
    hdr->_varOffs = 0;
    hdr->_isBoolean = 0;
    ::memset(&hdr->_pad[0], 0, sizeof(hdr->_pad));
    PayloadSegment* seg = (PayloadSegment*) (hdr+1);
    *seg =  PayloadSegment(0,0,false,false);
    ++seg;
    *seg =  PayloadSegment(inputCount, inputCount,false,false);
    ++seg;
    char* dataPtr = reinterpret_cast<char*>(seg);
    std::shared_ptr<ConstChunkIterator> src = chunk.getConstIterator();
    while (!src->end())
    {
        Value const& v = src->getItem();
        char const* d = reinterpret_cast<char*> (v.data());
        //TODO: this fails if v is null
        ::memcpy(dataPtr, d, attrSize);
        dataPtr = dataPtr + attrSize;
        ++(*src);
    }
}


DelegateArrayIterator* DeRLEArray::createArrayIterator(const AttributeDesc& id) const
{
    return new DeRLEArray::ArrayIterator(*(DeRLEArray*)this, id, getPipe(0)->getConstIterator(id));
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
