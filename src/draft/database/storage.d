﻿/*
 Copyright: Copyright Piotr Półtorak 2015-2016.
 License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Authors: Piotr Półtorak
 */

module draft.database.storage;

import std.traits;
import std.stdio;
import std.file;

enum magicString = "DLDb";
enum minimalPagaSize = 128;
enum tempBinSize = 32;

immutable dbAllocResolution = 4; //in bytes

enum PageNo: uint 		 { Null = 0, Master = 1 }

enum DbItemFlags: ubyte  { CellEmbedded = 1<<0, Compressed = 1<<1, Fragmented = 1<<2 }

enum DbTableFlags: ubyte { FixedSizeItem = 1<<0, MetaInfoEmbedded = 1<<1 }

enum DbType: ubyte	   	 { Char, Wchar, Dchar, 
                           Ubyte, Byte, Ushort, Short, Uint, Int, Ulong, Long, 
                          Float, Double, 
                          Array, StructStart, StructEnd, DbReferece }

enum DbFlags: uint      { InMemory = 1}

struct DbParams
{
    uint pageSize = minimalPagaSize;
    uint flags;
}

struct TableInfo
{
    string name;
    uint pageNo;
    DbType[] typeInfoArray;
}

struct DbAllocationResults
{
    DbPointer dataPtr;
    DbPointer freeSpacePtr;
}

struct DbHeader  
{
align(1):
    immutable(char)[4] magicString= .magicString; 
    uint pageSize = 0;
    uint freePage = PageNo.Null;
}

struct DbTableHeader  
{
align(1):
    ulong itemCount = 0;
    DbPointer freeDataPtr;
}

struct DbPointer
{
    ulong rawData = 0;
    
    ulong cellData()
    {
        // get lower 7 bytes
        return rawData & 0x00FF_FFFF_FFFF_FFFF;
    }
    
    void cellData(ulong cellData)
    {
        // get lower 7 bytes
        rawData = rawData & 0xFF00_0000_0000_0000;
        rawData = rawData | cellData;
    }
    
    uint offset()
    {
        // get higher 4 bytes with cleared flag byte
        return cast(uint)(rawData >> tempBinSize) & 0x00FF_FFFF ;
    }
    
    void offset(uint offset)
    {
        rawData = rawData & 0xFF00_0000_FFFF_FFFF;
        rawData = rawData | (cast(ulong)offset << tempBinSize);
    }
    
    uint pageNo()
    {
        // get lower 4 bytes
        return cast(uint)rawData;
    }

    void pageNo(uint pageNo)
    {
        rawData = rawData & 0xFFFF_FFFF_0000_0000;
        rawData = rawData | pageNo;
    }
    
    ubyte flags()
    {
        // get the highest byte
        return cast(ubyte)(rawData >> 56);
    }
    
    void flags(ubyte flags)
    {
        // get the highest byte
        
        rawData = rawData & 0x00FF_FFFF_FFFF_FFFF;
        rawData =  rawData | (cast(ulong)flags << 56);
    }
    
    string toString()
    {
        import std.conv;
        return "{flags=" ~ flags.to!string ~ " offset=" ~ offset.to!string ~ " pageNo=" ~ pageNo.to!string ~ "}";
    }
    
}

struct DbPage
{
    uint mTableHeaderOffset = 0;
    uint mDbHeaderOffset = 0;
    uint mPageNo = PageNo.Null;
    ubyte[] mRawBytes = null;

    this (uint pageNo, uint pageSize)
    {
        mTableHeaderOffset = (pageNo == PageNo.Master) ? DbHeader.sizeof : 0;
        mRawBytes.length = pageSize;
        mPageNo = pageNo;
    }

    void writeDbHeader(DbHeader dbHeader)
    {
        mRawBytes[0..DbHeader.sizeof] = cast(ubyte[])(cast(void*)&dbHeader)[0..DbHeader.sizeof];
    }

    DbHeader readDbHeader()
    {
        return *(cast(DbHeader*)cast(void*)mRawBytes.ptr);
    }

    void writeTableHeader(DbTableHeader tableHeader)
    {
        mRawBytes[mTableHeaderOffset..mTableHeaderOffset+DbTableHeader.sizeof] = cast(ubyte[])(cast(void*)&tableHeader)[0..DbTableHeader.sizeof];
    }

    DbTableHeader readTableHeader()
    {
        return *(cast(DbTableHeader*)cast(void*)mRawBytes[mTableHeaderOffset..mTableHeaderOffset+DbTableHeader.sizeof]);
    }

    void writeSlot(uint index, DbPointer pointer)
    {
        uint offset = index * cast(uint)DbPointer.sizeof;
        ulong rawPointer = pointer.rawData;
        mRawBytes[offset..offset + DbPointer.sizeof] = cast(ubyte[])(cast(void*)&rawPointer)[0..DbPointer.sizeof];
    }

    DbPointer readSlot(uint index, int size)
    {
        uint offset = index * cast(uint)DbPointer.sizeof;
        DbPointer pointer = DbPointer(*cast(ulong*)(cast(void*)(&mRawBytes[offset])));
        return pointer;
    }

    void writeLookupPointer(uint index, uint pointer)
    {
        auto offset = mTableHeaderOffset+DbTableHeader.sizeof + index * uint.sizeof;
        mRawBytes[offset..offset + pointer.sizeof] = cast(ubyte[])(cast(void*)(&pointer))[0..pointer.sizeof];
    }

    uint readLookupPointer(uint index)
    {
        auto offset = mTableHeaderOffset+DbTableHeader.sizeof + index * uint.sizeof;
        return *(cast(uint*)cast(void*)mRawBytes[offset..offset+uint.sizeof]);
    }

    void writeBytes(uint offset, ubyte[] data)
    {
        mRawBytes[offset..offset + data.length] = data;
    }

    ubyte[] readBytes(uint offset, int count)
    {
        return mRawBytes[offset..offset + count];
    }

    void dump(int bytesPerLine = 8)
    {
        assert (mPageNo);
        import std.range;
        import std.stdio;
        int lineNo;
        writefln( "-------------------- Page %2d --------------------------", mPageNo);
        foreach(line; chunks(cast(ubyte[]) mRawBytes,bytesPerLine))
        {
            writef( "Offset %4d:", lineNo*bytesPerLine);
            foreach(byteOfData; line)
            {
                writef("%4s ",byteOfData);
            }
            writeln();
            ++lineNo;
        }
        writefln( "-------------------------------------------------------", mPageNo);
    }

}

struct DbCell
{
    ubyte[] data;

    this(ubyte[] cellData)
    {
        data = cellData;
    }

    
    this(ulong cellData)
    {
        data.length = cellData.sizeof;
        data[0..cellData.sizeof] = cast(ubyte[])((cast(void*)&cellData)[0..cellData.sizeof]);
    }

    void from (T)(T item)
    {
        data.reserve = 256;
        static if(is(T == struct))
        {
            foreach(idx, memberType; FieldTypeTuple!(T))
            {
                static if(isArray!memberType)
                {
                    alias ElementType = typeof(item.tupleof[idx][0]);
                    static if(isBasicType!ElementType)
                    {
                        ulong length = item.tupleof[idx].length;
                        data ~= (cast(ubyte*)(&length))[0..8];

                        foreach (el ; item.tupleof[idx])
                        {
                            data  ~= (cast(ubyte*)&el)[0..ElementType.sizeof];
                        }
                    }
                    else
                    {
                        assert(false);
                    }
                }
                else static if(isBasicType!memberType)
                {
                    data ~= (cast(ubyte*)(&item.tupleof[idx]))[0..memberType.sizeof];
                }
                else
                {
                    assert(false);
                }
            }
        }
        else
        {
            assert(false);
        }
    }

    T to (T)()
    {
        T item;
        assert(data, "Invalid cell data");
        static if(is(T == struct))
        {
            foreach(idx, memberType; FieldTypeTuple!(T))
            {
                static if(isArray!memberType)
                {
                    alias ElementType = typeof(item.tupleof[idx][0]);
                    static if(isBasicType!ElementType)
                    {
                        item.tupleof[idx].length = *cast(size_t*)data[0..ulong.sizeof];
                        data = data[ulong.sizeof..$];
                        
                        foreach (i, el ; item.tupleof[idx])
                        {
                            cast(Unqual!ElementType)item.tupleof[idx][i] = *cast(ElementType*)data[0..ElementType.sizeof];
                            data = data[ElementType.sizeof..$];
                        }
                    }
                    else
                    {
                        assert(false);
                    }
                }
                else static if(isBasicType!memberType)
                {
                    alias typeof(item.tupleof[idx]) targetType;
                    item.tupleof[idx] = *cast(targetType*)data[0..targetType.sizeof];
                    data = data[targetType.sizeof..$];
                }
                else
                {
                    assert(false);
                }
            }
        }
        else
        {
            assert(false);
        }

        return item;
    }

    unittest
    {
        writeln("Unittest [Cell] start");

        static struct B
        {
            byte a;
            int b;
            uint c;
            ulong d;
            double e;
            float f;
        }

        static struct C
        {
            int a;
            char[] chars;
            string str;
        }

        static struct D
        {
            string str;
            int[] a;
        }

        B b = B(110,-5_441_697,3_456_924_743,7_648_136_946_296, -4.2, 10.125 );
        DbCell cellB;
        cellB.from(b);

        C c = C(-1_345_429_012, ['h','e','l','l','o'], "world");
        DbCell cellC;
        cellC.from(c);

        D d = D("D Programming Language", [-1_345_429_012, -1, 0, 1, 1_345_429_012]);
        DbCell cellD;
        cellD.from(d);

        DbCell cellB2 = DbCell(cellB.data);
        B b2 = cellB2.to!B();
        assert(b2 == b);

        DbCell cellC2 = DbCell(cellC.data);
        C c2 = cellC2.to!C();
        assert(c2 == c);

        DbCell cellD2 = DbCell(cellD.data);
        D d2 = cellD2.to!D();
        assert(d2 == d);
        writeln("Unittest [Cell] passed!");
    }
}

struct DbFile
{
    string mFilePath;
    ubyte[] mBuffer;
    uint mPageSize;
    uint mPageCount;
    uint mFlags;

    this(string path, DbParams params)
    {
        mPageSize = params.pageSize;
        mFlags  = params.flags;
        mFilePath = path;
    }

    DbPage loadPage(uint pageNo)
    {
        DbPage page = DbPage(pageNo, mPageSize);

        if(mFlags & DbFlags.InMemory)
        {
            page.mRawBytes = mBuffer[cast(size_t)(pageNo-1) * mPageSize .. cast(size_t)pageNo * mPageSize].dup;
        }
        else
        {
            auto file = File(mFilePath);
            file.seek( (pageNo-1) * mPageSize);
            file.rawRead(page.mRawBytes);
        }
        return page;
    }

    void storePage(uint pageNo, ubyte[] pageData)
    {
        if(mFlags & DbFlags.InMemory)
        {
            mBuffer[cast(size_t)(pageNo-1)*mPageSize..cast(size_t)pageNo*mPageSize] = pageData;
        }
        else
        {
            ulong offset = (pageNo-1) * mPageSize;
            assert (pageNo <= mPageCount, "Writing page over file size"); 
            auto file = File(mFilePath, "r+");
            file.seek( (pageNo-1) * mPageSize);
            file.rawWrite(pageData);
        }
    }

    uint appendPages(uint count)
    {
        mPageCount += count;
        if(mFlags & DbFlags.InMemory)
        {
            mBuffer.length = mPageSize*mPageCount;
        }
        else
        {
            auto file = File(mFilePath, "r+");
            file.seek(mPageSize*mPageCount-1);
            file.rawWrite([cast(byte)0]);
        }
        

        return mPageCount;
    }

    static DbFile create(string path, DbParams params)
    {
        if( (params.flags && DbFlags.InMemory) == false)
        {
            if(!exists(path))
            {
                File(path, "w");
            }
            else
            {
                throw new Exception("DataBase file already exits!");
            }
        }
        return DbFile(path, params);
    }

    void dump()
    {
        for(int i=1; i <= (mBuffer.length / mPageSize); ++i)
        {
            loadPage(i).dump;
        }
    }

    unittest
    {
        import std.file;
        scope (exit) remove("test.db");

        writeln("Unittest [DbFile] start");
        auto dbFile = DbFile.create("test.db", DbParams(128));
        dbFile.appendPages(1);
        dbFile.storePage(PageNo.Master,DbPage(1,128).mRawBytes);



        writeln("Unittest [DbFile] passed!");
    }

}

struct DbStorage
{
    DbFile mDbFile;
    DbNavigator mNavigator;
    DbDataAllocator mDataAllocator;
    DbPageAllocator mPageAllocator;

    this(string path, DbParams params)
    {
        mDbFile = DbFile(path,params);
        mPageAllocator = DbPageAllocator(&mDbFile);
        mNavigator = DbNavigator(&mDbFile, &mPageAllocator);
        mDataAllocator = DbDataAllocator(&mDbFile, &mPageAllocator);
    }

    this(DbFile dbFile)
    {
        mDbFile = dbFile;
        mPageAllocator = DbPageAllocator(&mDbFile);
        mNavigator = DbNavigator(&mDbFile, &mPageAllocator);
        mDataAllocator = DbDataAllocator(&mDbFile, &mPageAllocator);
    }

    uint initializeStorage()
    {
        uint pageNo = mPageAllocator.reserveFreePage;

        assert (pageNo == PageNo.Master);
        DbPage page = mDbFile.loadPage(pageNo);
        DbHeader dbHeader;
        page.writeDbHeader(dbHeader);

        page.writeTableHeader(DbTableHeader());
        mDbFile.storePage(pageNo, page.mRawBytes);
        return pageNo;
    }

    static DbStorage* create(string path, DbParams params)
    {
        auto dbFile = DbFile.create(path, params);
        return new DbStorage(dbFile);
    }

    uint createTable()
    {
        uint pageNo = mPageAllocator.reserveFreePage;
        assert(pageNo != PageNo.Null);
        DbPage page = mDbFile.loadPage(pageNo);
        page.writeTableHeader(DbTableHeader());
        mDbFile.storePage(pageNo, page.mRawBytes);
        return pageNo;
    }

    void addItem(T)(uint pageNo, T item)
    {
        DbPage tableRootPage = mDbFile.loadPage(pageNo);
        DbTableHeader tableHeader = tableRootPage.readTableHeader;
        auto itemCount = tableHeader.itemCount;

        DbCell cell;
        cell.from(item);

        // find slot for a new pointer in slotPage
        // from time to time new lookup pages need to be added
        DbPage slotPage = mNavigator.aquireDbSlotPage(tableRootPage, itemCount+1);

        auto slotsPerPage = mDbFile.mPageSize / 8;
        auto slotPageIndex = (itemCount) % slotsPerPage;

        if (cell.data.length > (DbPointer.sizeof - DbItemFlags.sizeof))
        {
            // we need a separate storage

            DbAllocationResults result = mDataAllocator.allocateData(tableHeader.freeDataPtr,cell.data);
            slotPage.writeSlot(cast(uint)slotPageIndex,result.dataPtr);
            
            //update free pointer
            tableHeader.freeDataPtr = result.freeSpacePtr;
        }
        else
        {
            // data fits in the slot
            DbPointer pointer;
            pointer.flags = pointer.flags | DbItemFlags.CellEmbedded;
            pointer.cellData(*cast(ulong*)(cast(void*)cell.data));
            slotPage.writeSlot(cast(uint)slotPageIndex,pointer);
        }
        //update table header
        tableHeader.itemCount++;
        tableRootPage.writeTableHeader(tableHeader);
        mDbFile.storePage(slotPage.mPageNo, slotPage.mRawBytes);
        mDbFile.storePage(pageNo, tableRootPage.mRawBytes);
    }

    ulong getNextDbItemId(uint pageNo, ulong id)
    {
        ++id;
        DbPage page = mDbFile.loadPage(pageNo);
        DbTableHeader tableHeader = page.readTableHeader;
        return (id <= tableHeader.itemCount) ? id :  0;
    }
    
    T fetchDbItem(T)(uint tableRootPage, ulong id)
    {
        DbPage rootPage = mDbFile.loadPage(tableRootPage);
        DbPage slotPage = mNavigator.getDbSlotPage(rootPage, id);

        auto slotsPerPage = mDbFile.mPageSize / 8;
        uint slotIndex = cast(uint)((id-1) % slotsPerPage);
        DbPointer pointer = slotPage.readSlot(slotIndex,DbPointer.sizeof);

        DbCell cell;
        //Check id data is embedded in the pointer

        if (pointer.flags & DbItemFlags.CellEmbedded)
        {
            cell = DbCell(pointer.cellData);
        }
        else
        {
            cell = DbCell(mDataAllocator.readCellData(pointer,tempBinSize));
        }
        return cell.to!T;
    }

    // TODO updateItem and addItem has common parts. Is it worth it to extract a few lines for both?
    void updateItem(T)(uint tableRootPage, ulong itemId, T item)
    {
        // read old item to release its storage

        DbPage rootPage = mDbFile.loadPage(tableRootPage);
        DbTableHeader tableHeader = rootPage.readTableHeader;

        DbPage slotPage = mNavigator.getDbSlotPage(rootPage, itemId);

        uint slotIndex = cast(uint)((itemId-1) % mDbFile.mPageSize);

        DbPointer itemPointer = slotPage.readSlot(slotIndex,DbPointer.sizeof);

        DbCell cell;
        cell.from(item);

        //Check if the previous data is embedded into DbPointer or not
        if (itemPointer.flags & !DbItemFlags.CellEmbedded)
        {
            //TODO
            //check how much space is allocated and reuse if possible
            //have to relese unneeded storage

        }

        if (cell.data.length > (DbPointer.sizeof - DbItemFlags.sizeof))
        {
            // we need a separate storage
            DbAllocationResults result = mDataAllocator.allocateData(tableHeader.freeDataPtr,cell.data);
            slotPage.writeSlot(cast(uint)slotIndex,result.dataPtr);
            //update free pointer
            tableHeader.freeDataPtr = result.freeSpacePtr;

        }
        else
        {
            // data fits in the slot
            DbPointer newPointer;
            newPointer.flags = newPointer.flags | DbItemFlags.CellEmbedded;
            newPointer.cellData(*cast(ulong*)(cast(void*)cell.data));
            slotPage.writeSlot(cast(uint)slotIndex,newPointer);
        }

        rootPage.writeTableHeader(tableHeader);
        mDbFile.storePage(slotPage.mPageNo, slotPage.mRawBytes);
        mDbFile.storePage(tableRootPage, rootPage.mRawBytes);
    }

    void removeItem(uint rootPageNo, ulong itemId)
    {
        DbPage rootPage = mDbFile.loadPage(rootPageNo);
        DbTableHeader dbTableHeader = rootPage.readTableHeader;
        DbPointer freeDataPtr = dbTableHeader.freeDataPtr;

        DbPage slotPageDel = mNavigator.getDbSlotPage(rootPage, itemId);
        uint slotIndexDel = cast(uint)((itemId-1) % mDbFile.mPageSize);
        DbPointer itemPointer = slotPageDel.readSlot(slotIndexDel,DbPointer.sizeof);

        // move released bins to the beggining of the free bins list
        mDataAllocator.deallocateData(itemPointer, freeDataPtr); 

        // the storage reclaimed from deleted item is in the front of the free bins list 
        dbTableHeader.freeDataPtr = itemPointer;

        // put the last item in the place of the removed one
        DbPage slotPageLast = mNavigator.getDbSlotPage(rootPage, dbTableHeader.itemCount);
        uint slotIndexLast = cast(uint)((dbTableHeader.itemCount-1) % mDbFile.mPageSize);
        DbPointer lastItemPointer = slotPageLast.readSlot(slotIndexLast, DbPointer.sizeof);
        slotPageDel.writeSlot(slotIndexDel, lastItemPointer);
        mDbFile.storePage(slotPageDel.mPageNo, slotPageDel.mRawBytes);

        // Update item count and save root page along with the table header
        --dbTableHeader.itemCount;
        rootPage.writeTableHeader(dbTableHeader);
        mDbFile.storePage(rootPage.mPageNo, rootPage.mRawBytes);
    }

    void dropTable(ulong pageNo)
    {
        assert(0);
    }

    unittest
    {
        writeln("Unittest [DbStorage] start");

        static struct C
        {
            int a;
            string name;
        }

        DbStorage storage = DbStorage("",DbParams(256, DbFlags.InMemory));

        uint masterTablePageNo = storage.initializeStorage();

        uint regularTable = storage.createTable();

        auto bigItem = C(5, "123456789 123456789 123456789 123456789 123456789 " //50
                            "123456789 123456789 123456789 123456789 1234567890"); //100
        storage.addItem(regularTable,bigItem); 

        assert(storage.fetchDbItem!C(regularTable, 1) == bigItem);

        writeln("Unittest [DbStorage] passed!");
    }

}

struct DbPageAllocator
{
    DbFile* mDbFile;

    uint reserveFreePage(uint count = 1)
    {
        return mDbFile.appendPages(1);
    }

}

struct DbDataAllocator
{
    DbFile *mDbFile;
    DbPageAllocator *mPageAllocator;

    DbAllocationResults allocateData(DbPointer freeDataPtr, ubyte[] cellData, int binSize = tempBinSize)
    {
        assert(cellData.length > 0);

        if (freeDataPtr.pageNo == PageNo.Null)
        {
            freeDataPtr = makeNewHeapPage(binSize);
        }

        DbPointer nextFreeSpacePtr = writeCellData(freeDataPtr, cellData, binSize);
        if (nextFreeSpacePtr.pageNo == PageNo.Null)
        {
            nextFreeSpacePtr = makeNewHeapPage(binSize);
        }

        DbAllocationResults result;
        result.dataPtr = freeDataPtr;
        result.freeSpacePtr = nextFreeSpacePtr;
        return result;
    }

    void deallocateData(DbPointer dataPtr, DbPointer freeDataPtr)
    {
        DbPage dataPage = mDbFile.loadPage(dataPtr.pageNo);
        ubyte[] bytes = cast(ubyte[])(cast(void*)&freeDataPtr.rawData)[0..DbPointer.sizeof];
        dataPage.writeBytes(dataPtr.offset, bytes);
    }

    DbPointer makeNewHeapPage(int binSize)
    {
        uint pageNo = mPageAllocator.reserveFreePage();
        DbPointer beginPointer;
        beginPointer.pageNo = pageNo;
        DbPage newHeapPage = mDbFile.loadPage(pageNo);
        for (int offset = 0; offset < mDbFile.mPageSize ; offset+= binSize)
        {
            DbPointer binPointer;

            uint pointerOffset = offset+ binSize;
            if ( pointerOffset <= (mDbFile.mPageSize -binSize))
            {
                binPointer.offset = pointerOffset;
                binPointer.pageNo = pageNo;
            }

            newHeapPage.writeBytes(offset, cast(ubyte[])(cast(void*)&binPointer.rawData)[0..DbPointer.sizeof]);
        }
        mDbFile.storePage(newHeapPage.mPageNo, newHeapPage.mRawBytes);
        return beginPointer;
    }

    DbPointer writeCellData(DbPointer heapDataPtr, ubyte[] data, uint binSize)
    {
        DbPage currHeapPage = mDbFile.loadPage(heapDataPtr.pageNo);
        ubyte[] nextFreePtrBytes = currHeapPage.readBytes(heapDataPtr.offset, DbPointer.sizeof);
        DbPointer nextFreePtr = DbPointer( *cast(ulong*)cast(void*)nextFreePtrBytes.ptr);
        // TODO Expand to 2^64 available size. Currently 256
        ubyte[] cellLengthBytes = [cast(ubyte)data.length];
        assert(data.length < 256);
        const ulong bytesToWrite = data.length + cellLengthBytes.length;
        long leftDataBytes = bytesToWrite;

        if (bytesToWrite + DbPointer.sizeof <= binSize)
        {
            currHeapPage.writeBytes(heapDataPtr.offset+ cast(uint)DbPointer.sizeof, cellLengthBytes);
            currHeapPage.writeBytes(heapDataPtr.offset+ cast(uint)DbPointer.sizeof + cast(uint)cellLengthBytes.length, data);
            mDbFile.storePage(currHeapPage.mPageNo, currHeapPage.mRawBytes);
        }
        else
        {
            //data needs to be fragmented. It's bigger than binSize.
            size_t dataOffset = 0;
            // if data length is bigger than binSize then dataOffsetEnd is inside data array
            size_t dataSegmentSize = binSize - DbPointer.sizeof - cellLengthBytes.length;

            assert (dataSegmentSize <= bytesToWrite, "dataSegmentSize to large");


            // store cell size
            currHeapPage.writeBytes(heapDataPtr.offset+cast(uint)DbPointer.sizeof,cellLengthBytes);
            heapDataPtr.offset = cast(uint)(heapDataPtr.offset+ cellLengthBytes.length);
            leftDataBytes -= cellLengthBytes.length;
            while(true)
            {
                // actual data is written after the pointer and in the case of the first segment, also cell size
                ubyte[] dataPortion = data[dataOffset..dataOffset+dataSegmentSize];
                currHeapPage.writeBytes(heapDataPtr.offset+cast(uint)DbPointer.sizeof, dataPortion);

                mDbFile.storePage(heapDataPtr.pageNo, currHeapPage.mRawBytes);

                dataOffset += dataSegmentSize;

                //  dataSegmentSize should be always adapted to the actual data length
                leftDataBytes -= dataSegmentSize;
                dataSegmentSize = (leftDataBytes<=binSize) ? leftDataBytes : binSize - DbPointer.sizeof;

                assert(nextFreePtr.pageNo != PageNo.Null, "No more free space");
                // follow free list
                heapDataPtr = nextFreePtr;
                currHeapPage = mDbFile.loadPage(nextFreePtr.pageNo);

                nextFreePtrBytes = currHeapPage.readBytes(nextFreePtr.offset, DbPointer.sizeof);
                nextFreePtr = DbPointer( *cast(ulong*)cast(void*)nextFreePtrBytes.ptr);

                if (leftDataBytes <= binSize)
                {
                    //write the last portion
                    dataPortion = data[dataOffset..dataOffset+dataSegmentSize];
                    currHeapPage.writeBytes(heapDataPtr.offset, dataPortion);
                    mDbFile.storePage(currHeapPage.mPageNo, currHeapPage.mRawBytes);
                    break;
                }
            }
        }

        if(nextFreePtr.pageNo == PageNo.Null)
        {
            nextFreePtr = makeNewHeapPage(binSize);
        }
        assert(nextFreePtr.rawData, "No free bin available");
        return nextFreePtr;
    }

    ubyte[] readCellData(DbPointer dataPtr, uint binSize)
    {
        ubyte[] cellData;
        DbPage dataPage = mDbFile.loadPage(dataPtr.pageNo);
        // TODO expand to 2^64
        ubyte[] lenghtBytes = dataPage.readBytes(dataPtr.offset + cast(uint)DbPointer.sizeof, 1);
        const ulong bytesToRead = lenghtBytes[0];
        ulong dataLeft = bytesToRead;
        cellData.length = bytesToRead;
        if ( (bytesToRead + DbPointer.sizeof + lenghtBytes.length) <= binSize)
        {
            cellData = dataPage.readBytes(dataPtr.offset+ cast(uint)DbPointer.sizeof + cast(uint)lenghtBytes.length, bytesToRead);
        }
        else
        {
            ubyte[] nextDataPtrBytes = dataPage.readBytes(dataPtr.offset, DbPointer.sizeof);

            DbPointer nextPtr = DbPointer( *cast(ulong*)cast(void*)nextDataPtrBytes.ptr);
            size_t dataOffset = 0;
            // if data length is bigger than binSize then dataOffsetEnd should be inside data array
            size_t dataSegmentSize = binSize - DbPointer.sizeof - lenghtBytes.length;
            dataPtr.offset = cast(uint)(dataPtr.offset + lenghtBytes.length);
            while(true)
            {
                cellData[dataOffset..dataOffset+dataSegmentSize] = dataPage.readBytes(dataPtr.offset+cast(uint)DbPointer.sizeof, cast(uint)dataSegmentSize);
                dataOffset += dataSegmentSize;
                dataLeft -= dataSegmentSize;

                dataPtr = nextPtr;

                nextDataPtrBytes = dataPage.readBytes(dataPtr.offset, DbPointer.sizeof);
                nextPtr = DbPointer( *cast(ulong*)cast(void*)nextDataPtrBytes.ptr);

                if (dataLeft < binSize)
                {
                    dataPage = mDbFile.loadPage(dataPtr.pageNo);
                    cellData[dataOffset..dataOffset+dataLeft] = dataPage.readBytes(dataPtr.offset, cast(uint)dataLeft);
                    break;
                }
                dataSegmentSize = binSize - DbPointer.sizeof;
            }

        }

        return cellData;
    }

    int bitmapSize()
    {
        // 8 means 8 bits per byte
        return mDbFile.mPageSize / ( 8 * dbAllocResolution);
    }

    unittest
    {
        writeln("Unittest [DbDataAllocator] start");

        uint pageSize = 512;
        uint testTableTootPage = 1;
        auto dbFile = new DbFile("",DbParams(256, DbFlags.InMemory));
        auto pageAlloc = new  DbPageAllocator(dbFile);
        DbDataAllocator allocator = DbDataAllocator(dbFile, pageAlloc);

        auto res = allocator.makeNewHeapPage(tempBinSize);

        
        static struct A
        {
            string test = "Test";
        }

        A t;
        DbCell cell;
        cell.from(t);
        allocator.allocateData(DbPointer(0), cell.data);
        writeln("Unittest [DbDataAllocator] passed!");
    }
}

struct DbNavigator
{
    DbFile* mDbFile;
    DbPageAllocator* mPageAllocator;


    DbPage aquireDbSlotPage(DbPage rootPage, ulong itemId)
    {

        uint slotPageNo = 0;

        uint slotsPerPage = mDbFile.mPageSize / 8;
        uint lookupIndex = cast(uint)((itemId-1) / slotsPerPage);

        DbPage finalLookupPage;

        //Firstly check if a new slot page is needed.
        if( isSlotPageAllocNeeded(itemId) )
        {
            slotPageNo = mPageAllocator.reserveFreePage();
            finalLookupPage = aquireFinalLookupPage(rootPage, itemId);
            finalLookupPage.writeLookupPointer(lookupIndex,slotPageNo);

        }
        else
        {
            finalLookupPage = aquireFinalLookupPage(rootPage, itemId);

            slotPageNo = finalLookupPage.readLookupPointer(lookupIndex);
        }

        return mDbFile.loadPage(slotPageNo);
    }

    DbPage getDbSlotPage(DbPage rootPage, ulong itemId)
    {
        auto slotsPerPage = mDbFile.mPageSize / 8;
        uint slotPageIndex = cast(uint)((itemId-1) / slotsPerPage);
        DbPage finalLookupPointer = aquireFinalLookupPage(rootPage, itemId);
        auto slotPage = finalLookupPointer.readLookupPointer(slotPageIndex);
        return mDbFile.loadPage(slotPage);
    }

    DbPage aquireFinalLookupPage(DbPage rootPage, ulong itemId)
    {
        DbPage finalLookupPage = rootPage;
        // Check if lookup structure should be expanded	
        if ( isLookupPageAllocNeeded(itemId))
        {
            // TODO Handle tree expansion
            assert(0);
        }
        else
        {

        }

        return finalLookupPage;
    }

    bool isSlotPageAllocNeeded(ulong itemId)
    {
        return (itemId % (mDbFile.mPageSize/DbPointer.sizeof) == 1);
    }

    bool isLookupPageAllocNeeded(ulong itemId)
    {
        uint slotsPerPage = mDbFile.mPageSize / DbPointer.sizeof;

        // TODO Add condition for lookup tree expansion
        return false;
    }

    unittest
    {
        writeln("Unittest [DbNavigator] start");
        DbNavigator navigator;

        writeln("Unittest [DbNavigator] passed!");
    }
}
/**
 * Function converts ulong for array of nine bytes
 * In each field of array there is 7 bits for information 
 * from ulong, One bit is for information if next field of array
 * keeps data of ulong number
 * Params:
 * 	   number =  ulong to convert for array
 * Returns: Array to save in file
 */
byte[9] convertULtoTable(ulong number)
{
	size_t i = 0;
	byte[9] table;
	while(i < 9 && (number & (0xFFFFFFFFFFFFFFFF << (7 * i))))
		table[i] = cast(byte)((number >> (7 * i++)) & 0x7F | 0x80);
	if(i && !(number >> 63))table[i-1] &= 0x7F;
	return table;
}
/**
 * Function converts array to ulong
 * 7 bits of every field in array is ulong information
 * 1 bit is informaton if in next field of array there still more
 * data for ulong
 * Params: 
 * 		table = Array of bytes with information of ulong number
 * Returns: ulong number
 */
ulong convertTableToUL(byte[] table)
{
	ulong result = (table.length == 9) ? (cast(ulong)(table[8] & 0x80) << 56):0;
	for(size_t i=0; i < table.length; ++i)
		result |= cast(ulong)(table[i] & 0x7F) << (7 * i);
	return result;
}
unittest
{
	void checkConvertULtoTable(ulong num, int t0 = 0x0, int t1 = 0x0, int t2 = 0x0, int t3 = 0x0,
		int t4 = 0x0, int t5 = 0x0, int t6 = 0x0, int t7 = 0x0, int t8 = 0x0)
	{
		byte[9] tab = convertULtoTable(num);
		assert(tab[0] == cast(byte)t0);
		assert(tab[1] == cast(byte)t1);
		assert(tab[2] == cast(byte)t2);
		assert(tab[3] == cast(byte)t3);
		assert(tab[4] == cast(byte)t4);
		assert(tab[5] == cast(byte)t5);
		assert(tab[6] == cast(byte)t6);
		assert(tab[7] == cast(byte)t7);
		assert(tab[8] == cast(byte)t8);
	}
	writeln("Unittest [convertULtoTable] start");
	checkConvertULtoTable(0x0);
	checkConvertULtoTable(0x1,0x1);
	checkConvertULtoTable(0x7F,0x7F);
	checkConvertULtoTable(0x80,0x80,0x01);
	checkConvertULtoTable(0xFF,0xFF,0x01);
	checkConvertULtoTable(0x3FFF,0xFF,0x7F);
	checkConvertULtoTable(0x4000,0x80,0x80,0x01);
	checkConvertULtoTable(0x1FFFFF,0xFF,0xFF,0x7F);
	checkConvertULtoTable(0x200000,0x80,0x80,0x80,0x01);
	checkConvertULtoTable(0x0FFFFFFF,0xFF,0xFF,0xFF,0x7F);
	checkConvertULtoTable(0x10000000,0x80,0x80,0x80,0x80,0x01);
	checkConvertULtoTable(0x7FFFFFFFF,0xFF,0xFF,0xFF,0xFF,0x7F);
	checkConvertULtoTable(0x800000000,0x80,0x80,0x80,0x80,0x80,0x01);
	checkConvertULtoTable(0x3FFFFFFFFFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F);
	checkConvertULtoTable(0x40000000000,0x80,0x80,0x80,0x80,0x80,0x80,0x01);
	checkConvertULtoTable(0x1FFFFFFFFFFFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F);
	checkConvertULtoTable(0x2000000000000,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x01);
	checkConvertULtoTable(0x0FFFFFFFFFFFFFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F);
	checkConvertULtoTable(0x100000000000000,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x01);
	checkConvertULtoTable(0x7FFFFFFFFFFFFFFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x7F);
	checkConvertULtoTable(0x8000000000000000,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80,0x80);
	checkConvertULtoTable(0xFFFFFFFFFFFFFFFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF);
	checkConvertULtoTable(0xAFFFFFFFFFFFFFFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xAF);
	checkConvertULtoTable(0xF1FFFFFFFFFFFF80,0x80,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xF1);
	writeln("Unittest [convertULtoTable] passed!");

	writeln("Unittest [convertTableToUL] start");
	assert(cast(ulong)0x0 == convertTableToUL([0x0]));
	assert(cast(ulong)0x1 == convertTableToUL([0x1]));
	assert(cast(ulong)0x7F == convertTableToUL([0x7F]));
	assert(cast(ulong)0x80 == convertTableToUL([cast(byte)0x80,cast(byte)0x01]));
	assert(cast(ulong)0xFF == convertTableToUL([cast(byte)0xFF,cast(byte)0x01]));
	assert(cast(ulong)0x3FFF == convertTableToUL([cast(byte)0xFF,cast(byte)0x7F]));
	assert(cast(ulong)0x4000 == convertTableToUL([cast(byte)0x80,cast(byte)0x80,cast(byte)0x01]));
	assert(cast(ulong)0x1FFFFF == convertTableToUL([cast(byte)0xFF,cast(byte)0xFF,cast(byte)0x7F]));
	assert(cast(ulong)0x200000 == convertTableToUL([cast(byte)0x80,cast(byte)0x80,
				cast(byte)0x80,cast(byte)0x01]));
	assert(cast(ulong)0x0FFFFFFF == convertTableToUL([cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0x7F]));
	assert(cast(ulong)0x10000000 == convertTableToUL([cast(byte)0x80,cast(byte)0x80,
				cast(byte)0x80,cast(byte)0x80,cast(byte)0x01]));
	assert(cast(ulong)0x7FFFFFFFF == convertTableToUL([cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0xFF,cast(byte)0x7F]));
	assert(cast(ulong)0x800000000 == convertTableToUL([cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,
				cast(byte)0x80,cast(byte)0x80,cast(byte)0x01]));
	assert(cast(ulong)0x3FFFFFFFFFF == convertTableToUL([cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0x7F]));
	assert(cast(ulong)0x40000000000 == convertTableToUL([cast(byte)0x80,cast(byte)0x80,
				cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,cast(byte)0x01]));
	assert(cast(ulong)0x1FFFFFFFFFFFF == convertTableToUL([cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0x7F]));
	assert(cast(ulong)0x2000000000000 == convertTableToUL([cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,
				cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,cast(byte)0x01]));
	assert(cast(ulong)0x0FFFFFFFFFFFFFF == convertTableToUL([cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0x7F]));
	assert(cast(ulong)0x100000000000000 == convertTableToUL([cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,
				cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,
				cast(byte)0x01]));
	assert(cast(ulong)0x7FFFFFFFFFFFFFFF == convertTableToUL([cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0x7F]));
	assert(cast(ulong)0xFFFFFFFFFFFFFFFF == convertTableToUL([cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF]));
	assert(cast(ulong)0xAFFFFFFFFFFFFFFF == convertTableToUL([cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xAF]));
	assert(cast(ulong)0xF1FFFFFFFFFFFF80 == convertTableToUL([cast(byte)0x80,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,cast(byte)0xFF,
				cast(byte)0xFF,cast(byte)0xF1]));
	assert(cast(ulong)0x8000000000000000 == convertTableToUL([cast(byte)0x80,cast(byte)0x80,
				cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,cast(byte)0x80,
				cast(byte)0x80,cast(byte)0x80]));
	writeln("Unittest [convertTableToUL] passed!");
}