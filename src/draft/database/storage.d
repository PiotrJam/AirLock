/*
 Copyright: Copyright Piotr Półtorak 2015-2016.
 License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Authors: Piotr Półtorak
 */

module draft.database.storage;

import std.traits;
import std.stdio;

enum magicString = "DLDb";
enum minimalPagaSize = 128;


immutable dbAllocResolution = 4; //in bytes

enum PageNo: uint 		 { Null = 0, Master = 1 }

enum DbItemFlags: ubyte  { CellEmbedded = 2<<0, Compressed = 2<<1, Fragmented = 2<<2 }

enum DbTableFlags: ubyte { FixedSizeItem = 2<<0, MetaInfoEmbedded = 2<<1 }

enum DbType: ubyte	   	 { Char, Wchar, Dchar, 
                           Ubyte, Byte, Ushort, Short, Uint, Int, Ulong, Long, 
                          Float, Double, 
                          Array, StructStart, StructEnd, DbReferece }

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
        return cast(uint)(rawData >> 32) & 0x00FF_FFFF ;
    }
    
    void offset(uint offset)
    {
        rawData = rawData & 0xFF00_0000_FFFF_FFFF;
        rawData = rawData | (cast(ulong)offset << 32);
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

    void writeCell(uint offset, ubyte[] data)
    {
        mRawBytes[offset..offset + data.length] = data;
    }

    DbCell readCell(uint offset)
    {
        DbCell cell = DbCell(mRawBytes[offset..$]);
        return cell;
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
    string mPath;
    uint mPageSize;
    uint mPageCount;
    ubyte[] mBuffer;
    uint[] freePageIds = [];
    
    this(string path, uint pageSize)
    {
        mPath = path;
        mPageSize = pageSize;
    }
    
    DbPage loadPage(uint pageNo)
    {
        DbPage page = DbPage(pageNo, mPageSize);
        page.mRawBytes = mBuffer[cast(size_t)(pageNo-1) * mPageSize .. cast(size_t)pageNo * mPageSize].dup;
        return page;
    }

    void writePage(uint pageNo, ubyte[] pageData)
    {
        mBuffer[cast(size_t)(pageNo-1)*mPageSize..cast(size_t)pageNo*mPageSize] = pageData;
    }

    uint reserveFreePage(uint count = 1)
    {

        mBuffer.length += mPageSize*count;
        uint result = cast(uint)mBuffer.length / mPageSize;
        mPageCount += count;
        return result;
    }

    void dump()
    {
        for(int i=1; i <= (mBuffer.length / mPageSize); ++i)
        {
            loadPage(i).dump;
        }
    }

}

struct DbStorage
{
    DbFile * mDbFile;
    DbNavigator mNavigator;
    DbDataAllocator mDataAllocator;

    this(DbFile * dbFile)
    {
        mDbFile = dbFile;
        mNavigator = DbNavigator(dbFile, mDbFile.mPageSize);
        mDataAllocator = DbDataAllocator(dbFile);
    }

    uint createTable(uint pageNo)
    {
        assert(pageNo == PageNo.Null);

        pageNo = mDbFile.reserveFreePage;

        DbPage page = mDbFile.loadPage(pageNo);
        if(pageNo == PageNo.Master) // this means we have to initialize all db info
        {
            DbHeader dbHeader;
            page.writeDbHeader(dbHeader);
        }
        page.writeTableHeader(DbTableHeader());
        mDbFile.writePage(pageNo, page.mRawBytes);
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
        mDbFile.writePage(slotPage.mPageNo, slotPage.mRawBytes);
        mDbFile.writePage(pageNo, tableRootPage.mRawBytes);
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
            DbPage dataPage = mDbFile.loadPage(pointer.pageNo);
            cell = dataPage.readCell(pointer.offset);
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
        mDbFile.writePage(slotPage.mPageNo, slotPage.mRawBytes);
        mDbFile.writePage(tableRootPage, rootPage.mRawBytes);
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
        mDbFile.writePage(slotPageDel.mPageNo, slotPageDel.mRawBytes);

        // Update item count and save root page along with the table header
        --dbTableHeader.itemCount;
        rootPage.writeTableHeader(dbTableHeader);
        mDbFile.writePage(rootPage.mPageNo, rootPage.mRawBytes);
    }

    void dropTable(ulong pageNo)
    {
        assert(0);
    }
}
struct DbDataAllocator
{
    DbFile * mDbFile;

    this(DbFile * dbFile)
    {
        assert(dbFile);
        mDbFile = dbFile;
        if (mDbFile.mPageCount > 0)
        {
            DbPage masterPage = mDbFile.loadPage(PageNo.Master);
            DbHeader header = masterPage.readDbHeader;
        }
    }

    DbAllocationResults allocateData(DbPointer freeDataPtr, ubyte[] cellData)
    {

        if (freeDataPtr.pageNo == PageNo.Null)
        {
            freeDataPtr = makeNewHeapPage(64);
        }

        DbPointer nextFreeSpacePtr = writeCellData(freeDataPtr, cellData, 64);
        if (nextFreeSpacePtr.pageNo == PageNo.Null)
        {
            nextFreeSpacePtr = makeNewHeapPage(64);
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
        uint pageNo = mDbFile.reserveFreePage();
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
        mDbFile.writePage(newHeapPage.mPageNo, newHeapPage.mRawBytes);
        return beginPointer;
    }

    DbPointer writeCellData(DbPointer heapDataPtr, ubyte[] data, uint binSize)
    {
        assert(data.length <= binSize, "More data than bin not supported yet!");
        DbPage currHeapPage = mDbFile.loadPage(heapDataPtr.pageNo);
        ubyte[] bytes = currHeapPage.readBytes(heapDataPtr.offset, binSize);
        DbPointer nextFreePtr = DbPointer( *cast(ulong*)cast(void*)bytes.ptr);

        bool saved = false;

        currHeapPage.writeBytes(heapDataPtr.offset, data);
        mDbFile.writePage(heapDataPtr.pageNo, currHeapPage.mRawBytes);
        while(!saved)
        {
            saved = true;
        }

        return nextFreePtr;
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
        DbDataAllocator allocator = DbDataAllocator(new DbFile("",pageSize));

        auto res = allocator.makeNewHeapPage(32);

        
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

    this(DbFile * dbFile, uint pageSize)
    {
        mDbFile = dbFile;
    }

    DbPage aquireDbSlotPage(DbPage rootPage, ulong itemId)
    {

        uint slotPageNo = 0;

        uint slotsPerPage = mDbFile.mPageSize / 8;
        uint lookupIndex = cast(uint)((itemId-1) / slotsPerPage);

        DbPage finalLookupPage;

        //Firstly check if a new slot page is needed.
        if( isSlotPageAllocNeeded(itemId) )
        {
            slotPageNo = mDbFile.reserveFreePage();
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

unittest
{
    static struct TestData
    {
        int a;
        int b;
        int c;
    }

    writeln("Unittest [storage.d] start");

    DbPage page = DbPage(PageNo.Master,256);
    assert (page.mRawBytes.length == 256);

    DbHeader dbHeader;

    DbTableHeader tableHeader;
    tableHeader.itemCount = 0;

    page.writeDbHeader(dbHeader);
    page.writeTableHeader(tableHeader);

    TestData testData= TestData(11,12,13);
    DbCell cell;
    cell.from(testData);
    auto freeSpaceOffset = cast(uint)(page.mTableHeaderOffset+DbTableHeader.sizeof + tableHeader.itemCount * TestData.sizeof);
    page.writeCell(freeSpaceOffset, cast(ubyte[])cell.data);
    tableHeader.itemCount = 1;
    page.writeTableHeader(tableHeader);
    TestData testData2= TestData(111,112,113);
    DbCell cell2;
    cell2.from(testData2);
    freeSpaceOffset = cast(uint)(page.mTableHeaderOffset+DbTableHeader.sizeof + tableHeader.itemCount * TestData.sizeof);
    page.writeCell(freeSpaceOffset, cast(ubyte[])cell2.data);

    DbPage page2 = DbPage(PageNo.Master,256);
    page2.mRawBytes = page.mRawBytes;
    uint offset = cast(uint)(page.mTableHeaderOffset+DbTableHeader.sizeof + tableHeader.itemCount * TestData.sizeof);
    DbCell cell3 = page2.readCell(offset);

    TestData result = cell3.to!TestData();

    assert (result == TestData(111,112,113));
    writeln("Unittest [storage.d] passed!");
}
