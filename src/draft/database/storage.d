/*
Copyright: Copyright Piotr Półtorak 2015-2016.
License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
Authors: Piotr Półtorak
*/

module draft.database.storage;

import std.stdio;
import std.traits;

enum magicString = "DLDb";
enum  PageNo:uint { Null =0, Master=1};

enum DbFlags:ubyte {CellEmbedded = 2^^0, Compressed = 2^^1}

struct TableInfo
{
	string name;
	uint pageNo;
}

struct DbHeader  
{
align(1):
	immutable(char)[4] magicString= .magicString; 
	uint pageSize = 0;
	uint changeCounter = 0;
}


struct DbTableHeader  
{
align(1):
	ulong itemCount = 0;
	DbPointer freeDataPtr;

}

struct DbTableMetaInfo
{

}

struct DbPage
{
	uint mTableHeaderOffset = 0;
	uint mDbHeaderOffset = 0;
	uint mPageSize = 0;
	uint mPayloadSize = 0;
	uint mPageNo = PageNo.Null;
	void[] mRawBytes = null;

	this (uint pageNo, uint pageSize)
	{
		mTableHeaderOffset = (pageNo == PageNo.Master) ? DbHeader.sizeof : 0;
		mPageSize = pageSize;
		mRawBytes.length = mPageSize;
		mPageNo = pageNo;
		mPayloadSize = mPageSize - mTableHeaderOffset;
	}

	void writeDbHeader(DbHeader dbHeader)
	{
		mRawBytes[0..DbHeader.sizeof] = cast(ubyte[])(cast(void*)&dbHeader)[0..DbHeader.sizeof];
	}

	void writeTableHeader(DbTableHeader tableHeader)
	{
		mRawBytes[mTableHeaderOffset..mTableHeaderOffset+DbTableHeader.sizeof] = cast(ubyte[])(cast(void*)&tableHeader)[0..DbTableHeader.sizeof];
	}

	void writeSlot(uint index, DbPointer pointer)
	{
		uint offset = index * cast(uint)DbPointer.sizeof;
		ulong rawPointer = pointer.rawData;
		mRawBytes[offset..offset + DbPointer.sizeof] = (cast(void*)&rawPointer)[0..DbPointer.sizeof];

	}

	void writeCell(uint offset, void[] data)
	{
		mRawBytes[offset..offset + data.length] = data;
	}


	uint loadLookupPointer(uint index)
	{
		auto offset = mTableHeaderOffset+DbTableHeader.sizeof + index * uint.sizeof;
		return *(cast(uint*)cast(void*)mRawBytes[offset..offset+uint.sizeof]);
	}

	void writeLookupPointer(uint index, uint pointer)
	{
		auto offset = mTableHeaderOffset+DbTableHeader.sizeof + index * uint.sizeof;
		mRawBytes[offset..offset + pointer.sizeof] = cast(void[])(cast(void*)(&pointer))[0..pointer.sizeof];
	}

	DbTableHeader readTableHeader()
	{
		return *(cast(DbTableHeader*)cast(void*)mRawBytes[mTableHeaderOffset..mTableHeaderOffset+DbTableHeader.sizeof]);
	}

	DbPointer readSlot(uint offset, int size)
	{

		DbPointer pointer = DbPointer(*cast(ulong*)(cast(void*)(&mRawBytes[offset])));
		return pointer;
	}

	DbPointer readPointer(uint offset)
	{
		return *cast(DbPointer*)mRawBytes[offset..offset + DbPointer.sizeof];
	}

	DbCell readCell(uint offset)
	{
		DbCell cell = DbCell(mRawBytes[offset..$]);
		return cell;
	}

	void dump(int bytesPerLine = 8)
	{
		assert (mPageNo);
		import std.range;
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
	void[] data;

	this(void[] cellData)
	{
		data = cellData;
	}


	this(ulong cellData)
	{
		data.length = cellData.sizeof;
		data[0..cellData.sizeof] = (cast(void*)&cellData)[0..cellData.sizeof];
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

struct DbPointer
{
	ulong rawData;

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

}


struct DbFile
{
	void[] mBuffer;
	uint mPageSize;
	uint[] freePageIds = [];
	
	this(ubyte[] buffer, uint pageSize)
	{
		mPageSize = pageSize;
		mBuffer = buffer;
	}
	
	DbPage loadPage(uint pageNo)
	{
		DbPage page = DbPage(pageNo, mPageSize);
		page.mRawBytes = mBuffer[cast(size_t)(pageNo-1) * mPageSize .. cast(size_t)pageNo * mPageSize].dup;
		return page;
	}

	void writePage(uint pageNo, void[] pageData)
	{
		mBuffer[cast(size_t)(pageNo-1)*mPageSize..cast(size_t)pageNo*mPageSize] = pageData;
	}

	uint reserveFreePage(uint count = 1)
	{

		mBuffer.length += mPageSize*count;
		uint result = cast(uint)mBuffer.length / mPageSize;
		return result;
	}

	void dumb()
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
	uint mPageSize;

	this(DbFile * dbFile)
	{
		mDbFile = dbFile;
		mPageSize = dbFile.mPageSize;
		mNavigator = DbNavigator(dbFile, mPageSize);
		mDataAllocator = DbDataAllocator(dbFile);
	}

	uint createTable(uint pageNo)
	{
		if (pageNo == PageNo.Null )
		{
			// allocate a new page if invalid page number provided
			// get new or unused page
			pageNo = mDbFile.reserveFreePage;
		}
		else if (pageNo == PageNo.Master)
		{

		}

		DbPage page = DbPage(pageNo,mPageSize);
		if(pageNo == PageNo.Master)
		{
			page.writeDbHeader(DbHeader());
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
		auto slotIndex = itemCount % mPageSize;

		if (cell.data.length > (DbPointer.sizeof - DbFlags.sizeof))
		{
			// we need a separate storage

			//empty table
			if (tableHeader.freeDataPtr.pageNo == PageNo.Null)
			{
				tableHeader.freeDataPtr = mDataAllocator.initialPointer();
			}
			DbPointer newDataPointer = mDataAllocator.allocateData(tableHeader.freeDataPtr,cell.data);
			DbPage dataPage = mDbFile.loadPage(tableHeader.freeDataPtr.pageNo);
			dataPage.writeCell(tableHeader.freeDataPtr.offset,cell.data);
			mDbFile.writePage(tableHeader.freeDataPtr.pageNo,dataPage.mRawBytes);
			slotPage.writeSlot(cast(uint)slotIndex,tableHeader.freeDataPtr);
			tableHeader.freeDataPtr = newDataPointer;

		}
		else
		{
			// data fits in the slot
			DbPointer pointer;
			pointer.flags = pointer.flags | DbFlags.CellEmbedded;
			pointer.cellData(*cast(ulong*)(cast(void*)cell.data));
			slotPage.writeSlot(cast(uint)slotIndex,pointer);
		}

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

		uint offset = cast(uint)((id-1) % mPageSize * ulong.sizeof);

		DbPointer pointer = slotPage.readSlot(offset,DbPointer.sizeof);

		DbCell cell;
		//Check the flag
		if (pointer.flags & DbFlags.CellEmbedded)
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

	void removeItem(T)(ulong pageNo, T item)
	{
	}

	void updateItem(T)(ulong pageNo, ulong id, T item)
	{
	}

	void dropTable(ulong pageNo)
	{
	}

}

struct DbDataAllocator
{
	DbFile * mDbFile;

	DbPointer allocateData(DbPointer freeDataPtr, void[] data)
	{
		assert (freeDataPtr.pageNo != PageNo.Null);
		DbPointer pointerNewFree = freeDataPtr;
		uint newOffset = freeDataPtr.offset + cast(uint)data.length;

		assert(newOffset < mDbFile.mPageSize);
		pointerNewFree.offset = newOffset;
		return pointerNewFree;
	}

	DbPointer initialPointer()
	{
		DbPointer pointer;
		uint pageId = mDbFile.reserveFreePage();
		pointer.pageNo = pageId;
		pointer.offset = 0;
		return pointer;
	}

}

struct DbNavigator
{
	DbFile* mDbFile;
	ulong mPageCount;
	uint mPageSize;

	this(DbFile * dbFile, uint pageSize)
	{
		mDbFile = dbFile;
		mPageSize = pageSize;
	}

	DbPage aquireDbSlotPage(DbPage rootPage, ulong itemId)
	{
		auto slotPage = 0;
		// check if we need a new lookup page
		//isLookupAllocNeeded(itemId);
		//auto left = mPageSize - mTableHeader.mItemCount * DbPointer.sizeof;
		auto slotsPerPage = mPageSize / 8;

		auto slotPageIndex = itemId / slotsPerPage;

		if(itemId % slotsPerPage == 1)
		{
			slotPage = mDbFile.reserveFreePage();
			rootPage.writeLookupPointer(cast(uint)slotPageIndex,slotPage);

		}
		else
		{
			slotPage = rootPage.loadLookupPointer(cast(uint)slotPageIndex);
		}
		// for now only one level of lookup
		return mDbFile.loadPage(slotPage);
	}

	DbPage getDbSlotPage(DbPage rootPage, ulong itemId)
	{
		auto slotsPerPage = mPageSize / 8;
		auto slotPageIndex = itemId / slotsPerPage;
		auto slotPage = rootPage.loadLookupPointer(cast(uint)slotPageIndex);
		return mDbFile.loadPage(slotPage);
	}

	void allocateLookupPages(uint itemCount)
	{
		if (isSlotPageAllocNeeded(itemCount))
		{
			// Allocate a new page for DbPointer
			DbPage newPage;
			newPage.mPageNo = mDbFile.reserveFreePage();
			uint offset = newPage.mTableHeaderOffset+cast(uint)DbTableHeader.sizeof;
			newPage.writeLookupPointer(offset,newPage.mPageNo);
		}
		
		if (isLookupAllocNeeded(itemCount))
		{
			
		}
	}


	bool isSlotPageAllocNeeded(ulong itemId)
	{
		return false;
	}
	
	bool isLookupAllocNeeded(ulong itemId)
	{
		return false;
	}

	unittest
	{
		writeln("Unittest [DbAllocator] start");
		DbNavigator navigator;
		writeln("Unittest [DbAllocator] passed!");
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
	assert (page.mPageSize == 256);
	assert (page.mRawBytes.length == 256);

	DbHeader dbHeader;

	dbHeader.pageSize = page.mPageSize;
	dbHeader.changeCounter = 17;

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
