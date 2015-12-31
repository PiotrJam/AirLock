/*
Copyright: Copyright Piotr Półtorak 2015-.
License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
Authors: Piotr Półtorak
*/

module draft.database.storage;

import std.stdio;
import std.traits;

enum magicString = "DLDb";
enum PageNo { Null =0, Master=1};

struct TableInfo
{
	string name;
	ulong pageId;
}

struct DbHeader  
{
align(1):
	immutable(char)[4] magicString= .magicString; 
	uint pageSize;
	uint changeCounter = 0;
}

struct TableHeader  
{
align(1):
	immutable(char)[4] magicString = [71, 72, 73, 74]; 
	uint itemCount = 0;
}

struct Page
{
	uint mTableHeaderOffset;
	uint mDbHeaderOffset = 0;
	uint mPageSize;
	ulong mPageNo;
	ubyte[] mRawBytes;

	this (ulong pageNo, uint pageSize)
	{
		mTableHeaderOffset = (pageNo == PageNo.Master) ? DbHeader.sizeof : 0;
		mPageSize = pageSize;
		mRawBytes.length = mPageSize;
		mPageNo = pageNo;
	}

	void writeDbHeader(DbHeader dbHeader)
	{
		mRawBytes[0..DbHeader.sizeof] = cast(ubyte[])(cast(void*)&dbHeader)[0..DbHeader.sizeof];
	}

	void writeTableHeader(TableHeader tableHeader)
	{
		mRawBytes[mTableHeaderOffset..mTableHeaderOffset+TableHeader.sizeof] = cast(ubyte[])(cast(void*)&tableHeader)[0..TableHeader.sizeof];
	}

	uint writeCell(uint freeSpaceOffset, ubyte[] data)
	{
		mRawBytes[freeSpaceOffset..freeSpaceOffset + data.length] = data;
		return cast(uint)data.length;
	}

	TableHeader readTableHeader()
	{
		return *(cast(TableHeader*)cast(void*)mRawBytes[mTableHeaderOffset..mTableHeaderOffset+TableHeader.sizeof]);
	}

	Cell readCell(int offset, int size)
	{

		Cell cell = Cell(mRawBytes[offset..offset+size]);
		return cell;
	}

	void dump(int bytesPerLine = 8)
	{
		assert (mPageNo);
		import std.range;
		int lineNo;
		writefln( "-------------------- Page %2d --------------------------", mPageNo);
		foreach(line; mRawBytes.chunks(bytesPerLine))
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

struct Cell
{
	ubyte[] data;

	void from (T)(T item)
	{
		data.reserve = 100;
		static if(is(T == struct))
		{
			foreach(idx, memberType; FieldTypeTuple!(T))
			{
				static if(is(memberType == struct) || is(memberType == class))
				{
				}
				static if(isArray!memberType)
				{
					uint length = item.tupleof[idx].length;
					data ~= (cast(ubyte*)(&length))[0..4];

					foreach (el ; item.tupleof[idx])
					{
						data  ~= cast(void*)el;
					}
				}
				else
				{
					data ~= (cast(ubyte*)(&item.tupleof[idx]))[0..memberType.sizeof];
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
		foreach(idx, memberType; FieldTypeTuple!(T))
		{
			static if(isArray!memberType)
			{
//				uint length = item.tupleof[idx].length;
//				data ~= (cast(ubyte*)(&length))[0..4];
//				
//				foreach (el ; item.tupleof[idx])
//				{
//					data  ~= cast(void*)el;
//				}
			}
			else
			{
				alias typeof(item.tupleof[idx]) targetType;
				item.tupleof[idx] = cast(targetType)data[0];
				data = data[targetType.sizeof..$];
			}

		}
		return item;
	}
}


struct DbFile
{
	ubyte[] mBuffer;
	uint mPageSize;
	ulong[] freePageIds;
	
	this(ref ubyte[] buffer, uint pageSize)
	{
		mPageSize = pageSize;
		mBuffer = buffer;
	}
	
	Page loadPage(ulong num)
	{
		Page page = Page(num, mPageSize);
		page.mRawBytes = mBuffer[cast(size_t)(num-1) * mPageSize .. cast(size_t)num * mPageSize].dup;
		return page;
	}

	void writePage(ulong pageNo, ubyte[] pageData)
	{
		mBuffer[cast(size_t)(pageNo-1)*mPageSize..cast(size_t)pageNo*mPageSize] = pageData;
	}

	void appendPage(Page page)
	{
	}

	uint freePageId()
	{
		// TODO
		return 0;
	}
}

struct DbStorage
{
	DbFile mDbFile;
	uint mPageSize;

	this(DbFile dbFile, uint pageSize)
	{
		mDbFile = dbFile;
		mPageSize = pageSize;
	}

	ulong createTable(ulong pageNo = PageNo.Null)
	{
		if (pageNo == PageNo.Null )
		{
			// allocate a new page if invalid page number provided
			// get new or unused page
			//pageNo = mDbFile.freePageId;
			pageNo = 2;
		}

		Page page = Page(pageNo,mPageSize);
		if(PageNo.Master)
		{
			page.writeDbHeader(DbHeader());
		}
		page.writeTableHeader(TableHeader());
		mDbFile.writePage(pageNo, page.mRawBytes);
		return pageNo;
	}

	void addItem(T)(ulong pageNo, T item)
	{
		Page page = mDbFile.loadPage(pageNo);
		TableHeader tableHeader = page.readTableHeader;

		Cell cell;
		cell.from(item);
		uint freeSpaceOffset = page.mTableHeaderOffset+TableHeader.sizeof + tableHeader.itemCount * T.sizeof;
		uint cellSize = page.writeCell(freeSpaceOffset, cell.data);
		tableHeader.itemCount++;
		page.writeTableHeader(tableHeader);
		mDbFile.writePage(pageNo, page.mRawBytes);
	}

	ulong getNextDbItemId(ulong pageNo, ulong id)
	{
		++id;
		Page page = mDbFile.loadPage(pageNo);
		TableHeader tableHeader = page.readTableHeader;
		return (id <= tableHeader.itemCount) ? id :  0;
	}
	
	T fetchDbItem(T)(ulong pageNo, ulong id)
	{
		Page page = mDbFile.loadPage(pageNo);
		uint offset = page.mTableHeaderOffset+TableHeader.sizeof + cast(uint)(id-1) * T.sizeof;
		return page.readCell(offset,T.sizeof).to!T();

	}

	void removeItem(T)(ulong pageNo, T item)
	{
		// TODO
	}

	void updateItem(T)(ulong pageNo, ulong id, T item)
	{
		//can be inplace
	}

	void dropTable(ulong pageNo)
	{
		
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

	writeln("unittest storage.d");

	Page page = Page(PageNo.Master,256);
	assert (page.mPageSize == 256);
	assert (page.mRawBytes.length == 256);

	DbHeader dbHeader;

	dbHeader.pageSize = page.mPageSize;
	dbHeader.changeCounter = 17;

	TableHeader tableHeader;
	tableHeader.itemCount = 0;

	page.writeDbHeader(dbHeader);
	page.writeTableHeader(tableHeader);
	page.dump(16);

	TestData testData= TestData(11,12,13);
	Cell cell;
	cell.from(testData);
	uint freeSpaceOffset = page.mTableHeaderOffset+TableHeader.sizeof + tableHeader.itemCount * TestData.sizeof;
	uint cellSize = page.writeCell(freeSpaceOffset, cast(ubyte[])cell.data);
	tableHeader.itemCount = 1;
	page.writeTableHeader(tableHeader);
	TestData testData2= TestData(111,112,113);
	Cell cell2;
	cell2.from(testData2);
	freeSpaceOffset = page.mTableHeaderOffset+TableHeader.sizeof + tableHeader.itemCount * TestData.sizeof;
	cellSize = page.writeCell(freeSpaceOffset, cast(ubyte[])cell2.data);
	page.dump(16);

	Page page2 = Page(PageNo.Master,256);
	page2.mRawBytes = page.mRawBytes;
	uint offset = page.mTableHeaderOffset+TableHeader.sizeof + tableHeader.itemCount * TestData.sizeof;
	TestData result = page2.readCell(offset,TestData.sizeof).to!TestData();
	writeln(result);

	assert (result == TestData(111,112,113));
	writeln("unittest storage.d passed");

}