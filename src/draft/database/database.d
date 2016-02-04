/*
Copyright: Copyright Piotr Półtorak 2015-2016.
License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
Authors: Piotr Półtorak
*/

module draft.database.database;

import draft.database.table;
import draft.database.storage;
import std.algorithm;
import std.array;
import std.stdio;

struct DataBase
{

	DbStorage * mStorage;

	this(DbStorage * dbStorage)
	{
		mStorage = dbStorage;
	}

	void createStorage()
	{	
		//create Master Table
		auto masterTable = Collection!TableInfo(mStorage, PageNo.Master);
		auto rootPageId =  masterTable.create();

		//insert master table info Item to table
		masterTable.put(TableInfo("_Internal.MasterTable",rootPageId));
	}

	Collection!T createCollection(T)(string name)
	{
		auto masterTable = Collection!TableInfo(mStorage, PageNo.Master);
		// check if exists
		//create new table
		auto newTable = Collection!T(mStorage, PageNo.Null);
		auto rootPageId = newTable.create();
		masterTable.put(TableInfo(name,rootPageId));
		return newTable;
	}

	void dropCollection(T)(string name)
	{
		auto tables = Collection!TableInfo(mStorage, PageNo.Master);
		mStorage.dropTable(tables.front.pageId);
	}

	Collection!T collection(T) (string name)
	{
		ulong pageNo = PageNo.Master;
		if(name != "_Internal.MasterTable")
		{
			pageNo = 2;
		}

		auto masterTable = Collection!TableInfo(mStorage, PageNo.Master);
		// check if exists
		return Collection!(T)(mStorage, pageNo);
	}


	const(string[]) getCollections() 
	{
		import std.array;
		auto masterTable = Collection!TableInfo(mStorage, PageNo.Master);
		return masterTable.map!(item => item.name).array;
	}

}
