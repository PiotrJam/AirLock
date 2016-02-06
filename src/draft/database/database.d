﻿/*
Copyright: Copyright Piotr Półtorak 2015-.
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

	// This constructor is probably useful only for testing purpose
	this(DbStorage * dbStorage, uint pageSize =128)
	{
		if (!dbStorage)
		{
			mStorage = new DbStorage(new DbFile(null, pageSize));
		}
		createStorage();
	}

	// TODO
	this(string path)
	{

	}

	void createStorage()
	{	
		//create Master Table
		auto masterTable = Collection!TableInfo(mStorage, PageNo.Null);
		auto rootPageId =  masterTable.mTableRootPage;
		assert (rootPageId == 1);
		//insert master table info Item to table
		masterTable.put(TableInfo("_Internal.MasterTable",rootPageId));
	}

	Collection!T createCollection(T)(string name)
	{
		auto masterTable = Collection!TableInfo(mStorage, PageNo.Master);
		// check if exists
		//create new table
		auto newTable = Collection!T(mStorage, PageNo.Null);
		auto rootPageId = newTable.mTableRootPage;
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
		auto masterTable = Collection!TableInfo(mStorage, PageNo.Master);

		uint pageNo = PageNo.Null;
		if(name == "_Internal.MasterTable")
		{
			pageNo = PageNo.Master;
		}
		else
		{
			auto found = masterTable.filter!(a => a.name == name);
			assert(!found.empty, "Collection not found");
			pageNo = found.front.pageNo;
			assert (pageNo != PageNo.Master);
		}

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