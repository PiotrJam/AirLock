﻿/*
Copyright: Copyright Piotr Półtorak 2015-.
License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
Authors: Piotr Półtorak
*/
module draft.database.table;

import draft.database.storage;


struct Collection(T)
{
	uint mTableRootPage = PageNo.Null;
	DbStorage * mDbStorage;
	long mCurrentId = 0;


	this(DbStorage *dbStorage, uint tableRootPage)
	{
		mDbStorage = dbStorage;
		mTableRootPage = tableRootPage;
		if (mTableRootPage != PageNo.Null)
		{
			mCurrentId = dbStorage.getNextDbItemId(mTableRootPage,mCurrentId);
		}
		else
		{
			// no table, so a new empty one has to be created
			mTableRootPage = mDbStorage.createTable(mTableRootPage);
		}
	}
	bool empty()
	{
		return (mCurrentId == 0);
	}
	
	void popFront()
	{
		mCurrentId = mDbStorage.getNextDbItemId(mTableRootPage, mCurrentId);
	}
	
	T front()
	{
		return mDbStorage.fetchDbItem!T(mTableRootPage, mCurrentId);
	}

	void put(T item)
	{
		mDbStorage.addItem!T(mTableRootPage, item);
	}

	void update(T oldItem, T newItem)
	{
		ulong id = 0;//mDbStorage.getNextDbItemId(0);
		while (id != 0)
		{
			//auto item = mDbStorage.fetchDbItem!T(id);
			//if (item == oldItem)
			{
				//mDbStorage.updateItem(id,newItem);
			}
			//id = mDbStorage.getNextDbItemId(id);
		}
	}


	void remove(T item)
	{
		mDbStorage.dropTable(mTableRootPage);
	}


	void setKey(alias key)()
	{
		/*
		foreach(idx, memberType; FieldTypeTuple!(T))
		{
			static if (T.tupleof[idx].stringof == key)
			{
			}
		}*/
	}

}

version (none)
{
struct Tool
{

	static string[] memberTypes(T)()
	{
		import std.traits;

		string[] types;
		static if(is(T == struct))
		{
			foreach(idx, memberType; FieldTypeTuple!(T))
			{
				types ~= memberType.stringof;
			}
		}
		return types;
	}
}


	alias Member = Tuple!(string,DbMember);
	alias DbMember = Algebraic!(int, long, string, DbReference);
}