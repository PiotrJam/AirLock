/*
 Copyright: Copyright Piotr Półtorak 2015-2016.
 License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Authors: Piotr Półtorak
 */
module draft.database.collection;

import draft.database.storage;


struct Collection(T)
{

    this(DbStorage *dbStorage, uint tableRootPage)
    {
        mDbStorage = dbStorage;
        mTableRootPage = tableRootPage;

        if (tableRootPage != PageNo.Null)
        {
            mCurrentId = dbStorage.getNextDbItemId(mTableRootPage,mCurrentId);
        }
    }

    uint createColletion(uint tableRootPage)
    {
        if (mTableRootPage == PageNo.Null)
        {
            // no table, so a new empty one has to be created
            mTableRootPage = mDbStorage.createTable();
        }
        else if (mTableRootPage == PageNo.Master)
        {
            mTableRootPage = mDbStorage.initializeStorage();
        }

        return mTableRootPage;
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
        ulong itemId = 1;
        while (itemId != 0)
        {
            auto item = mDbStorage.fetchDbItem!T(mTableRootPage, itemId);
            if (item == oldItem)
            {
                mDbStorage.updateItem(mTableRootPage, itemId, newItem);
            }
            itemId = mDbStorage.getNextDbItemId(mTableRootPage, itemId);
        }
    }

    void removeItem(T item)
    {
        ulong itemId = 1;
        while (itemId != 0)
        {
            auto currItem = mDbStorage.fetchDbItem!T(mTableRootPage, itemId);
            if (currItem == item)
            {
                mDbStorage.removeItem(mTableRootPage, itemId);
            }
            itemId = mDbStorage.getNextDbItemId(mTableRootPage, itemId);
        }
    }

    T opIndex(int i)
    {
        return mDbStorage.fetchDbItem!T(mTableRootPage, i+1);
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

    // TODO separate collectionId and rootPageId
    uint collectionId()
    {
        return mTableRootPage;
    }

private:
    uint mTableRootPage = PageNo.Null;
    DbStorage * mDbStorage;
    long mCurrentId = 0;

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