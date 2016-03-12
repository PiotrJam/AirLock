/*
 Copyright: Copyright Piotr Półtorak 2015-2016.
 License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
 Authors: Piotr Półtorak
 */

module draft.database.database;

import draft.database.collection;
import draft.database.storage;
import std.algorithm;
import std.array;




struct DataBase
{

    DbStorage * mStorage;

    this(string path, DbParams params)
    {
        mStorage = new DbStorage(path, params.pageSize);
        createStorage();
    }


    this(string path)
    {
        mStorage = new DbStorage(path);
    }

    void createStorage()
    {	
        //create Master Table (root page only)
        auto masterTable = Collection!TableInfo(mStorage, PageNo.Null);
        masterTable.createColletion(PageNo.Master);
        auto rootPageId =  masterTable.mTableRootPage;
        assert (rootPageId == PageNo.Master);

        //insert master table info Item to table
        masterTable.put(TableInfo("_Internal.MasterTable",rootPageId));
    }

    Collection!T createCollection(T)(string name)
    {
        auto masterTable = Collection!TableInfo(mStorage, PageNo.Master);
        // check if exists
        //create new table
        auto newTable = Collection!T(mStorage, PageNo.Null);
        auto rootPageId = newTable.createColletion(PageNo.Null);

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
            assert(pageNo != PageNo.Master);
        }

        // TODO check if exists
        return Collection!(T)(mStorage, pageNo);
    }

    
    const(string[]) getCollections() 
    {
        import std.array;
        auto masterTable = Collection!TableInfo(mStorage, PageNo.Master);
        return masterTable.map!(item => item.name).array;
    }

}
