/*
Copyright: Copyright Piotr Półtorak 2015-2016.
License: $(WEB boost.org/LICENSE_1_0.txt, Boost License 1.0).
Authors: Piotr Półtorak
*/
module main;

import draft.database;

import std.algorithm;
import std.array;
import std.stdio;

void main(string[] args)
{

}

unittest
{
	writeln("Unittest [main] 1 start");

	ushort pageSize = 128;

	static struct A
	{
		int a;
		short b;
	}

	ubyte[] storageFileBytes;
	storageFileBytes.length = 10 * pageSize;
	DbFile dbFile = DbFile(storageFileBytes, pageSize);
	DbStorage dbStorage = DbStorage(&dbFile, pageSize);
	DataBase db = DataBase(&dbStorage);
	db.createStorage();
	auto numDbColl = db.createCollection!A("B.numbers");

	int[] numbers = [40,41,42,43];
	short c =1;
	numbers
		.map!(a => A(a,c++))
			.copy(numDbColl);
	auto data = db.collection!A("B.numbers");

	assert(data.array == [A(40,1),A(41,2), A(42,3), A(43,4)]);
	writeln("Unittest [main] 1 passed!");

}
/*
unittest
{
	writeln("Test 2 start");
	static struct A
	{
		long a;
		string b;
	}

	DbFile dbFile = DbFile(rawDbFile);
	DbStorage dbStorage = DbStorage(dbFile);
	DataBase db = DataBase(&dbStorage);
	auto songDbColl = db.collection!A("B.songs");

	if (songDbColl.empty)
	{

	}
	writeln("************** Reading   *****************");
	songDbColl = db.collection!A("B.songs");
    songDbColl.setKey!"a";

	//writeln (songColl);
	auto found = songDbColl.filter!(a => a.b.canFind("ma"));
	writeln (found);

	writeln("************** Update   *****************");
	foreach(oldItem;found)
	{
		A newItem = {10,oldItem.b ~ " <updated>"};
		writeln(newItem);
		songDbColl.update(oldItem,newItem);
	}
	writeln("Test 2 passed!");
}

unittest
{
	writeln("Test 3 start");
	import std.datetime;

	static struct Author
	{
		string name;
	}
	static struct Post
	{
		string text;
		Author author;
		DateTime date;
	}
	
	static struct ForumThread
	{
		Author author;
		string title;
		Post[] posts;
	public: 
		string foo(){return "bar";};
	}

	ubyte rawDbFile[];
	DbFile dbFile = DbFile(rawDbFile);
	DbStorage dbStorage = DbStorage(dbFile);
	DataBase db = DataBase(&dbStorage);
	auto forum = db.collection!Post("C.posts");
	forum.put(Post());
	writeln("Test 3 passed!");
}

*/