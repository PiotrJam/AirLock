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

	static struct A
	{
		int a;
		short b;
	}

	static struct B
	{
		uint a;
		long b;
		float c;
	}

	static struct C
	{
		int a;
		string name;
	}


	static struct D
	{
		string city;
		string country;
	}

	DataBase db = DataBase(null,128);
	auto collA = db.createCollection!A("SmallIntegers");
	auto collB = db.createCollection!B("Numbers");
	auto collC = db.createCollection!C("Mixed");
	auto collD = db.createCollection!D("Strings");

	int[] smallIntegers = [40,41,42,43];
	short c =1;
	smallIntegers
		.map!(a => A(a,c++))
			.copy(collA);

	auto numbers = [B(1,-1,0.2), B(100_000_000,-1_000_000_000_000,8.71234), B(9_876,5_123_456_789_012,-0.2)];
	numbers.copy(collB);

	auto mixed = [C(1,"test"), C(2,"Hello World!"), C(-1,"Boom"), C(0,"P")];
	mixed.copy(collC);

	auto strings = [D("Poznan","Poland"), D("Budapest","Hungary"), D("Warsaw","Poland"), D("Phobos","Mars")];
	strings.copy(collD);

	auto data = db.collection!A("SmallIntegers");
	assert(data.array == [A(40,1),A(41,2), A(42,3), A(43,4)]);

	auto data2 = db.collection!B("Numbers");
	assert(data2.array == numbers);

	auto data3 = db.collection!C("Mixed");
	assert(data3.array == mixed);

	auto data4 = db.collection!D("Strings").filter!(s => (s.country == "Poland" || s.city.canFind("dap")));

	assert(data4.array == [D("Poznan","Poland"), D("Budapest","Hungary"), D("Warsaw","Poland")]);

	writeln("Unittest [main] 1 passed!");

}

unittest
{
	writeln("Test 2 start");
	static struct A
	{
		string a;
		int[] b;
	}

	DataBase db = DataBase(null,128);
	auto collectionA = db.createCollection!A("Scores");

	[A("James",[1,2,3]), A("Hero 2", [345,-94321,0,6621]), A("R2D2",[13,-987654321])].copy(collectionA);
	collectionA = db.collection!A("Scores");
	auto found = collectionA.filter!(a => a.a.canFind("2"));
	foreach(oldItem;found)
	{
		A newItem = {oldItem.a ~ " <updated>", [3]};

		collectionA.update(oldItem,newItem);
	}
	assert(collectionA.array == [A("James",[1,2,3]), A("Hero 2 <updated>", [3]), A("R2D2 <updated>",[3])]);

	writeln("Test 2 passed!");
}

unittest
{
	writeln("Test 3 start");
	static struct A
	{
		string a;
		int[] b;
		ubyte c;
	}
	
	DataBase db = DataBase(null,128);
	auto collectionA = db.createCollection!A("Many");
	
	for (int i = 0 ; i < 200; ++i)
	{
		import std.conv;
		auto a = A((i+1000).to!string(),[i+1,i+2,i+3,i+4], cast(ubyte)i);
		collectionA.put(a);

	}
	collectionA = db.collection!A("Many");

	collectionA.update(A("1002",[3,4,5,6], 2), A("Boooom",[3,4,5,6], 2));

	writeln("Test 3 passed!");
}

/*
unittest
{
	writeln("Test 4 start");
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
	writeln("Test 4 passed!");
}


*/