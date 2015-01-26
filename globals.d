module globals;
//globals, helper functions, and sort data

import core.sys.posix.sys.stat;  //dev_t ino_t time_t
import std.bitmanip;
import std.process : ProcessPipes;
import std.stdio;
import std.typecons;
import std.string : format;
import std.digest.digest : toHexString;
import std.array : appender;
import std.conv;
import std.regex;
import std.path : baseName;
import core.sys.posix.signal;
import std.algorithm : isSorted;

int ctrlc;

extern(C) void siginthandle(int sig){
	ctrlc=1;
	sigset(SIGINT, SIG_DFL);
}


struct dbentry {
	string	path;
	time_t	mtime;
	dev_t	device;     /* ID of device containing file */
	ino_t	inode;     /* inode number */
	ulong	size;
	ubyte[16] md5;
	ubyte[16] mid;
	mixin(bitfields!(
        	bool, "dir1", 1,
        	bool, "flag", 1,
        	bool, "md5filled", 1,
        	ubyte, "midfilled", 2,	//0 - not filled, 1 - filled, altmid { 2 - 0x00 filled, 3 - 0xff filled }
		ubyte, "machine", 3));
}

alias int dbIndex;


__gshared dbentry[dbIndex] dball;
dbIndex globalindex = 0;
ulong numafiles;

struct hostdbentry {
	string hostname;
	ProcessPipes conn;
	int index;
}

hostdbentry[] machinedb;
ubyte[string] machlu;


struct GlobalOptions {
	bool noprogressbar;
	bool quiet;
	bool[string] stdouttypes;
	bool noresults;
	bool cache;
	string resultsdir;
	string fileprefix;
	string delim;
	bool skipmd5;
	uint midsize;
}
GlobalOptions options;

File filelog;

enum probables { name, time, nameandtime, none };
probables probs = probables.nameandtime;



//duptype => index => extrakey
//"unique" => index => -1
//"duplicate" => index => dupkey
dbIndex[dbIndex][string] results;
dbIndex[dbIndex][string] extraresults;
dbIndex[dbIndex][string] testresults;
dbIndex[dbIndex][string] testextraresults;



//alias Hardlink = Tuple!(dev_t, ino_t);
alias MHardlink = Tuple!(ubyte, dev_t, ino_t);	//machine,dev,inode
unittest {
	alias TupleKey = Tuple!(int, string);
	auto key1  = TupleKey(123, "abc");
	auto key1b = TupleKey(123, "abc");
	auto key2  = TupleKey(123, "def");
	auto key3  = TupleKey(234, "abc");
	auto key4  = TupleKey(234, "def");
 
	assert(key1 == key1b);
	assert(typeid(key1).getHash(&key1) == typeid(key1b).getHash(&key1b));
	assert(typeid(key1).compare(&key1, &key1b) == 0);

	assert(key1 < key2);
	assert(!(key2 < key1));
	assert(key1 < key3);
	assert(key3 < key4);

	int[TupleKey] aa;
	aa[key1] = 1000;
	assert(aa[key1b] == 1000);

	// changing aa[key2] should not affect aa[key1]
	aa[key2] = 2000;
	assert(aa[key1] == 1000);

	// aa[key2] should be recoverable
	assert(aa[key2] == 2000);

	// creating new instance of MyKey with same value as key2 should work
	// as expected
	assert(aa[TupleKey(123, "def")] == 2000);

	// ... insert other tests here
}

dbentry[MHardlink] md5cachedb;

dbIndex[][MHardlink] harddb; //mach-dev-inode => [key1, keyn...]
int countlinkuniq;
int countlinkdup;




void log(S...)(S args) {
	if (!options.quiet)
		writeln(args);
	if (!options.noresults)
		filelog.writeln(args);
}

void logf(S...)(S args) {
	log(format(args));
}



void hextoubyte(string str, ref ubyte[16] target) {
	for(int i=0; i<32; i+=2)
		target[i/2] = to!ubyte(str[i..i+2],16);
}



string writeobj(M...)(dbentry e) {
	string s;
	foreach (int i,string m; M) {
		s ~= writeobj!(m)(e);
		if (i < M.length-1) s ~= ", ";
	}
	return s;
}

string writeobj(string member)(dbentry e) {
	string s;
	s = `"` ~ member ~ `": `;

	static if ( is(typeof(__traits(getMember, e, member)) == string) )
		s ~= `"` ~ jsonescape(__traits(getMember, e, member)) ~ `"`;
	else static if ( is(typeof(__traits(getMember, e, member)) == ubyte[16]) ) {
		s ~= `"` ~ toHexString(__traits(getMember, e, member)) ~ `"`;
		/*
		writeln("\nbegin");
		writeln("orig: ", __traits(getMember, e, member));
		string test = toHexString(__traits(getMember, e, member));
		writeln("hex: ", test);
		ubyte[16] a,b;
		for(int i=0; i<32; i+=2)
			a[i/2] = to!ubyte(test[i..i+2],16);
		writeln("back: ", a);
		hextoubyte(test,b);
		writeln("func: ", b);
		*/
	}
	else
		s ~= to!string(__traits(getMember, e, member));

	return s;
}


/** Handle illegal json chars (not actually illegal, but most json's can't handle it?)
	Replace all control characters with #XX where XX is it's hex representation
	Needed for linux filenames which can contain just about anything.
	ext mode for linux-byte <-> json utf8 if filename not in utf8 space
*/
string ccescape(string s, bool ext=false) {
	auto app = appender!string();
	foreach(char c; s) {
		if (c < 32 || (ext && c > 127))
			app.put( format("#x%02X",c) );
		else if (c == '#')
			app.put("##");
		else
			app.put(c);
	}
	return app.data;
}
unittest {
	assert(ccescape("hello")==`hello`);
	assert(ccescape("\x05hello")==`#x05hello`);
	assert(ccescape("hello\x05")==`hello#x05`);
	assert(ccescape("\x00")==`#x00`);
	assert(ccescape("\x19")==`#x19`);
	assert(ccescape("\x20")==" ");
	assert(ccescape(`\x19`)==`\x19`);
	assert(ccescape(`#x19`)==`##x19`);
	assert(ccescape("\xFF",false)=="\xFF");
	assert(ccescape("\xFF",true)=="#xFF");
}

string ccunescape(string s) {
	auto app = appender!string();
	
	char state = 0;
	for(int i=0; i<s.length; i++) {
		char c = s[i];
		if (state=='#') {
			if (c == '#')
				app.put('#');
			else if (c == 'x') {
				app.put( to!ubyte(s[i+1..i+3], 16) );
				i+=2;
			}
			else //throw new Exception("Original string wasn't escaped");
				app.put("#" ~ c);
			
			state=0;
			continue;
		}
		
		//state == 0
		if (c == '#') {
			state = '#';
			continue;
		}
		else app.put(c);
	}
	
	if(state=='#') app.put('#');
	return app.data;
}
unittest {
	assert(ccunescape("hello")=="hello");
	assert(ccunescape(`#x05hello`)=="\x05hello");
	assert(ccunescape(`hello#x05`)=="hello\x05");
	assert(ccunescape(`#x00`)=="\x00");
	assert(ccunescape(`#x19`)=="\x19");

	assert(ccunescape(`\x19`)==`\x19`);
	assert(ccunescape(`##x19`)==`#x19`);

	assert(ccunescape(`##`)==`#`);
	assert(ccunescape(`####`)==`##`);
	assert(ccunescape(`####z`)==`##z`);
}

string jsonescape(string s) {
	try {
		std.utf.validate(s);
		s = ccescape(s,false);
	} catch (std.utf.UTFException e) {
		s = ccescape(s,true);
	}

	static auto esc = ctRegex!(`(?=[\\"])`,"g");
	return replaceAll(s, esc, `\`);
}
unittest {
	assert(jsonescape("\xFF")=="#xFF");
}

string tohostpath(dbIndex k) {
	auto m = dball[k].machine;
	if(m>0) return machinedb[m].hostname ~ ":" ~ dball[k].path;
	else return dball[k].path;
}



struct comparepair {
	bool function(dbIndex x, dbIndex y) less;
	bool function(dbIndex x, dbIndex y) equal;
	string desc;

	//int opCmp(ref const S s) const { }
}


bool direqual(dbIndex x, dbIndex y) {return dball[x].dir1 == dball[y].dir1; }

bool timeequal(dbIndex x, dbIndex y) { return dball[x].mtime == dball[y].mtime; }
bool nameequal(dbIndex x, dbIndex y) { return baseName(dball[x].path) == baseName(dball[y].path); }
bool timenameequal(dbIndex x, dbIndex y) { return dball[x].mtime == dball[y].mtime ? baseName(dball[x].path) == baseName(dball[y].path) : false; }

bool midequal(dbIndex x, dbIndex y) { return dball[x].size == dball[y].size ? toHexString(dball[x].mid) == toHexString(dball[y].mid) : false; }
bool md5equal(dbIndex x, dbIndex y) { return toHexString(dball[x].md5) == toHexString(dball[y].md5); }
bool sizeequal(dbIndex x, dbIndex y) { return dball[x].size == dball[y].size; }
bool machineequal(dbIndex x, dbIndex y) { return dball[x].machine == dball[y].machine; }
bool hardequal(dbIndex x, dbIndex y) 
	{ return dball[x].device == dball[y].device ? (dball[x].inode == dball[y].inode) : false; }

bool sortondir(dbIndex x, dbIndex y) { return dball[x].dir1 > dball[y].dir1; }

bool sortontime(dbIndex x, dbIndex y) { return dball[x].mtime < dball[y].mtime; }
bool sortonname(dbIndex x, dbIndex y) { return baseName(dball[x].path) < baseName(dball[y].path); }
bool sortontimename(dbIndex x, dbIndex y)
{
	if (dball[x].mtime < dball[y].mtime) return true;
	if (dball[x].mtime == dball[y].mtime)
		return baseName(dball[x].path) == baseName(dball[y].path);
	return false;
}
bool sortonmid(dbIndex x, dbIndex y) 
{
	if (dball[x].size < dball[y].size) return true;
	if (dball[x].size == dball[y].size)
		return toHexString(dball[x].mid) < toHexString(dball[y].mid);
	return false;
}
bool sortonmd5(dbIndex x, dbIndex y) { return toHexString(dball[x].md5) < toHexString(dball[y].md5); }
bool sortonsize(dbIndex x, dbIndex y) { return dball[x].size < dball[y].size; }
bool sortonmachine(dbIndex x, dbIndex y) { return dball[x].machine < dball[y].machine; }
bool sortonhardlink(dbIndex x, dbIndex y)
{
	if (dball[x].device < dball[y].device) return true;
	if (dball[x].device == dball[y].device) {
		return dball[x].inode < dball[y].inode;
	}
	return false;
}

comparepair compdir = { less:&sortondir, equal:&direqual };

comparepair comptime = { less:&sortontime, equal:&timeequal, desc:"time" };
comparepair compname = { less:&sortonname, equal:&nameequal, desc:"name" };
comparepair comptimename = { less:&sortontimename, equal:&timenameequal, desc:"name-time" };

comparepair compmid = { less:&sortonmid, equal:&midequal };
comparepair compmd5 = { less:&sortonmd5, equal:&md5equal };
comparepair compsize = { less:&sortonsize, equal:&sizeequal };
comparepair compmachine = { less:&sortonmachine, equal:&machineequal };
comparepair comphardlink = { less:&sortonhardlink, equal:&hardequal };


bool testequalvialess(comparepair comp, dbIndex x, dbIndex y) {
	bool testge,testle;
	testge = !comp.less(x,y);
	testle = !comp.less(y,x);
	//testeq = comp.equal(x,y);
	return testge && testle;
}

class SetIter {
	this(ref dbIndex[] keys, comparepair[] comp) {
		sortby = new SortBy();
		sortby.listcomp = comp;
		this.sortedkeys = keys;
		
		bool delegate(dbIndex x, dbIndex y) dg = &sortby.less;
		assert(isSorted!(dg)(keys));

		popFront();
	}

	bool empty() {
		return start >= sortedkeys.length;
	}

	dbIndex[] front() {
		return sortedkeys[start..end];
	}

	void popFront() {
		start=end;
		int pos=start;

		if ( (pos+1) >= sortedkeys.length ) {
			end=pos+1;
			return;
		}
			
		while(sortby.equal(sortedkeys[pos], sortedkeys[pos+1])) {
			//assert( sortby.equalbyless(sortedkeys[pos], sortedkeys[pos+1]) );
			++pos;
			if ( (pos+1) >= sortedkeys.length) break;
		}
		end = pos+1;
	}
	

private:
	dbIndex[] sortedkeys;
	int start,end;
	SortBy sortby;
}

class SortBy {
	this() {}

	this(comparepair[] listcomp) {
		this.listcomp = listcomp;
	}

	bool less(dbIndex x, dbIndex y) {
		for(int i=0; i<listcomp.length; ++i) {
			auto fl = listcomp[i].less;
			//auto fe = listcomp[i].equal;
			if (fl(x,y)) return true;
			if (fl(y,x)) return false;
			//if (fe(x,y)) continue;
		}
		return false;
	}
	bool equal(dbIndex x, dbIndex y) {
		for(int i=0; i<listcomp.length; ++i) {
			auto fe = listcomp[i].equal;
			if (!fe(x,y)) return false;
		}
		return true;
	}

	bool equalbyless(dbIndex x, dbIndex y) {
		for(int i=0; i<listcomp.length; ++i) {
			auto fl = listcomp[i].less;
			if ( (!fl(x,y) && !fl(y,x)) == false ) return false;
		}
		return true;
	}
/*
	int cmp(dbIndex x, dbIndex y) {
		if (this.less(x,y)) return -1;
		else if (this.equal(x,y)) return 0;
		else return 1;
	}

	int opCall(dbIndex x, dbIndex y) {
		return less(x,y);
	}
*/
	comparepair[] listcomp;
}

dbIndex[][bool] splitdirs(dbIndex[] keys) {
	dbIndex[][bool] dirsplit;
	dirsplit[true] = null;
	dirsplit[false] = null;
	foreach(k;keys) 
		dirsplit[dball[k].dir1] ~= k;
	
	return dirsplit;
}
