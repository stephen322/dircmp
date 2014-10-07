/*  TODO
-output parsable text for GUIs
-diff device concurrency
-output multiple duplicate matches
-treat symlinks as files
*/

import std.stdio;
import std.file;
import core.sys.posix.sys.stat;  // for stat_t: dev_t ino_t time_t
import std.datetime;
import std.algorithm;
import std.array;
import std.digest.md;
import std.path;	//baseName
import std.string;
import core.sys.posix.signal;
import std.getopt;
import std.c.stdlib;	//exit
import std.bitmanip;
import std.parallelism;
import std.conv;
import std.typecons;
import std.json;
import std.regex;
import std.process;



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
ulong leftfiles, rightfiles;
ulong totalfiles;

struct hostdbentry {
	string hostname;
	ProcessPipes conn;
}

hostdbentry[] machinedb;


dbIndex[][MHardlink] harddb; //mach-dev-inode => [key1, keyn...]
int countlinkuniq;
int countlinkdup;

//duptype => index => extrakey
//"unique" => index => -1
//"duplicate" => index => dupkey
dbIndex[dbIndex][string] results;
dbIndex[dbIndex][string] extraresults;
dbIndex[dbIndex][string] testresults;
dbIndex[dbIndex][string] testextraresults;

//struct GlobalOptions {
	bool noprogressbar;
	bool quiet;
	bool[string] stdouttypes;
	bool noresults;
	string fileprefix;
	File filelog;
	string resultsdir, delim;
//}
//GlobalOptions options;

enum probables { name, time, nameandtime, none };
probables probs = probables.nameandtime;

bool skipmd5 = false;



int ctrlc;

extern(C) void siginthandle(int sig){
	ctrlc=1;
	sigset(SIGINT, SIG_DFL);
}




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

//alias Hardlink = Tuple!(dev_t, ino_t);
alias MHardlink = Tuple!(ubyte, dev_t, ino_t);	//machine,dev,inode



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

void markuniques(ref dbIndex[] k, comparepair[] comp) {
	SetIter sets = new SetIter(k, comp);
	foreach(s; sets) {
		if (s.length == 1) { //uniques (1,0),(0,1)
			dball[s[0]].flag = true;
			continue;
		}

		auto spl = splitdirs(s);
		if (spl[false].length == 0) { //(2,0)
			assert(spl[true].length >= 2);
			foreach(i; spl[true])
				dball[i].flag = true;
			continue;
		}
		else if (spl[true].length == 0) { //(0,2)
			assert(spl[false].length >= 2);
			foreach(i; spl[false])
				dball[i].flag = true;
			continue;
		}
	}
}

int[2] removeflagged(ref dbIndex[] k, bool clearentry=false) {
	int count[2] = [0,0];
	int i=cast(int)k.length-1;
	while (i>=0) {
		if (dball[k[i]].flag) {
			dball[k[i]].flag = false;
			++count[!dball[k[i]].dir1];
			//if (clearentry) dball.remove(k[i]); //need keys for output stats
			swap(k[i], k.back);
			k.popBack();
		}
		--i;
	}

	//if (clearentry) dball.rehash;
	return count;
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


void log(S...)(S args) {
	if (!quiet)
		writeln(args);
	if (!noresults)
		filelog.writeln(args);
}

void logf(S...)(S args) {
	log(format(args));
}

void output(string duptype, string method, dbIndex key, dbIndex key2=-1) {
	string outstring = format("%s (%s): %s%s", duptype, method, tohostpath(key), key2==-1 ? "" : delim ~ tohostpath(key2));
	results[duptype][key] = key2;


	if (stdouttypes.get(duptype,false) && !quiet)
		writeln(outstring);

	if (!noresults)
		filelog.writeln(outstring);

	if(duptype != "probable-duplicate" && duptype != "probable-unique") {
		if (duptype == "unique" && key in results["probable-duplicate"])
			extraresults["checkcorrupt"][key] = results["probable-duplicate"][key];

		results["probable-duplicate"].remove(key);
		results["probable-unique"].remove(key);
	}
}

int outputallunique(string method, dbIndex[] keys, bool delegate(dbIndex i) testfunc = (dbIndex i) => dball[i].flag && dball[i].dir1) {
	int count;
	foreach(dbIndex k; keys)
		if (testfunc(k)) {
			output("unique",method, k);
			++count;
		}

	return count;
}

void outputstats(ulong afiles, string dir=null, string delim="\t") {
	int processedcount = 0;
	log();
	log(afiles, " files scanned in directories of interest at start.");

	bool[string] hidezero = ["probable-unique":true, "probable-duplicate":true, "zero":true, "duplicate-hardlink":true];
	int[string] statsorder;
	auto duptypes = results.keys;
	foreach(i; duptypes) statsorder[i] = 0;
	statsorder["unique"] = 100;
	statsorder["probable-unique"] = 90;
	statsorder["duplicate"] = 80;
	statsorder["duplicate-hardlink"] = 70;
	statsorder["probable-duplicate"] = 60;
	statsorder["zero"] = 50;
	sort!((a,b) => statsorder[a] > statsorder[b])(duptypes);
	foreach(typ; duptypes) {
		processedcount+=results[typ].length;
		if ((results[typ].length == 0) && (typ in hidezero && hidezero[typ]))
			continue;
		log(format("\t%s: %d files", typ, results[typ].length));

		//file output
		//TODO: user customize sort
		if (!noresults && dir!=null && results[typ].length) {
			auto keys = results[typ].keys;
			sort!((a,b)=> dball[a].path < dball[b].path)(keys);
			
			auto f = File(dir ~ fileprefix ~ typ ~ ".txt", "w");
			foreach(k; keys) {
				auto v=results[typ][k];
				if (v == -1)
					f.writeln(tohostpath(k));
				else
					f.writeln(tohostpath(k), delim, tohostpath(v));
			}
		}
	}

	if (!noresults && dir!=null && extraresults["checkcorrupt"].length) {
		auto f = File(dir ~ fileprefix ~ "checkcorrupt.txt", "w");
		foreach(k,v; extraresults["checkcorrupt"])
			f.writeln(tohostpath(k), delim, tohostpath(v));
	}

	log(processedcount, " - Total processed.");
	assert(afiles==processedcount);
}



class SpeedRate {
	this() {
		sw.start();
		lastx = 0;
	}

	struct sample {
		double x = 0;
		uint msecs = 0;
		@property double xpersec() {
			return x * 1000 / (msecs==0? 1:msecs);
		}
	}

	static immutable uint nsamples = 21;
	sample[nsamples] samples;
	sample total;

	StopWatch sw;
	uint pos;
	double lastx = 0;
	
	void addcumusample(double x) {
		auto diffx = x - lastx;

		++pos;
		pos %= nsamples;

		auto tmpx = samples[pos].x;
		auto tmpmsecs = samples[pos].msecs;

		uint msecs = cast(uint)sw.peek().msecs;

		samples[pos].x = diffx;
		samples[pos].msecs = msecs;
		
		total.x += diffx - tmpx;
		total.msecs += msecs - tmpmsecs;
		//if (getxpersec() < 1) writefln("%.2f %.2f", total.x, total.msecs);
		lastx = x;
		restart();
	}
	
	double getavg() {	return total.xpersec; }
	double get1xpersec() {	return samples[pos].xpersec; }
	double getmedian() {
		auto c = samples.dup;
		sort!("a.xpersec < b.xpersec")(c);
		//ignore empty
		int i=0;
		for (i=0; i<nsamples && c[i].msecs==0 ; i++) {}
		i%=nsamples;
		auto a = c[i..$];
		//writefln("%d %s  ", i, c[i..$]);
		return a[a.length/2].xpersec;
	}
	double getharmavg() {
		int count=0; double h=0;
		foreach(s; samples) {
			if(s.msecs==0) continue;
			++count;
			h += 1/s.xpersec;
			//writefln("%d %s %.2f  ", count, s.xpersec, h);
		}
		return (cast(double)count)/h;
	}


	long peekmsecs() {	return sw.peek().msecs;	}

	void restart() {
		sw.stop();
		sw.reset();
		sw.start();
	}
}
__gshared SpeedRate speedrate;


void progress_console(double cur, double total, string units, string text="") {
	static ulong lastline;
	if (noprogressbar)
		return;

	synchronized {
	if (speedrate.peekmsecs() >= 1000)
		speedrate.addcumusample(cur);
	}

	auto rate = speedrate.getmedian();
	auto timeremain = cast(uint)((total-cur) / rate);
	auto sec = timeremain % 60;
	auto min = (timeremain/60) % 60;
	auto hr = (timeremain/3600);
	
	double percent = cur*100/total;
	string output=format("  %.0f%% %.1f %s/sec %d:%02d:%02d %.0f/%.0f %s %s", percent, rate, units, hr,min,sec, cur, total, units, text);
	string line;
	if (lastline > output.length)
		line = output ~ format("%*s\r", lastline - output.length, " ");
	else
		line = output ~ "\r";
	write(line);

	lastline=output.length;
	stdout.flush();
}
auto renderprogress = &progress_console;

void progress_json(double cur, double total, string units, string text="") {
	if (noprogressbar)
		return;

	if (speedrate.peekmsecs() < 500 && cur!=total) return;

	speedrate.restart();
	
	writefln(`{ "progress":%.2f }`, cur);
	stdout.flush();
}




void runserver() {
	quiet = true;
	noresults = true;
	//noprogressbar = true;
	renderprogress = &progress_json;
	try {
		double ssum;
		string line;
		while ((line = stdin.readln()) !is null) {
			auto j = parseJSON(line);

			//writeln(line);

			auto func = j["func"].str;
			switch (func) {
			case "dirscan":
				auto dir = j["dir"].str;
				auto indexstart = j["indexstart"].integer;
				bool recurse = (j["recurse"].type == JSON_TYPE.TRUE) ? true : false;
	
				//writeln("data: ", indexstart, " ", dir, " ", recurse);
				globalindex = cast(int)indexstart;
				dirscan(false, dir, recurse);
				auto endindex = globalindex;
	
				for(dbIndex i=cast(int)indexstart; i < cast(int)endindex; i++) {
					with (dball[i]) {
					string tmp = format(`{ "%s": %d, "%s": "%s", "%s": %d, "%s": %d, "%s": %d, "%s": %d }`,
	"index", i, "path", jsonescape(path), "mtime", mtime, "device", device, "inode", inode, "size", size) ~ "\n";
					write(tmp);
					}
				}
	
				writeln(`{ "result": 0 }`);
				stdout.flush();

				//build hardlink db
				auto keys = dball.keys;
				sort!(sortonhardlink)(keys);
				SetIter hlsets = new SetIter(keys, [compmachine, comphardlink]);
				foreach(s; hlsets) {
					auto hlkey = MHardlink(cast(ubyte)0, dball[s[0]].device, dball[s[0]].inode);
					if (s.length > 1) 
						harddb[hlkey] = s.dup;
				}
				break;
			case "midscan":
				auto progbarbak = noprogressbar;
				noprogressbar = true;
				auto appkeys = appender!(dbIndex[])();
				foreach(jv; j["keys"].array)
					appkeys.put(cast(dbIndex)jv.integer);
				int ms = cast(int) j["midsize"].integer;
				
				void writemidjson(dbIndex k) {
					writefln(`{ "key":%d, "midmd5":"%s", "midfilled":%d%s}`, k, toHexString(dball[k].mid), dball[k].midfilled, dball[k].md5filled?`, "md5filled":1 `:" ");
				}
				
				assert(isSorted!(sortonhardlink)(appkeys.data));
				midscanall(appkeys.data, ms, &writemidjson);
				writeln(`{ "result": 0 }`);
				stdout.flush();
				noprogressbar = progbarbak;
				break;
			case "md5file":
				dbIndex k = cast(dbIndex) j["key"].integer;
				ssum=0;
				md5file(k, ssum, dball[k].size);
				writefln(`{ "key":%d, "md5sum":"%s" }`, k, toHexString(dball[k].md5));
				writeln(`{ "result": 0 }`);
				stdout.flush();
				break;
			case "exit":
			default:
				exit(0);
				break;
			}
		}
	} catch(Exception e) {
		writefln(`{ "result": -1, "msg":"%s" }`, jsonescape(e.msg));
		exit(0);
	}

	exit(0);
}


void runtest(string path) {
	readdump(true, path, 0, true);
	dball.rehash;
}


//TODO: permissions exception
//TODO: warn/exit on directories in both left and right
void dirscan(bool doi, string dir, bool recursive) {
	ulong numfiles=0;
	ulong totalsize=0;
	foreach (DirEntry e; dirEntries(dir, recursive?SpanMode.breadth:SpanMode.shallow, false)) {
		auto a = e.linkAttributes();
		//writeln(e.name(), attrIsFile(a) ?" file":" notfile");
		if (!attrIsSymlink(a) && attrIsFile(a)) {
			auto tmp = e.statBuf();
			dball[globalindex] = dbentry.init;
			with (dball[globalindex]) {
				path = e.name();
				//mtime = e.timeLastModified();
				mtime = tmp.st_mtime;
				device = tmp.st_dev;
				inode = tmp.st_ino;
				size = tmp.st_size;
				dir1 = doi;

				totalsize += size;
			}
			++numfiles;
			if (doi) ++leftfiles;
			else ++rightfiles;
			++globalindex;
		}
	}
	totalfiles+=numfiles;
	log(format("%d (%s) files in %s", numfiles, doi?"A":"B", dir));
	log(format("\t%d bytes.", totalsize));
}

void singlefilescan(string filename) {
	ulong totalsize=0;
	
	auto e = DirEntry(filename);
	auto a = e.linkAttributes();

	if (!attrIsSymlink(a) && attrIsFile(a)) {
		auto tmp = e.statBuf();
		dball[globalindex] = dbentry.init;
		with (dball[globalindex]) {
			path = e.name();
			//mtime = e.timeLastModified();
			mtime = tmp.st_mtime;
			device = tmp.st_dev;
			inode = tmp.st_ino;
			size = tmp.st_size;
			dir1 = true;

			totalsize += size;
		}
		++leftfiles;
		++globalindex;
		totalfiles++;
		log(format("(A) file: %s", e.name()));
		log(format("\t%d bytes.", totalsize));
	}
	else {
		logf("Error on %s", filename);
	}
}

void remotedirscan(string host, bool doi, string dir, bool recurse) {
	ulong numfiles=0, totalsize=0;
	long result=-999;

	auto rsh_cmd = environment.get("DIRCMP_RSH");
	if (rsh_cmd == null)
		rsh_cmd = "ssh -C %H dircmp --server";

	string rsh;
	auto hostreplace = ctRegex!(`%H`,"g");
	if (match(rsh_cmd, hostreplace))
		rsh = replaceAll(rsh_cmd, hostreplace, host);
	else
		rsh = rsh_cmd ~ " " ~ host ~ " dircmp --server";

	auto pipes = pipeShell(rsh);
	machinedb ~= hostdbentry(host,pipes);
	//scope(exit) wait(pipes.pid);

	/*
	auto testterm = tryWait(pipes.pid);
	if (testterm.terminated) {
		log("Error: RSH command terminated.");
		exit(-1);
	}
	*/

	pipes.stdin.writefln(`{ "func":"dirscan", "indexstart":%d, "dir":"%s", "recurse":%s }`, 
		globalindex, jsonescape(dir), recurse?"true":"false");
	pipes.stdin.flush();
	foreach(line; pipes.stdout.byLine) {
		//writeln(line);
		auto j = parseJSON(line);
		if ("result" in j.object) {
			result = j["result"].integer;
			break;
		}

		dbentry dbe;
		with (dbe) {
			//path = host ~ ":" ~ j["path"].str;
			path = ccunescape(j["path"].str);
			mtime = cast(time_t) j["mtime"].integer;
			device = cast(dev_t) j["device"].integer;
			inode = cast(ino_t) j["inode"].integer;
			size = cast(ulong) j["size"].integer;

			dir1 = doi;
			machine = cast(ubyte)(machinedb.length - 1);

			totalsize+=size;
		}
		assert(globalindex==j["index"].integer);
		dball[globalindex] = dbe;
		++globalindex; ++numfiles;
		//writeln(dball[globalindex-1]);
		
	}

	if (result == -999) {
		log("Error: RSH connection failure.");
		exit(-1);
	}

	totalfiles+=numfiles;
	log(format("%d (%s) files in %s", numfiles, doi?"A":"B", host ~ ":" ~ dir));
	log(format("\t%d bytes.", totalsize));
}

void readdump(bool doi, string file, ubyte machnum=1, bool testmode=false) {
	ulong numfiles=0;
	ulong totalsize=0;

	auto f = File(file,"r");
	string line;

	/*
	if (!testmode) {
		assert(machnum == machinedb.length);
		machinedb ~= hostdbentry.init;
		machinedb[machnum].hostname = file;
	}
	*/

	while ((line = f.readln()) !is null) {
		try {
		auto j = parseJSON(line);
		auto index = globalindex;
		
		if ("testfile" in j.object) {
			if ("probables" in j.object) {
				string p = j["probables"].str;
				switch(p) {
					case "name":	probs = probables.name; break;
					case "time":	probs = probables.time; break;
					case "nameandtime": probs = probables.nameandtime; break;
					case "none":	probs = probables.none; break;
					default: stderr.writeln("Unknown probables type.");
				}
			}
			if ("skipmd5" in j.object) skipmd5 = j["skipmd5"].type == JSON_TYPE.TRUE ? true : false;
			continue;
		}
		if ("test" in j.object) {
			auto id = cast(dbIndex) j["test"].integer;
			dbIndex k2 = ("key2" in j.object) ? cast(dbIndex)j["key2"].integer : -1;
			testresults[j["result"].str][id] = k2;
			continue;
		}
		if ("testextra" in j.object) {
			auto id = cast(dbIndex) j["testextra"].integer;
			dbIndex ek2 = ("ekey2" in j.object) ? cast(dbIndex)j["ekey2"].integer : -1;
			testextraresults[j["extraresult"].str][id] = ek2;
			continue;
		}

		if (testmode && "index" in j.object) index = cast(dbIndex)j["index"].integer;

		dball[index] = dbentry.init;
		with (dball[index]) {
			assert("path" in j.object);
			path = ccunescape(j["path"].str);
			mtime = cast(time_t) j["mtime"].integer;
			device = cast(dev_t) j["device"].integer;
			inode = cast(ino_t) j["inode"].integer;
			size = cast(ulong) j["size"].integer;
			/*
			foreach(i,m; j["mid"].array) 
				mid[i] = cast(ubyte)m.uinteger;
			foreach(i,m; j["md5"].array) 
				md5[i] = cast(ubyte)m.uinteger;
			*/
			if ("mid" in j.object) hextoubyte(j["mid"].str, mid);
			if ("md5" in j.object) hextoubyte(j["md5"].str, md5);
			midfilled = cast(ubyte) j["midfilled"].integer;
			md5filled = j["md5filled"].type == JSON_TYPE.TRUE ? true : false;

			dir1 = doi;
			machine = machnum;

			//needed for tests
			if (testmode && "dir1" in j.object) {
				dir1 = j["dir1"].type == JSON_TYPE.TRUE ? true : false;
				doi = dir1;
			}
			if (testmode && "machine" in j.object) machine = cast(ubyte)j["machine"].integer;

			totalsize+=size;
		}
		++numfiles;
		if (doi) ++leftfiles;
		else ++rightfiles;
		//writeln(dball[globalindex]);
		++globalindex;
		} catch(Exception e) {
			writefln("numfile: %d  Error: %s", numfiles, e.msg);
		}
		
	}
	totalfiles+=numfiles;
	log(format("%d files in %s", numfiles, file));
	log(format("\t%d bytes.", totalsize));
}


void midmd5file(immutable dbIndex k, ulong pos, ulong length, ref ubyte[] buf) {
	if (dball[k].midfilled)
		return;

	auto size = dball[k].size;
	auto f = File(dball[k].path, "rb");
	if (size <= length) {	//scan whole file
		//writeln("small file ", dball[k].path, " ", size);
		//auto buf = f.byChunk(4096).front();
		auto sl = f.rawRead(buf);
		dball[k].mid[] = md5Of(sl);
		dball[k].md5[] = dball[k].mid;
		dball[k].md5filled = true;
	} else {
		f.seek(pos);
		auto sl = f.rawRead(buf);
		assert(buf.length == sl.length);
		dball[k].mid[] = md5Of(sl);
		//writeln("mid md5: ", dball[k].mid, " ", dball[k].path);
	}
	dball[k].midfilled = 1;
}

void midscan(dbIndex k, int midsize, ref ubyte[] buffer) {
	ulong size=dball[k].size;
	ulong mid=size/2 - midsize/2;
	ulong alignedmid;
	uint p2 = midsize;

	//block align scan -- avoid possible 2 block reads
	//except if result would be 0 (we don't want first bytes)
	p2--;
	p2 |= p2 >> 1;
	p2 |= p2 >> 2;
	p2 |= p2 >> 4;
	p2 |= p2 >> 8;
	p2 |= p2 >> 16;
	p2++;
	alignedmid = mid & ~(p2-1);
	//writefln("old: %d %x    new: %d %x   p2: %d %x", mid, mid, alignedmid, alignedmid, p2, p2);

	if (alignedmid != 0)
		mid = alignedmid;

	midmd5file(k,mid,midsize,buffer);
}

void fillmidharddb(dbIndex k, void delegate(dbIndex) dg = null) {
	//fill mid on each hardlink
	auto hlkey = MHardlink(dball[k].machine, dball[k].device, dball[k].inode);
	if (hlkey in harddb)
	foreach(hl; harddb[hlkey]) {
		if (hl == k || dball[hl].midfilled) continue;
		dball[hl].mid[] = dball[k].mid;
		dball[hl].midfilled = dball[k].midfilled;
		if (dball[k].md5filled) {
			dball[hl].md5[] = dball[k].md5;
			dball[hl].md5filled = true;
		}
		if (dg && dball[hl].midfilled) dg(hl);
	}
}


void midscanall(dbIndex[] keys, int midsize, void delegate(dbIndex) dg = null) {
	//ubyte[4096] buffer;
	auto buffer = new ubyte[midsize];
	
	auto mhl = new SortBy([compmachine, comphardlink]);
	auto mhldg = &mhl.less;
	sort!(mhldg)(keys);

	int i=0;
	
	SetIter machines = new SetIter(keys, [compmachine]);
	speedrate = new SpeedRate();
	foreach(mkeys; machines) {
		if(dball[mkeys[0]].machine == 0) {
			foreach(dbIndex k; mkeys) {
				if (dball[k].midfilled) continue;
				midscan(k, midsize, buffer);
				if (dg) dg(k);
				fillmidharddb(k, dg);
				++i;
				if (i%11==0) renderprogress(i, keys.length, "files");
			}
		}
		else { //****Remote machine
			string jpre=format(`{ "func":"midscan", "midsize":%d, "keys":`, midsize);
			string jpost=`}`;
			//writeln(jpre, to!string(mkeys), jpost);
			auto conn = machinedb[dball[mkeys[0]].machine].conn;
			conn.stdin.writeln(jpre, to!string(mkeys), jpost);
			conn.stdin.flush();
			int countd=0;
			foreach(line; conn.stdout.byLine) {
				//writeln(line);
				auto j = parseJSON(line);
				if ("result" in j.object) break;
				if ("midmd5" in j.object) {
					dbIndex k = cast(dbIndex) j["key"].integer;
					hextoubyte(j["midmd5"].str, dball[k].mid);
					dball[k].midfilled = cast(ubyte) j["midfilled"].integer;

					if (midsize >= dball[k].size) {
						assert("md5filled" in j.object);
						dball[k].md5[] = dball[k].mid;
						dball[k].md5filled = true;
					}
					fillmidharddb(k, dg);

					++i;
					if (i%11==0) renderprogress(i, keys.length, "files");
				}
				else if ("progress" in j.object) {
					//ignore
				}
			}
		}
	}

	renderprogress(keys.length, keys.length, "files");
	if (!quiet) writeln();
}

void md5fillharddb(dbIndex k) {
	//fill md5 on each hardlink
	auto hlkey = MHardlink(dball[k].machine, dball[k].device, dball[k].inode);
	if (hlkey in harddb)
	foreach(hl; harddb[hlkey]) {
		if (hl == k || dball[hl].md5filled) continue;
		dball[hl].md5[] = dball[k].md5;
		dball[hl].md5filled = dball[k].md5filled;
	}
}

void md5file(dbIndex k, ref double sizesum, double totalsize) {
	if (dball[k].md5filled) return;

	MD5 calcmd5;
	auto f = File(dball[k].path, "rb");
	calcmd5.start();
	uint j=0;
	foreach (buffer; f.byChunk(64 * 1024)) {
		if (ctrlc) {
			if (!quiet) writeln("\nSIGINT caught");
			return;
		}
		calcmd5.put(buffer);
		sizesum+=buffer.length;
		++j;
		if (j%64 == 0) 
			renderprogress(sizesum/1000000, totalsize/1000000, "MB", (dball[k].dir1?"A - ":"B - ") ~ baseName(dball[k].path));
	}
	f.close();
	dball[k].md5[] = calcmd5.finish();
	dball[k].md5filled = true;
	//writeln("md5: ", toHexString(dball[k].md5), " ", dball[k].path);
	renderprogress(sizesum/1000000, totalsize/1000000, "MB", (dball[k].dir1?"A - ":"B - ") ~ baseName(dball[k].path));

	md5fillharddb(k);
}
/*
void md5sumcmd(dbIndex k, ref double sizesum, double totalsize) {
	if (dball[k].md5filled) return;

	import std.process;
	auto md5sum = execute(["md5sum", dball[k].path]);
	if (md5sum.status != 0) throw new Exception("md5sum failed.");
	string s = md5sum.output[0..32];
	
	hextoubyte(s, dball[k].md5);
	dball[k].md5filled = true;
	
	sizesum+=cast(double)dball[k].size;
}
*/

void hextoubyte(string str, ref ubyte[16] target) {
	for(int i=0; i<32; i+=2)
		target[i/2] = to!ubyte(str[i..i+2],16);
}

/** Finds the closest path matching name in klist to k1
    by matching basename then finding longest matching path
*/
dbIndex NameMatch(dbIndex k1, dbIndex[] klist) {
	if (klist.length==1) return klist[0];

	dbIndex closest=-1;
	int closestcount=-1;

	auto k1path = dball[k1].path.split("/");

	foreach(k2; klist) {
		auto k2path = dball[k2].path.split("/");
		int i = cast(int) k1path.length - 1;
		int j = cast(int) k2path.length - 1;

		//no name match? skip entry
		if (k1path[i] != k2path[j]) continue;

		int pathcount = 0;
		--i; --j;
		while (i>=0 && j>=0) {
			if (k1path[i] == k2path[j])
				++pathcount;
			--i; --j;
		}
		if (pathcount > closestcount) {
			closest = k2;
			closestcount = pathcount;
		}
	}
	//writeln(klist.length, " ", closest==-1?"NO  ":"YES ", dball[k1].path, "\t", dball[closest==-1?klist[0]:closest].path);

	if (closest < 0) return klist[0];	//no basename match
	return closest;
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

string writeobj(string member)(dbentry e) {
	string s;
	s = `"` ~ member ~ `": `;

	static if ( is(typeof(__traits(getMember, e, member)) == string) )
		s ~= `"` ~ __traits(getMember, e, member) ~ `"`;
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

void outputjson() {
	auto filename = resultsdir ~ fileprefix ~ "json";
	log("Dumping json data to ", filename);
	auto w = File(filename,"w");
	w.writefln(`{ "testfile":"generated", "skipmd5":%s, "probables":"%s" }`, 
		skipmd5?"true":"false", probs);
	foreach(k,e; dball) {
		string s;
		string[] pairs = [
			format("%s%d", `"index":`, k),
			writeobj!"dir1"(e),
			format("%s%s%s", `"path":"`, jsonescape(e.path), `"`),
			writeobj!"mtime"(e),
			writeobj!"device"(e),
			writeobj!"inode"(e),
			writeobj!"size"(e),
			writeobj!"mid"(e),
			writeobj!"md5"(e),
			writeobj!"midfilled"(e),
			writeobj!"md5filled"(e) ];
		s = pairs.join(", ");
		w.writeln("{" ~ s ~ "}");
	}
	foreach(typ,lup; results) {
		foreach(k,v; lup)
			w.writefln(`{ "test":%d, "result":"%s", "key2":%d }`, k, typ, v);
	}
	foreach(typ,lup; extraresults) {
		foreach(k,v; lup)
			w.writefln(`{ "testextra":%d, "extraresult":"%s", "ekey2":%d }`, k, typ, v);
	}
}

void testoutput(string testpath) {
	int passedtotal,testtotal;
	foreach(typ,lup; testresults) {
		int passed=0, typtotal=0;
		foreach(k,v; lup) {
			typtotal++;
			//don't check key2 for now, multiple correct answers
			//TODO: check and make sure key2 is a valid answer
			//if (k in results[typ] && results[typ][k] == v)
			if (k in results[typ])
				passed++;
			//else
				//writefln("Failed: %d %d - %s - %s", k,v, (k in dball) ? dball[k].path : "ERROR", typ);
		}
		writefln("%s: %d/%d %s", typ, passed, typtotal, passed==typtotal?"Passed":"Failed");
		passedtotal+=passed;
		testtotal+=typtotal;
	}
	foreach(typ,lup; testextraresults) {
		int passed=0, typtotal=0;
		foreach(k,v; lup) {
			typtotal++;
			if (k in extraresults[typ] && extraresults[typ][k] == v)
				passed++;
			else
				writefln("Failed: %d %d - %s - %s", k,v, dball[k].path, typ);
		}
		writefln("%s: %d/%d %s", typ, passed, typtotal, passed==typtotal?"Passed":"Failed");
		passedtotal+=passed;
		testtotal+=typtotal;
	}
	writefln("%s: %d/%d %s", testpath, passedtotal, testtotal, passedtotal==testtotal?"PASSED":"FAILED");
	if(passedtotal==testtotal) exit(0);
	else exit(testtotal-passedtotal);
}







int main(string[] args) {
	results["zero"] = null;
	results["unique"] = null;
	results["duplicate"] = null;
	results["duplicate-hardlink"] = null;
	results["probable-unique"] = null;
	results["probable-duplicate"] = null;
	extraresults["checkcorrupt"] = null;

	stdouttypes = ["unique":false, "probable-unique":false, "duplicate":false, "probable-duplicate":false];
	string[] consoleshowtypes;

	uint midsize = 4096;
	
	bool server = false;
	bool finddupsearly = false;
	skipmd5 = false;
	bool iknowwhatimdoing = false;
	bool jsonresults = false;
	noresults = false;
	quiet = false;
	noprogressbar = false;
	fileprefix = "dircmp-results.";
	resultsdir = "./";
	delim = "\t";
	string[] leftdirs,rightdirs,leftrdirs,rightrdirs,afiles,bfiles,a1files;
	string[] dumpndirs,dumprdirs;
	string dumpfile = "dircmp.dump";
	string testpath;

	noprogressbar = false;
	
	
	machinedb ~= hostdbentry.init;	//machinedb[0] reserved
	auto hasHost = ctRegex!(`^([^/\\]+):(.+$)`);

	
	
	void helpoutput() {
		writeln("\nUsage:");
		writeln(args[0], " [options] [--an|--ar] [a-dir] [--bn|--br] [b-dir] ...");
		writeln(q"EOS

Compares directories 'a' against directories 'b'.  All files in 'a' will
be categorized as a unique or duplicate file based on its presense in 'b'.
This program implements a rdfind-like algorithm to reduce the amount of time
spent md5-scanning whole files.  Parsible output files 'dircmp-results'
will be generated.

--an [DirectoryOfInterest] (Non-recursive search)
--ar [DirectoryOfInterest] (Recursive search)
--bn [CompareDir] (Non-recursive search)
--br [CompareDir] (Recursive search)
     At least one of --ar or --an, and one of --br or --bn is required.
     Multiple --ar,--an,--br,--bn options accepted.

--noprogressbar
     Don't output progress bar
--quiet|-q
     No console output; implies --noprogressbar

--finddupsearly|--ssd
     Sort md5 file-scaning to find duplicates as early as possible.
     Default behaviour is to sort by inode to reduce harddisk thrashing.
--skipmd5
     Skip the full-file md5 scan phase.  Not recommended.
--probables [name|time|nameandtime|none]
     If files match size and mid-md5 check, this will guess to categorize 
     all remaining files before the md5 scan on name or time.  Useful 
     if you skip the md5 scan or break out of it early.
--midsize|--ms [bytes]
     Use custom size for mid-bytes scan.  Default 4096 bytes.

--noresults
     Don't write results to disk.
--resultsdir [directory]
     Change output directory.  Default: current directory.
--delim [delim]
     Use custom delimiter when outputting duplicate files.  Default \t.
--consoleshow|--c [types]
     Types to show in console when found.
     Available: unique, duplicate, duplicate-hardlink,
                probable-unique, probable-duplicate, zero

EOS");
		std.c.stdlib.exit(-1);
	}

	try {
		getopt(args,
			"help|h", std.functional.toDelegate(&helpoutput),
			"finddupsearly|ssd",&finddupsearly,
			"consoleshow|c",&consoleshowtypes,
			"quiet|q",&quiet,
			"noprogressbar",&noprogressbar,
			"a1",&a1files,
			"an",&leftdirs,
			"bn",&rightdirs,
			"ar", &leftrdirs,
			"br", &rightrdirs,
			"af", &afiles,
			"bf", &bfiles,
			"skipmd5", &skipmd5,
			"probables", &probs,
			"midsize|ms", &midsize,
			"noresults", &noresults,
			"resultsdir", &resultsdir,
			"delim", &delim,
			"dumpn", &dumpndirs,
			"dumpr", &dumprdirs,
			"dumpfile", &dumpfile,
			"iknowwhatimdoing", &iknowwhatimdoing,
			"server", &server,
			"runtest", &testpath,
			"jsonresults|json", &jsonresults,
			//std.getopt.config.caseInsensitive)
			std.getopt.config.caseSensitive);
	} catch(Exception e) {
		stderr.writeln(e.msg);
		return 1;
	}

	if (server) runserver();

	if (testpath) {
		//quiet=true;
		noprogressbar=true;
		noresults=true;
		resultsdir = "";
		runtest(testpath);
		goto startcomparison;
	}
	
	if(resultsdir[$-1] != '/')
		resultsdir ~= '/';
	if(noresults || dumpndirs.length || dumprdirs.length) {
		noresults = true;
		resultsdir = "";
	} else
		filelog = File(resultsdir ~ fileprefix ~ "log.txt", "w");
	if(quiet)
		noprogressbar=true;

	foreach(c; consoleshowtypes)
		stdouttypes[c] = true;
	
	if (dumpndirs.length + dumprdirs.length > 0) {
		dbIndex[] keys;

		foreach (dir;dumpndirs) dirscan(true, dir, false);
		foreach (dir;dumprdirs) dirscan(true, dir, true);

		keys = dball.keys;
		sort!(sortonhardlink)(keys);

		SetIter hlsets = new SetIter(keys, [compmachine, comphardlink]);
		foreach(s; hlsets) {
			auto hlkey = MHardlink(cast(ubyte)0, dball[s[0]].device, dball[s[0]].inode);
			if (s.length > 1) 
				harddb[hlkey] = s.dup;
		}

		midscanall(keys, midsize);

		keys = null;

		writeln();
		if (!skipmd5) {
			double sizesum=0, totalsize=0;
			foreach (k, ref v; dball) {
				if (!v.md5filled) keys ~= k;
				totalsize += v.size;
			}
			sort!(sortonhardlink)(keys);
			speedrate = new SpeedRate();
			foreach (k; keys) {
				//md5sumcmd(k, sizesum, totalsize);
				md5file(k, sizesum, totalsize);
				
				//fill md5 on each hardlink
				auto hlkey = MHardlink(cast(ubyte)0, dball[k].device, dball[k].inode);
				if (hlkey in harddb) {
					foreach(hl; harddb[hlkey]) {
						if (hl == k) continue;
						dball[hl].md5[] = dball[k].md5;
						dball[hl].md5filled = true;
						//writeln("md5 skipped: ", dball[hl].path, totalsize/1000000);
						totalsize -= dball[hl].size;
					}
					harddb[hlkey] = null;
				}

				core.memory.GC.collect();	//32bit build memory leak without this...
			}
			renderprogress(sizesum/1000000, totalsize/1000000, "MB");
		}
		writeln();


		//output to newline seperated json objects...for now

		auto w = File(dumpfile,"w");
		foreach(k,e; dball) {
			string s;
			string[] pairs = [
				format("%s%s%s", `"path":"`, jsonescape(e.path), `"`),
				writeobj!"mtime"(e),
				writeobj!"device"(e),
				writeobj!"inode"(e),
				writeobj!"size"(e),
				writeobj!"mid"(e),
				writeobj!"md5"(e),
				writeobj!"midfilled"(e),
				writeobj!"md5filled"(e) ];
			s = pairs.join(", ");
			w.writeln("{" ~ s ~ "}");
		}
		std.c.stdlib.exit(0);
	}

	if (leftdirs.length + leftrdirs.length + afiles.length + a1files.length == 0) {
		writeln("Need at least one Directory of Interest (--an or --ar argument).  See --help.");
		std.c.stdlib.exit(-1);
	}
	if (rightdirs.length + rightrdirs.length + bfiles.length == 0) {
		writeln("Need at least one Comparison Directory (--bn or --br argument).  See --help.");
		std.c.stdlib.exit(-1);
	}
	/*
	if (skipmd5 && !iknowwhatimdoing) {
		writeln("--skip-md5 requires command line option --iknowwhatimdoing.");
		std.c.stdlib.exit(-1);
	}
	*/
	if(!noresults && !isValidPath(resultsdir)) {
		writeln("Invalid results directory path.");
		std.c.stdlib.exit(-1);
	}

	foreach(f;a1files) {
		singlefilescan(f);
	}

	foreach (dir;leftdirs) {
		if (auto m = match(dir,hasHost)) remotedirscan(m.captures[1], true, m.captures[2], false);
		else dirscan(true, dir, false);
	}
	foreach (dir;leftrdirs) {
		if (auto m = match(dir,hasHost)) remotedirscan(m.captures[1], true, m.captures[2], true);
		else dirscan(true, dir, true);
	}
	foreach (dir;rightdirs) {
		if (auto m = match(dir,hasHost)) remotedirscan(m.captures[1], false, m.captures[2], false);
		else dirscan(false, dir, false);
	}
	foreach (dir;rightrdirs) {
		if (auto m = match(dir,hasHost)) remotedirscan(m.captures[1], false, m.captures[2], true);
		else dirscan(false, dir, true);
	}
	foreach (file;afiles) readdump(true, file, 0);
	foreach (file;bfiles) readdump(false, file, 0);
	
	dball.rehash;

/*
Conditions for guaranteed unique:
-? zero-file
-unique filesize
-all files of particular filesize are in dir1 or otherdirs, but not both
-unique mid-md5 for size <= 4096
-unique md5
-all files of particular md5 are in dir1 or otherdirs, but not both

Conditions for guaranteed duplicate:
-same dev-inode (hardlink)
-same mid-md5 for size <= 4096
-same md5

Probable Duplicate (avoid md5sum):
-same: size,mid-md5,
	-user specified: filename,time - Probable Unique on inverse

Check corrupt (if done md5sum):
-same: size,mid-md5,filename?,time?, but diff md5

*/
startcomparison:

	int[2] count;
	ulong oldcount;
	
	dbIndex[] keys = dball.keys;
	log(keys.length, " total files.\n");


//***zero size***
	sort!(sortonsize)(keys);

	//manage zero byte files
	int c = 0;
	while (dball[keys.front].size == 0) {
		if(dball[keys.front].dir1)
			output("zero","stat",keys.front);
		//dball.remove(keys.front); //need keys for output stats
		keys.popFront();
		c++;
	}
	log(results["zero"].length, " zero-byte files.  ", c - results["zero"].length, " from dirn.  ", keys.length, " files left in workset.");


//***hardlinks***
	auto mhl = new SortBy([compmachine, comphardlink]);
	auto mhldg = &mhl.less;
	sort!(mhldg)(keys);
	int countdeferred;	
	SetIter hlsets = new SetIter(keys, [compmachine, comphardlink]);
	foreach(s; hlsets) {
		if (s.length == 1) continue; //skip uniques (1,0),(0,1)

		auto bi = MHardlink(dball[s[0]].machine, dball[s[0]].device, dball[s[0]].inode);
		auto spl = splitdirs(s);

		if (spl[false].length >= 1 && spl[true].length >= 1) {	//duplicates
			foreach(k; spl[true]) {
				output("duplicate-hardlink","hardlink",k,NameMatch(k, spl[false]));
				dball[k].flag = true;
				if (spl[false].length >= 2) {
					foreach(j; spl[false])
						harddb[bi] ~= j;
				}
			}
		} else {
			foreach(k; s)
				harddb[bi] ~= k;
		}

	}
	harddb.rehash;

	count = removeflagged(keys);

	assert(count[1]==0);
	log(count[0], " hardlinked duplicates.  ", keys.length, " files left in workset.");


//***size***
	sort!(sortonsize)(keys);
	markuniques(keys, [compsize]);
	outputallunique("filesize",keys);
	count = keys.removeflagged();
	log(count, " uniques (filesize).  ", keys.length, " files left in workset.");


//---scan mid md5
	//TODO: feature: use alternate file section if mid is all 0x00 or 0xFF; use last bytes(?)
	log("\nScanning middle ", midsize, " bytes on all remaining files.");

	speedrate = new SpeedRate();
	midscanall(keys, midsize);
	log();

//***mid md5
	sort!(sortonmid)(keys);
	markuniques(keys, [compmid]);
	outputallunique("mid-md5",keys);
	count = removeflagged(keys);
	log(count, " uniques (mid-md5).  ", keys.length, " files left in workset.");


//***smallfile md5
	{
		//handle <=4096 files since it's already done...
		//uniques already handled in previous step, duplicates left
		dbIndex[] smallentries;
		foreach(k; keys)
			if (dball[k].size <= midsize)
				smallentries ~= k;
		
		sort!(sortonmd5)(smallentries);
		SetIter sets = new SetIter(smallentries, [compmd5]);
		foreach(s; sets) {
			assert(s.length > 1);
			auto spl = splitdirs(s);
			assert(spl[true].length && spl[false].length);
			foreach(i; spl[true]) {
				dball[i].flag = true;
				foreach(dn; spl[false]) dball[dn].flag = true;

				auto k = NameMatch(i, spl[false]);
				assert(dball[k].size <= midsize);
				assert(dball[i].size <= midsize);
				assert(!dball[k].dir1);
				assert(dball[i].dir1);
				assert(dball[i].md5filled);
				assert(dball[i].md5 == dball[k].md5);
				output("duplicate", "smallfile-md5", i, k);
			}
		}

		count = removeflagged(keys);
		log(count, " duplicates (smallfile-md5).  ", keys.length, " files left in workset.");
	}

//###calc probables
	{
		log();
		//TODO: nameortime ? nah, no need...
		SortBy nametime = new SortBy();

		log("Categorize probables by ", probs);
		switch(probs) {
		case probables.name:
			nametime.listcomp ~= compname;
			break;
		case probables.time:
			nametime.listcomp ~= comptime;
			break;
		default:
		case probables.nameandtime:
			nametime.listcomp ~= compname;
			nametime.listcomp ~= comptime;
			break;
		case probables.none:
			break;
		}

		bool delegate(dbIndex x, dbIndex y) dg = &nametime.less;

		auto method = map!(a=>a.desc)(nametime.listcomp).join("-");

		auto probkeys = keys.dup;
		
		sort!(sortonmid)(probkeys);
		SetIter itmid = new SetIter(probkeys, [compmid]);
		SetIter itsel;
		dbIndex[] idir1, idirn;
		//int numsets;
		
		foreach(midset; itmid) {
			assert(midset.length >= 2); //mid uniques handled in prev step
			/*
			if (midset.length == 1) {
				//writeln(dball[midset[0]].size, " ", dball[midset[0]].mid);
				continue; }
			*/

			sort!(dg)(midset);
			//foreach(i; midset) writeln(baseName(dball[i].path)); writeln();

			itsel = new SetIter(midset, nametime.listcomp);
			foreach(ts; itsel) {
				//++numsets;
				if (ts.length == 1) {
					if (dball[ts[0]].dir1)
						output("probable-unique",method,ts[0]);
					idir1=null;
					idirn=null;
					continue;
				}
				foreach(i; ts) {
					if (dball[i].dir1) idir1 ~= i;
					else idirn ~= i;
				}

				if (idirn.length) {
					foreach(k;idir1)
						output("probable-duplicate",method,k,NameMatch(k, idirn));
				} else {
					foreach(k;idir1)
						output("probable-unique",method,k);
				}
				idir1=idirn=null;
			}
			
			
		}
		//writeln(count);
		//writeln(numsets);
		logf("%d probably-unique.  ", results["probable-unique"].length);
		logf("%d probably-duplicate.", results["probable-duplicate"].length);
		log();
	}
	
	if(skipmd5) {
		outputstats(leftfiles,resultsdir,delim);
		if (testpath) testoutput(testpath);
		if (jsonresults) outputjson();
		std.c.stdlib.exit(0);
	}

	core.memory.GC.collect();

//---scan md5
	sigset(SIGINT, &siginthandle);

	//make midmd5 database
	//[size-hash][dir1] -> [key1, keyn...]
	//dbIndex[][bool][BigInt] midhashdb;
	dbIndex[][bool][string] midhashdb;
	sort!(sortonmid)(keys);
	SetIter si = new SetIter(keys,[compmid]);
	foreach(s; si) {
		//BigInt b = cast(BigInt)dball[s[0]].size << 128;
		//b += cast(BigInt)("0x" ~ toHexString(dball[s[0]].mid));
		string b = format("%.16X%s",dball[s[0]].size, toHexString(dball[s[0]].mid));
		foreach (k; s) {
			//writefln("%s %X %s", b, dball[k].size, toHexString(dball[k].mid));
			midhashdb[b][dball[k].dir1] ~= k;
		}
	}
	midhashdb.rehash;
	//writeln(midhashdb.length, " midhash db keys.");

	double totalsize=0,startsize=0;
	double sizesum=0;
	

	//md5db[md5] = [key1, keyn...]
	dbIndex[][ubyte[16]] md5db;

	log("MD5 all remaining files.");

	//get size estimate
	mhl = new SortBy([compmachine, comphardlink]);
	mhldg = &mhl.less;
	sort!(mhldg)(keys);	
	hlsets = new SetIter(keys, [compmachine, comphardlink]);
	foreach(s; hlsets)
		totalsize += dball[s[0]].size;


	SortBy dirhl;
	if (finddupsearly) dirhl = new SortBy([compmid, compdir, compmachine, comphardlink]);
	else dirhl = new SortBy([compdir, compmachine, comphardlink]);
	bool delegate(dbIndex x, dbIndex y) dgdirhl = &dirhl.less;
	sort!(dgdirhl)(keys);

	startsize=totalsize;
	c = 0;
	oldcount = results["duplicate"].length;
	speedrate = new SpeedRate();
md5search:
	foreach(dbIndex k; keys) {
		assert(dball[k].size > midsize);

		//skipflagged
		if (dball[k].flag) {
			assert(dball[k].dir1 == false);
			//logf("Removed from scan: %s - %d kbytes", dball[k].path, dball[k].size/1000);
			continue;
		}


		if (dball[k].machine == 0)
			md5file(k,sizesum,totalsize);
		else if (!dball[k].md5filled) {
			auto conn = machinedb[dball[k].machine].conn;
			conn.stdin.writefln(`{ "func":"md5file", "key":%d }`, k);
			conn.stdin.flush();
			foreach(line; conn.stdout.byLine) {
				//writeln(line);
				auto j = parseJSON(line);
				if ("result" in j.object) break;
				if ("md5sum" in j.object) {
					assert(k==cast(dbIndex) j["key"].integer);
					hextoubyte(j["md5sum"].str, dball[k].md5);
					dball[k].md5filled=true;
					md5fillharddb(k);

					sizesum+=dball[k].size;
					renderprogress(sizesum/1000000, totalsize/1000000, "MB", to!string(dball[k].machine) ~ " - " ~ baseName(dball[k].path));
				}
				else if ("progress" in j.object) {
					renderprogress(sizesum/1000000 + j["progress"].floating, totalsize/1000000, "MB", to!string(dball[k].machine) ~ " - " ~ baseName(dball[k].path));
				}
			}
		}
		

		if (ctrlc) {
			//writeln("Break on ", dball[k].path);
			break;
		}

		md5db[dball[k].md5] ~= k;
		auto s = splitdirs(md5db[dball[k].md5]);

		//handle duplicates
		if (s[true].length && s[false].length) {
			foreach(i; s[true]) {
				if (i !in results["duplicate"]) {
					assert(dball[i].size==dball[s[false][0]].size);
					//NameMatch won't work here because md5s in dirn could be missing still
					output("duplicate","md5scan",i,NameMatch(i, s[false]));
					dball[i].flag = true;
				}
			}
			
			//***** midhash section
			/* *****Explanation*****
				Some md5s need not be painfully run for certain files in dirn.
				In a set of files that all have the same size and midmd5, the conditions
				to skip are:
				1) all the md5s have been found for dir1.
				2) all the files from dir1 have been eliminated as duplicate in the previous step.
				With no more files in dir1, the files in the same size-mid set for dirn can be safely skipped.
			*/
			//BigInt b = cast(BigInt)dball[k].size << 128;
			//b += cast(BigInt)("0x" ~ toHexString(dball[k].mid));
			string b = format("%.16X%s",dball[k].size, toHexString(dball[k].mid));
			//writefln("%X %d", b, midhashdb.length);
			assert(b in midhashdb);
			assert(true in midhashdb[b]);

			foreach(i; midhashdb[b][true]) {
				if(!dball[i].md5filled) {
					assert(0, "Sort should ensure dir1 is always calc'ed before dirn");
					continue md5search; //if there's still md5s to be found, continue.  no big deal.
				}
			}
			//flag 2e to remove from scan
			foreach(i; midhashdb[b][true])
				if (i !in results["duplicate"])
					continue md5search;	//there's still 1f or 1e present...can't remove 2e
			
			long[MHardlink] toremove;
			foreach (dn; midhashdb[b][false]) {
				dball[dn].flag=true;
				if (!dball[dn].md5filled) {
					++c;
					//calc removed size
					auto hlkey = MHardlink(dball[dn].machine, dball[dn].device, dball[dn].inode);
					toremove[hlkey] = dball[dn].size;
					//writeln(dball[dn].path);
				}
			}
			foreach(uniq,sz; toremove)
				totalsize -= sz;
			toremove=null;
		}
	}
	renderprogress(sizesum/1000000, totalsize/1000000, "MB");
	count = removeflagged(keys);
	log();
	logf("%d files %.1f Mbytes removed from scan.", c, (startsize-totalsize)/1000000);
	log();
	log(count, " duplicates found during md5scan.  ", keys.length, " files left in workset.");

//***md5
	//incase of break, remove all !md5filled
	c = 0;

	foreach(k; keys) {
		if (!dball[k].md5filled) {
			dball[k].flag = true;
			++c;
		}
	}

	removeflagged(keys);

	if (c)
		log(c, " files not scanned.");
	else { //can't do uniques if user break, and duplicates should be found mid-scan
		sort!(sortonmd5)(keys);
		markuniques(keys,[compmd5]);
		outputallunique("md5",keys);
		count = removeflagged(keys);
		log(count, " uniques (md5).  ", keys.length, " files left in workset.");
		
		if (keys.length)
			writeln("Uh oh!  Should have no files left.  Please submit bug report.");
	}

	outputstats(leftfiles, resultsdir, delim);
	
	if (testpath) testoutput(testpath);
	if (jsonresults) outputjson();
	
	return 0;
}
