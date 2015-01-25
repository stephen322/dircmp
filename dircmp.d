/*  TODO
-output parsable text for GUIs
-diff device concurrency
-output multiple duplicate matches
-treat symlinks as files
-eliminate unique-sized b-files during scan
-option to cache md5 data between runs
*/

import std.stdio;
import std.algorithm;
import std.array;
import std.string;
import std.conv;
import std.getopt;
import std.parallelism;
import std.json;
import std.regex;
import core.sys.posix.signal;

//ugh, a linker error if I use the definition from globals.d??
import std.typecons;
alias MHardlink = Tuple!(ubyte, dev_t, ino_t);	//machine,dev,inode

import globals;
import progress;
import server;
import scanschecksums;



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



void runtest(string path) {
	readdump(true, path, 0, true);
	dball.rehash;
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
	auto hasHost = ctRegex!(`^([^/\\]+):(.+$)`);  // host:path

	
	
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
	sort!(sortonmid)(keys);
	SetIter sets = new SetIter(keys, [compmid]);
	foreach(s; sets) {
		assert(s.length > 1);
		if (dball[s[0]].size > midsize) break;
		
		auto spl = splitdirs(s);
		assert(spl[true].length && spl[false].length);  //uniques eliminated in last step
		foreach(i; spl[true]) {
			dball[i].flag = true;
			foreach(dn; spl[false]) dball[dn].flag = true;
			
			auto k = NameMatch(i, spl[false]);
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
			
			//***** midhash possibleskip section
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
