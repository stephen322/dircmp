module scanschecksums;

import std.stdio;
import std.digest.md;
import core.sys.posix.sys.stat;  //dev_t ino_t time_t
import std.path : baseName, isValidPath;
import std.c.stdlib : exit;
import std.file;
import std.string;
import std.regex;
import std.process;
import std.json;
import std.conv;

import globals;
import progress;



//TODO: permissions exception
//TODO: warn/exit on directories in both left and right
void dirscan(bool doi, string dir, bool recursive) {
	ulong numfiles=0, numcached;
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

				auto hlkey = MHardlink(machine, device, inode);
				if (hlkey in md5cachedb) {
					auto ce = md5cachedb[hlkey];
					if (ce.mtime == mtime && ce.size == size) {
						midfilled = ce.midfilled;
						mid = ce.mid;
						md5filled = ce.md5filled;
						md5 = ce.md5;
						++numcached;
					}
				}
				
				totalsize += size;
			}
			++numfiles;
			if (doi) ++numafiles;
			++globalindex;
		}
	}
	
	log(format("%d (%s) files in %s", numfiles, doi?"A":"B", dir));
	if (numcached > 0) log(format("\tCached entries found: %d", numcached));
	log(format("\t%d bytes.", totalsize));
}

void singlefilescan(string filename) {
	ulong totalsize=0, numcached;
	
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
			
			auto hlkey = MHardlink(machine, device, inode);
			if (hlkey in md5cachedb) {
				auto ce = md5cachedb[hlkey];
				if (ce.mtime == mtime && ce.size == size) {
					midfilled = ce.midfilled;
					mid = ce.mid;
					md5filled = ce.md5filled;
					md5 = ce.md5;
					++numcached;
				}
			}

			totalsize += size;
		}
		++numafiles;
		++globalindex;
		log(format("(A) file: %s", e.name()));
		if (numcached) log(format("\tCached"));
		log(format("\t%d bytes.", totalsize));
	}
	else {
		logf("Error on %s", filename);
	}
}

int remoteconnect(string host) {
	auto rsh_cmd = environment.get("DIRCMP_RSH");
	if (rsh_cmd == null)
		rsh_cmd = "ssh -C %H dircmp --server";

	string rsh;
	auto hostreplace = ctRegex!(`%H`,"g");
	if (match(rsh_cmd, hostreplace))
		rsh = replaceAll(rsh_cmd, hostreplace, host);
	else
		rsh = rsh_cmd ~ " " ~ host ~ " dircmp --server";

	ProcessPipes pipes = pipeShell(rsh);
	auto index = cast(int)machinedb.length;
	machinedb ~= hostdbentry(host,pipes,index);
	machlu[host] = cast(ubyte)(index);
	
	//scope(exit) wait(pipes.pid);

	/*
	auto testterm = tryWait(pipes.pid);
	if (testterm.terminated) {
		log("Error: RSH command terminated.");
		exit(-1);
	}
	*/
	return index;
}

void remotedirscan(int mindex, bool doi, string dir, bool recurse) {
	ulong numfiles=0, totalsize=0, numcached;
	long result=-999;

	auto pipes = machinedb[mindex].conn;
	
	//FIXME: very ugly, have to re-read md5cache file
	if (options.cache)
		loadmd5cacheremote(options.resultsdir ~ options.fileprefix ~ "md5cache", cast(ubyte)mindex);
	
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
			machine = cast(ubyte)(mindex);
			
			auto hlkey = MHardlink(machine, device, inode);
			if (hlkey in md5cachedb) {
				auto ce = md5cachedb[hlkey];
				if (ce.mtime == mtime && ce.size == size) {
					midfilled = ce.midfilled;
					mid = ce.mid;
					md5filled = ce.md5filled;
					md5 = ce.md5;
					++numcached;
				}
			}

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

	log(format("%d (%s) files in %s", numfiles, doi?"A":"B", machinedb[mindex].hostname ~ ":" ~ dir));
	if (numcached > 0) log(format("\tCached entries found: %d", numcached));
	log(format("\t%d bytes.", totalsize));
}

void readmd5cache(string filename, void delegate(dbentry,string) callback) {
	if (!exists(filename))
		return;
	
	auto f = File(filename,"r");
	string line, hostname;
	while ((line = f.readln()) !is null) {
		auto j = parseJSON(line);
		if ("midsize" in j.object) {
			if (j["midsize"].integer != options.midsize) {
				stderr.writeln(`"midsize" mismatch.  Delete cache files or adjust midsize accordingly.`);
				exit(-1);
			}
			continue;
		}
		
		dbentry dbe;
		with (dbe) {
			if ("host" in j.object) {
				hostname = j["host"].str;
				if (hostname in machlu) machine = machlu[hostname];
				else continue;
			} else {
				machine = 0;
			}
			mtime = cast(time_t) j["mtime"].integer;
			device = cast(dev_t) j["device"].integer;
			inode = cast(ino_t) j["inode"].integer;
			size = cast(ulong) j["size"].integer;
			if ("mid" in j.object) hextoubyte(j["mid"].str, mid);
			if ("md5" in j.object) hextoubyte(j["md5"].str, md5);
			midfilled = cast(ubyte) j["midfilled"].integer;
			md5filled = j["md5filled"].type == JSON_TYPE.TRUE ? true : false;
		}
		callback(dbe, line);
	}
}

void loadmd5cacheremote(string filename, ubyte machine) {
	void fill(dbentry e, string line) {
		if (e.machine != machine)
			return;
		auto hlkey = MHardlink(e.machine, e.device, e.inode);
		md5cachedb[hlkey] = e;
	}
	readmd5cache(filename, &fill);
}

void loadmd5cachedb(string filename) {
	void fill(dbentry e, string line) {
		auto hlkey = MHardlink(e.machine, e.device, e.inode);
		md5cachedb[hlkey] = e;
	}
	readmd5cache(filename, &fill);
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
			if ("skipmd5" in j.object) options.skipmd5 = j["skipmd5"].type == JSON_TYPE.TRUE ? true : false;
			if ("midsize" in j.object) options.midsize = cast(uint)j["midsize"].integer;
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
		if (doi) ++numafiles;
		//writeln(dball[globalindex]);
		++globalindex;
		} catch(Exception e) {
			writefln("numfile: %d  Error: %s", numfiles, e.msg);
		}
		
	}
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

//fill mid on each hardlink
void fillmidharddb(dbIndex k, void delegate(dbIndex) dg = null) {
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
	if (!options.quiet) writeln();
}

void md5fillharddb(dbIndex k) {
	//fill md5 on each hardlink
	auto hlkey = MHardlink(dball[k].machine, dball[k].device, dball[k].inode);
	if (hlkey in harddb)
	foreach(hl; harddb[hlkey]) {
		if (dball[hl].md5filled) continue;
		dball[hl].md5[] = dball[k].md5;
		dball[hl].md5filled = dball[k].md5filled;
	}
}

void md5file(dbIndex k, ref double sizesum, double totalsize) {
	if (dball[k].md5filled)
		return;
	
	MD5 calcmd5;
	auto f = File(dball[k].path, "rb");
	calcmd5.start();
	uint j=0;
	foreach (buffer; f.byChunk(64 * 1024)) {
		if (ctrlc) {
			if (!options.quiet) writeln("\nSIGINT caught");
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

	import std.process : execute;
	auto md5sum = execute(["md5sum", dball[k].path]);
	if (md5sum.status != 0) throw new Exception("md5sum failed.");
	string s = md5sum.output[0..32];
	
	hextoubyte(s, dball[k].md5);
	dball[k].md5filled = true;
	
	sizesum+=cast(double)dball[k].size;
}
*/