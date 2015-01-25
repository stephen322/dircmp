module server;

import std.stdio;
import std.json;
import std.algorithm;
import std.c.stdlib : exit;
import std.array : appender;
import std.digest.digest : toHexString;

import globals;
import progress;
import scanschecksums;


void runserver() {
	quiet = true;
	noresults = true;
	//noprogressbar = true;
	renderprogress = &progress_json;
	try {
		string line;
		while ((line = stdin.readln()) !is null) {
			//writeln(line);
			auto j = parseJSON(line);
			processinst(j);
		}
	} catch(Exception e) {
		writefln(`{ "result": -1, "msg":"%s" }`, jsonescape(e.msg));
		exit(0);
	}

	exit(0);
}

void processinst(JSONValue j) {
	double ssum;

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