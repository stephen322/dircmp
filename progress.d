module progress;

import std.stdio;
import std.string;
import std.datetime;
import std.algorithm : sort;

import globals;

__gshared SpeedRate speedrate;
auto renderprogress = &progress_console;

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

void progress_console(double cur, double total, string units, string text="") {
	static ulong lastline;
	if (options.noprogressbar)
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

void progress_json(double cur, double total, string units, string text="") {
	if (options.noprogressbar)
		return;

	if (speedrate.peekmsecs() < 500 && cur!=total) return;

	speedrate.restart();
	
	writefln(`{ "progress":%.2f }`, cur);
	stdout.flush();
}