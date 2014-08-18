all: dircmp

dircmp: dircmp.o
	gdc dircmp.o -o dircmp

dircmp.o: dircmp.d
	gdc -O2 -c dircmp.d

install: dircmp
	strip dircmp
	cp -a dircmp /usr/local/bin/
	chown root:root /usr/local/bin/dircmp

