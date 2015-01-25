SOURCES=dircmp.d scanschecksums.d server.d globals.d progress.d
OBJECTS=$(SOURCES:.d=.o)

all: dircmp

dircmp: $(OBJECTS)
	gdc -o dircmp $(OBJECTS)

%.o: %.d
	gdc -O2 -c $< -o $@

install: dircmp
	strip dircmp
	cp -a dircmp /usr/local/bin/
	chown root:root /usr/local/bin/dircmp

