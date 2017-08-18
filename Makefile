CXX=g++
CFLAGS=-std=c++0x -O3
CFLAGS+= -Ilib/bliss-0.73/ -Llib/bliss-0.73/
LIBS=-lbliss -lpthread

SOURCE=$(shell ls src/core/*.cpp src/struct/*.cpp src/utility/*.cpp)

TARGETS=bin/triangle_count bin/motif_count bin/trans_closure bin/pagerank bin/fsm bin/pr_cf bin/cc bin/clique_find_simple


all: bliss $(TARGETS)

#bin/clique_find: bliss src/apps/cliquefinding.cpp $(SOURCE)
#	$(CXX) $(CFLAGS) -o $@ src/apps/cliquefinding.cpp $(SOURCE) $(LIBS)

bin/triangle_count: src/apps/trianglecounting.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ src/apps/trianglecounting.cpp $(SOURCE) $(LIBS) 

bin/motif_count: src/apps/motifcounting.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ src/apps/motifcounting.cpp $(SOURCE) $(LIBS) 

bin/trans_closure: src/apps/transitiveclosure.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ src/apps/transitiveclosure.cpp $(SOURCE) $(LIBS) 

bin/pagerank: src/apps/pagerank.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ src/apps/pagerank.cpp $(SOURCE) $(LIBS)

bin/fsm: src/apps/fsm.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ src/apps/fsm.cpp $(SOURCE) $(LIBS)

bin/pr_cf: src/apps/pr_cf.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ src/apps/pr_cf.cpp $(SOURCE) $(LIBS)

bin/cc: src/apps/cc.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ src/apps/cc.cpp $(SOURCE) $(LIBS)

bin/clique_find_simple: src/apps/cliquefinding_simple.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ src/apps/cliquefinding_simple.cpp $(SOURCE) $(LIBS)


bliss:
	make -C lib/bliss-0.73/


clean:
	rm -f $(TARGETS)
	make -C lib/bliss-0.73/ clean
