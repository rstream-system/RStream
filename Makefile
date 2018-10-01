CXX=g++
CFLAGS=-std=c++0x -O3
CFLAGS+= -Ilib/bliss-0.73/ -Llib/bliss-0.73/
LIBS=-lbliss -lpthread

SOURCE=$(shell ls src/core/*.cpp src/struct/*.cpp src/utility/*.cpp)
OBJECTS=$(SOURCE:.cpp=.o)

TARGETS=bin/clique_find bin/triangle_count bin/motif_count bin/trans_closure bin/fsm
#TARGETS=bin/clique_find bin/triangle_count bin/motif_count bin/trans_closure bin/pagerank bin/fsm bin/pr_cf bin/cc

all: bliss $(SOURCES) $(TARGETS)

#bin/clique_find: bliss src/apps/cliquefinding.cpp $(OBJECTS)
#	$(CXX) $(CFLAGS) -o $@ src/apps/cliquefinding.cpp $(OBJECTS) $(LIBS)

bin/triangle_count: src/apps/trianglecounting.cpp $(OBJECTS)
	$(CXX) $(CFLAGS) -o $@ src/apps/trianglecounting.cpp $(OBJECTS) $(LIBS) 

bin/motif_count: src/apps/motifcounting.cpp $(OBJECTS)
	$(CXX) $(CFLAGS) -o $@ src/apps/motifcounting.cpp $(OBJECTS) $(LIBS) 

bin/trans_closure: src/apps/transitiveclosure.cpp $(OBJECTS)
	$(CXX) $(CFLAGS) -o $@ src/apps/transitiveclosure.cpp $(OBJECTS) $(LIBS) 

#bin/pagerank: src/apps/pagerank.cpp $(OBJECTS)
#	$(CXX) $(CFLAGS) -o $@ src/apps/pagerank.cpp $(OBJECTS) $(LIBS)

bin/fsm: src/apps/fsm.cpp $(OBJECTS)
	$(CXX) $(CFLAGS) -o $@ src/apps/fsm.cpp $(OBJECTS) $(LIBS)

#bin/pr_cf: src/apps/pr_cf.cpp $(OBJECTS)
#	$(CXX) $(CFLAGS) -o $@ src/apps/pr_cf.cpp $(OBJECTS) $(LIBS)

#bin/cc: src/apps/cc.cpp $(OBJECTS)
#	$(CXX) $(CFLAGS) -o $@ src/apps/cc.cpp $(OBJECTS) $(LIBS)

bin/clique_find: src/apps/cliquefinding_simple.cpp $(OBJECTS)
	$(CXX) $(CFLAGS) -o $@ src/apps/cliquefinding_simple.cpp $(OBJECTS) $(LIBS)

.cpp.o:
	$(CXX) $(CFLAGS) -c $< -o $@

bliss:
	make -C lib/bliss-0.73/


clean:
	rm -f $(TARGETS) $(OBJECTS)
	make -C lib/bliss-0.73/ clean
