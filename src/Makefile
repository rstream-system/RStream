CXX=g++
CFLAGS=-std=c++0x -O3
CFLAGS+= -I../lib/bliss-0.73/ -L../lib/bliss-0.73/
LIBS=-lbliss -lpthread

SOURCE=$(shell ls core/*.cpp struct/*.cpp utility/*.cpp)

TARGETS=../bin/triangle_count ../bin/motif_count ../bin/trans_closure ../bin/pagerank ../bin/fsm ../bin/pr_cf ../bin/cc ../bin/clique_find_simple

all: $(TARGETS)

#../bin/clique_find: bliss apps/cliquefinding.cpp $(SOURCE)
#	$(CXX) $(CFLAGS) -o $@ apps/cliquefinding.cpp $(SOURCE) $(LIBS)

../bin/triangle_count: bliss apps/trianglecounting.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ apps/trianglecounting.cpp $(SOURCE) $(LIBS) 

../bin/motif_count: bliss apps/motifcounting.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ apps/motifcounting.cpp $(SOURCE) $(LIBS) 

../bin/trans_closure: bliss apps/transitiveclosure.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ apps/transitiveclosure.cpp $(SOURCE) $(LIBS) 

../bin/pagerank: bliss apps/pagerank.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ apps/pagerank.cpp $(SOURCE) $(LIBS)

../bin/fsm: bliss apps/fsm.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ apps/fsm.cpp $(SOURCE) $(LIBS)

../bin/pr_cf: bliss apps/pr_cf.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ apps/pr_cf.cpp $(SOURCE) $(LIBS)

../bin/cc: bliss apps/cc.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ apps/cc.cpp $(SOURCE) $(LIBS)

../bin/clique_find_simple: bliss apps/cliquefinding_simple.cpp $(SOURCE)
	$(CXX) $(CFLAGS) -o $@ apps/cliquefinding_simple.cpp $(SOURCE) $(LIBS)



bliss:
	make -C ../lib/bliss-0.73/

clean:
	rm -f $(TARGETS)
	make -C ../lib/bliss-0.73/ clean
