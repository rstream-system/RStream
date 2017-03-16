/*
 * pagerank.cpp
 *
 *  Created on: Mar 7, 2017
 *      Author: kai
 */

#include "../core/engine.hpp"

using namespace RStream;

struct Update : T {
	float sum;

	Update(int t, float s) {
		target = t;
		sum = s;
	}
};

struct Vertex {
	int degree;
	float rank;
	float sum;
};

void init(char* vertices) {
	struct Vertex* v= (struct Vertex*) vertices;
	v->degree = 0;
	v->rank = 1.0;
	v->sum = 0.0;
}

inline std::ostream & operator<<(std::ostream & strm, const Update& update){
	strm << "(" << update.target << ", " << update.sum << ")";
	return strm;
}

Update* generate_one_update(Edge & e)
{
	Update* update = new Update(e.target, 0);
	return update;
}

void apply_one_update(T & update, char * vertex) {

}

int main(int argc, const char ** argv) {
	engine<Vertex> graph_engine("/home/icuzzq/Workspace/git/RStream/input/input");
	std::function<void(char*)> initialize = init;
	graph_engine.init_vertex(init);
	std::function<T*(Edge&)> gen_update = generate_one_update;
	graph_engine.scatter_no_vertex(generate_one_update);

}


