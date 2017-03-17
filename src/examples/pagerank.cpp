/*
 * pagerank.cpp
 *
 *  Created on: Mar 7, 2017
 *      Author: kai
 */

#include "../core/engine.hpp"
#include "../core/scatter.hpp"
#include "../core/gather.hpp"

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
	int vertexId;
};

void init(char* vertices) {
	struct Vertex* v= (struct Vertex*) vertices;
	v->degree = 0;
	v->vertexId = 0;
}

inline std::ostream & operator<<(std::ostream & strm, const Update& update){
	strm << "(" << update.target << ", " << update.sum << ")";
	return strm;
}

Update* generate_one_update(Edge & e, char* vertices)
{
	Update* update = new Update(e.target, 0);
	return update;
}

void apply_one_update(T & update, char * vertex) {

}

int main(int argc, const char ** argv) {
//	engine<Vertex> graph_engine("/home/icuzzq/Workspace/git/RStream/input/input");
//	std::function<void(char*)> initialize = init;
//	graph_engine.init_vertex(init);
//	std::function<T*(Edge&)> gen_update = generate_one_update;
//	graph_engine.scatter_no_vertex(generate_one_update);

	engine<Vertex, Update> e("/home/icuzzq/Workspace/git/RStream/input/input");
	Scatter<Vertex, Update> scatter_phase(e);
	scatter_phase.scatter_with_vertex(generate_one_update);
	Gather<Vertex, Update> gather_phase(e);
	gather_phase.gather(apply_one_update);
}


