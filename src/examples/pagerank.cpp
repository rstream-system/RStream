/*
 * pagerank.cpp
 *
 *  Created on: Mar 7, 2017
 *      Author: kai
 */

#include "../core/engine.hpp"
#include "../core/scatter.hpp"
#include "../core/gather.hpp"
#include "../core/relation_phase.hpp"

using namespace RStream;

struct Update : BaseUpdate{
	int target;
	float sum;

	Update(int t, float s) {
		target = t;
		sum = s;
	}

	Update() : target(0), sum(0.0) {}

	std::string toString(){
		return "(" + std::to_string(target) + ", " + std::to_string(sum) + ")";
	}
};

inline std::ostream & operator<<(std::ostream & strm, const Update& update){
	strm << "(" << update.target << ", " << update.sum << ")";
	return strm;
}

struct Vertex : BaseVertex {
	int degree;
//	float pr_value;
};

inline std::ostream & operator<<(std::ostream & strm, const Vertex& vertex){
	strm << "[" << vertex.id << "," << vertex.degree << "]";
	return strm;
}

//void init(char* vertices) {
//	struct Vertex* v= (struct Vertex*) vertices;
//	v->degree = 0;
////	v->vertexId = 0;
//}


Update* generate_one_update(Edge & e, Vertex* src_vertex)
{
	Update* update = new Update(e.target, 1.0);
	return update;
}

void apply_one_update(Update & update, Vertex* dst_vertex) {
	dst_vertex->degree += update.sum;
}

class R1 : public RPhase<Update> {
public:
	R1(Engine & e) : RPhase(e) {};

	bool filter(Update & update, VertexId edge_src, VertexId edge_target) {
		return false;
	}

	void project_columns(NewUpdateType * new_update) {

	}
};

int main(int argc, const char ** argv) {
//	engine<Vertex> graph_engine("/home/icuzzq/Workspace/git/RStream/input/input");
//	std::function<void(char*)> initialize = init;
//	graph_engine.init_vertex(init);
//	std::function<T*(Edge&)> gen_update = generate_one_update;
//	graph_engine.scatter_no_vertex(generate_one_update);

	Engine e("/home/icuzzq/Workspace/git/RStream/input/input");
	Scatter<Vertex, Update> scatter_phase(e);
	scatter_phase.scatter_with_vertex(generate_one_update);
	Gather<Vertex, Update> gather_phase(e);
	gather_phase.gather(apply_one_update);
	R1 r1(e);
	struct Update_Stream in_stream = {"update0"};
	struct Update_Stream out_stream = {"update1"};
	r1.join(in_stream, out_stream);
}


