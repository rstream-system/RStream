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
#include "../utility/preprocessing.hpp"

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

struct RInUpdate : BaseUpdate {
	int src;
	int target;

	RInUpdate(int s, int t) : src(s), target(t) {}
	RInUpdate() : src(0), target(0) {}
};

struct ROutUpdate : BaseUpdate {
	int src1;
	int src2;
	int target;

	ROutUpdate(int s1, int s2,  int t) : src1(s1), src2(s2), target(t) {}
	ROutUpdate() : src1(0), src2(0), target(0) {}
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

class R1 : public RPhase<RInUpdate, ROutUpdate> {
public:
	R1(Engine & e) : RPhase(e) {};

	bool filter(RInUpdate & update, VertexId edge_src, VertexId edge_target) {
		return false;
	}

	void project_columns(char * join_result, ROutUpdate * new_update) {

	}
};

//int main(int argc, const char ** argv) {
//	engine<Vertex> graph_engine("/home/icuzzq/Workspace/git/RStream/input/input");
//	std::function<void(char*)> initialize = init;
//	graph_engine.init_vertex(init);
//	std::function<T*(Edge&)> gen_update = generate_one_update;
//	graph_engine.scatter_no_vertex(generate_one_update);

//	Engine e("/home/icuzzq/Workspace/git/RStream/input/input");
//	Scatter<Vertex, Update> scatter_phase(e);
//	scatter_phase.scatter_with_vertex(generate_one_update);
//	Gather<Vertex, Update> gather_phase(e);
//	gather_phase.gather(apply_one_update);

//	R1 r1(e);
//	struct Update_Stream in_stream = {"update0"};
//	struct Update_Stream out_stream = {"update1"};
//	r1.join(in_stream, out_stream);
//}


int main(int argc, char ** argv) {
		int opt;
		std::string input = "";
		std::string output = "";
		VertexId vertices = -1;
		int partitions = -1;
		int edge_type = 0;

		while ((opt = getopt(argc, argv, "i:o:v:p:t:")) != -1) {
			switch (opt) {
			case 'i':
				input = optarg;
				break;
			case 'o':
				output = optarg;
				break;
			case 'v':
				vertices = atoi(optarg);
				break;
			case 'p':
				partitions = atoi(optarg);
				break;
			case 't':
				edge_type = atoi(optarg);
				break;
			}
		}
		if (input=="" || output=="" || vertices==-1) {
			fprintf(stderr, "usage: %s -i [input path] -o [output path] -v [vertices] -p [partitions] -t [edge type: 0=unweighted, 1=weighted]\n", argv[0]);
			exit(-1);
		}

		Preprocessing proc(input, output, partitions, vertices, edge_type);
		proc.generate_partitions();
}

