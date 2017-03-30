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
//#include "../core/io_manager.hpp"

using namespace RStream;

//struct Update : BaseUpdate{
//	int target;
//	float sum;
//
//	Update(int t, float s) {
//		target = t;
//		sum = s;
//	}
//
//	Update() : target(0), sum(0.0) {}
//
//	std::string toString(){
//		return "(" + std::to_string(target) + ", " + std::to_string(sum) + ")";
//	}
//};

struct RInUpdate : BaseUpdate {
	VertexId src;
//	int target;

	RInUpdate(VertexId t, VertexId s){
		target = t;
		src = s;
	}

	RInUpdate() {
//		target = 0;
//		src = 0;
	}
};

inline std::ostream & operator<<(std::ostream & strm, const RInUpdate& update){
	strm << "(" << update.target << ", " << update.src << ")";
	return strm;
}

struct ROutUpdate : BaseUpdate {
	VertexId src1;
	VertexId src2;
//	int target;

	ROutUpdate(VertexId t, VertexId s1, VertexId s2) {
		target = t;
		src1 = s1;
		src2 = s2;
	}
	ROutUpdate() {
//		target = 0;
//		src1 = 0;
//		src2 = 0;
	}
};

inline std::ostream & operator<<(std::ostream & strm, const ROutUpdate& update){
	strm << "(" << update.target << ", " << update.src1 << ", " << update.src2 << ")";
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

void init(char* data) {
	struct Vertex * v = (struct Vertex*)data;
	v->degree = 0;
}

RInUpdate* generate_one_update(Edge * e)
{
	RInUpdate* update = new RInUpdate(e->target, e->src);
	return update;
}

//void apply_one_update(Update * update, Vertex* dst_vertex) {
//	dst_vertex->degree += update->sum;
//}

class R1 : public RPhase<RInUpdate, ROutUpdate> {
public:
	R1(Engine & e) : RPhase(e) {};

	bool filter(RInUpdate * update, Edge * edge) {
		return false;
	}

	ROutUpdate * project_columns(RInUpdate * in_update, Edge * edge) {
//		std::cout << *edge << std::endl;
		ROutUpdate * new_update = new ROutUpdate(edge->target, in_update->src, in_update->target);
		return new_update;
	}
};


template<typename T>
void printUpdateStream(int num_partitions, std::string fileName, Update_Stream in_stream){
	for(int i = 0; i < num_partitions; i++) {
		std::cout << "--------------------" + (fileName + "." + std::to_string(i) + ".update_stream_" + std::to_string(in_stream)) + "---------------------\n";
		int fd_update = open((fileName + "." + std::to_string(i) + ".update_stream_" + std::to_string(in_stream)).c_str(), O_RDONLY);
		assert(fd_update > 0 );

		// get file size
		long update_file_size = io_manager::get_filesize(fd_update);

		char * update_local_buf = new char[update_file_size];
		io_manager::read_from_file(fd_update, update_local_buf, update_file_size, 0);

		// for each update
		for(size_t pos = 0; pos < update_file_size; pos += sizeof(T)) {
			// get an update
			T & update = *(T*)(update_local_buf + pos);
			std::cout << update << std::endl;
		}
	}
}

int main(int argc, char ** argv) {
//		int opt;
//		std::string input = "";
//		std::string output = "";
//		VertexId vertices = -1;
//		int partitions = -1;
//
//		while ((opt = getopt(argc, argv, "i:o:v:p:")) != -1) {
//			switch (opt) {
//			case 'i':
//				input = optarg;
//				break;
//			case 'o':
//				output = optarg;
//				break;
//			case 'v':
//				vertices = atoi(optarg);
//				break;
//			case 'p':
//				partitions = atoi(optarg);
//				break;
//			}
//		}
//		if (input=="" || output=="" || vertices==-1) {
//			fprintf(stderr, "usage: %s -i [input path] -o [output path] -v [vertices] -p [partitions] \n", argv[0]);
//			exit(-1);
//		}

		Engine e("/home/icuzzq/Workspace/git/RStream/input/input_new.txt", 3, 6);
		Scatter<Vertex, RInUpdate> scatter_phase(e);
		Update_Stream in_stream = scatter_phase.scatter_no_vertex(generate_one_update);
		printUpdateStream<RInUpdate>(e.num_partitions, e.filename, in_stream);

		R1 r1(e);
		Update_Stream out_stream = r1.join(in_stream);
		printUpdateStream<ROutUpdate>(e.num_partitions, e.filename, out_stream);
}



