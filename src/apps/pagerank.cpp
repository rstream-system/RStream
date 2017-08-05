//	/*
//	 * pagerank.cpp
//	 *
//	 *  Created on: Mar 7, 2017
//	 *      Author: kai
//	 */
//
//	#include "../core/engine.hpp"
//	#include "../core/scatter.hpp"
//	#include "../core/gather.hpp"
//	//#include "BaseApplication.hpp"
//
//	using namespace RStream;
//
//	struct Update : BaseUpdate{
//		float rank;
//
//		Update(int _target, float _rank) : BaseUpdate(_target), rank(_rank) {};
//
//		Update() : BaseUpdate(0), rank(0.0) {}
//
//		std::string toString(){
//			return "(" + std::to_string(target) + ", " + std::to_string(rank) + ")";
//		}
//	}__attribute__((__packed__));
//
//
//	struct Vertex : BaseVertex {
//		int degree;
//		float rank;
//		float sum;
//	}__attribute__((__packed__));
//
//	//inline std::ostream & operator<<(std::ostream & strm, const Vertex& vertex){
//	//	strm << "[" << vertex.id << "," << vertex.degree << "]";
//	//	return strm;
//	//}
//
//	void initttt(char* data, VertexId id) {
//		struct Vertex * v = (struct Vertex*)data;
//		v->degree = 0;
//		v->sum = 0;
//		v->rank = 1.0f;
//		v->id = id;
//	}
//
//	Update * generate_one_update(Edge * e, Vertex * v) {
//		Update* update = new Update(e->src, v->rank / v->degree);
//		return update;
//	}
//
//	void apply_one_update(Update * update, Vertex * dst_vertex) {
//		dst_vertex->sum += update->rank;
//		dst_vertex->rank = 0.15 + 0.85 * dst_vertex->sum;
//	}
//
//
//	int main(int argc, char ** argv) {
////		Engine e("/home/icuzzq/Workspace/git/RStream/input/input_new.txt", 3, 6);
//		Engine e(std::string(argv[1]), atoi(argv[2]), atoi(argv[3]));
////		e.init_vertex<Vertex>(initttt);
////		e.init_vertex<Vertex>(init);
////		e.compute_degree<Vertex>();
//
//		int num_iters = 5;
//		for(int i = 0; i < num_iters; i++) {
//			Scatter<Vertex, Update> scatter_phase(e);
//			Update_Stream in_stream = scatter_phase.scatter_with_vertex(generate_one_update);
//			Gather<Vertex, Update> gather_phase(e);
//			gather_phase.gather(in_stream, apply_one_update);
//		}
//	}
//
//
//
