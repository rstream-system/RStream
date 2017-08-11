//	/*
//	 * pagerank.cpp
//	 *
//	 *  Created on: Aug 10, 2017
//	 *      Author: Zhiqiang
//	 */
//
//	#include "../core/engine.hpp"
//	#include "../core/scatter.hpp"
//	#include "../core/gather.hpp"
//	#include "../core/global_info.hpp"
//
//	#include "../core/aggregation.hpp"
//	#include "../utility/ResourceManager.hpp"
//
//	using namespace RStream;
//
//	class MC : public MPhase {
//public:
//	MC(Engine & e, unsigned int maxsize) : MPhase(e, maxsize){};
//	~MC() {};
//
//	bool filter_join(MTuple_join & update_tuple){
//		return false;
//	}
//
//	bool filter_collect(MTuple & update_tuple){
//		return false;
//	}
//
//	bool filter_join_clique(MTuple_join_simple& update_tuple){
//		return update_tuple.get_added_element()->id <= update_tuple.at(update_tuple.get_size() - 2).id;
//	}
//};
//
//	struct Update_PR : BaseUpdate{
//		float rank;
//
//		Update_PR(int _target, float _rank) : BaseUpdate(_target), rank(_rank) {};
//
//		Update_PR() : BaseUpdate(0), rank(0.0) {}
//
//		std::string toString(){
//			return "(" + std::to_string(target) + ", " + std::to_string(rank) + ")";
//		}
//	}__attribute__((__packed__));
//
//
//	struct Vertex_PR : BaseVertex {
//		int degree;
//		float rank;
//		float sum;
//	}__attribute__((__packed__));
//
//	inline std::ostream & operator<<(std::ostream & strm, const Vertex_PR& vertex){
//		strm << "[" << vertex.id << "," << vertex.degree << ", " << vertex.rank << "]";
//		return strm;
//	}
//
//	void init(char* data, VertexId id) {
//		struct Vertex_PR * v = (struct Vertex_PR*)data;
//		v->degree = 0;
//		v->sum = 0;
//		v->rank = 1.0f;
//		v->id = id;
//	}
//
//	Update_PR * generate_one_update_init(Edge * e, Vertex_PR * v) {
//			Update_PR * update = new Update_PR(e->target, 1.0f / v->degree);
//			return update;
//		}
//
//	Update_PR * generate_one_update(Edge * e, Vertex_PR * v) {
//		Update_PR * update = new Update_PR(e->target, v->rank / v->degree);
//		return update;
//	}
//
//	void apply_one_update(Update_PR * update, Vertex_PR * dst_vertex) {
//		dst_vertex->sum += update->rank;
//		dst_vertex->rank = 0.15 + 0.85 * dst_vertex->sum;
//	}
//
//
//	bool filter_vertex(Vertex_PR & vertex){
////		std::cout << vertex << std::endl;
////		return vertex.rank > 10000 || vertex.rank < 1000;
//		return vertex.rank < 1200;
//	}
//
//
//	int main(int argc, char ** argv) {
//		if(argc != 7){
//			std::cerr << "Usage: input-graph, #partition-1, #iteration, out-file, #partition-2, maxsize" << std::endl;
//			return 0;
//		}
//
//		Engine e(std::string(argv[1]), atoi(argv[2]), 0);
//
//		//========================================================== page-ranking =======================================================
//		// get running time (wall time)
//		auto start_pagerank = std::chrono::high_resolution_clock::now();
//
//		std::cout << "--------------------Init Vertex--------------------" << std::endl;
//		e.init_vertex<Vertex_PR>(init);
//		std::cout << "--------------------Compute Degre--------------------" << std::endl;
//		e.compute_degree<Vertex_PR>();
//
//		int num_iters = atoi(argv[3]);
//
//		Scatter<Vertex_PR, Update_PR> scatter_phase(e);
//		Gather<Vertex_PR, Update_PR> gather_phase(e);
//
//		for(int i = 0; i < num_iters; i++) {
//			std::cout << "--------------------Iteration " << i << "--------------------" << std::endl;
//
//			Update_Stream in_stream;
//			if(i == 0) {
//				in_stream = scatter_phase.scatter_with_vertex(generate_one_update_init);
//			} else {
//				in_stream = scatter_phase.scatter_with_vertex(generate_one_update);
//			}
//
//			gather_phase.gather(in_stream, apply_one_update);
//
//			Global_Info::delete_upstream(in_stream, e);
//		}
//
//
//		auto end_pagerank = std::chrono::high_resolution_clock::now();
//		std::chrono::duration<double> diff_pagerank = end_pagerank - start_pagerank;
//		std::cout << "Finish page-ranking. Running time : " << diff_pagerank.count() << " s\n";
//
//
//		//========================================================== filtering =======================================================
//		// get running time (wall time)
//		auto start_filter = std::chrono::high_resolution_clock::now();
//
//		std::string out_file = argv[4];
//		scatter_phase.prune_graph(filter_vertex, out_file);
//
//		auto end_filter = std::chrono::high_resolution_clock::now();
//		std::chrono::duration<double> diff_filter = end_filter - start_filter;
//		std::cout << "Finish filtering. Running time : " << diff_filter.count() << " s\n";
//
//		//========================================================== clique-finding =======================================================
//
//		Engine ec(out_file, atoi(argv[5]), 1);
//		std::cout << Logger::generate_log_del(std::string("finish preprocessing"), 1) << std::endl;
//
//		// get running time (wall time)
//		auto start_clique = std::chrono::high_resolution_clock::now();
//
//		MC mPhase(ec, atoi(argv[6]));
//		Aggregation agg(ec, false);
//
//		//init: get the edges stream
//		std::cout << Logger::generate_log_del(std::string("init"), 1) << std::endl;
//		Update_Stream up_stream = mPhase.init_clique();
//		mPhase.printout_upstream(up_stream);
//
//		Update_Stream up_stream_new;
//		Update_Stream clique_stream;
//
//		for(unsigned int i = 0; i < mPhase.get_max_size() - 2; ++i){
//			std::cout << "\n\n" << Logger::generate_log_del(std::string("Iteration ") + std::to_string(i), 1) << std::endl;
//
//			//join on all keys
//			std::cout << "\n" << Logger::generate_log_del(std::string("joining"), 2) << std::endl;
//			up_stream_new = mPhase.join_all_keys_nonshuffle_clique(up_stream);
//			mPhase.delete_upstream(up_stream);
//			mPhase.printout_upstream(up_stream_new);
//
//			//collect cliques
//			std::cout << "\n" << Logger::generate_log_del(std::string("collecting"), 2) << std::endl;
//			clique_stream = agg.aggregate_filter_clique(up_stream_new, mPhase.get_sizeof_in_tuple());
//			mPhase.delete_upstream(up_stream_new);
//			mPhase.printout_upstream(clique_stream);
//
//			up_stream = clique_stream;
//		}
//		//clean remaining stream files
//		std::cout << std::endl;
//		mPhase.delete_upstream(up_stream);
//
//		//delete all generated files
//		std::cout << "\n\n" << Logger::generate_log_del(std::string("cleaning"), 1) << std::endl;
//		ec.clean_files();
//
//		auto end_clique = std::chrono::high_resolution_clock::now();
//		std::chrono::duration<double> diff_clique = end_clique - start_clique;
//		std::cout << "Finish clique-finding. Running time : " << diff_clique.count() << " s\n";
//
//	}
//
//
//
