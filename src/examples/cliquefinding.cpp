
//
///*
// * cliquefinding.cpp
// *
// *  Created on: Jul 7, 2017
// *      Author: icuzzq
// */
//
//#include "../core/engine.hpp"
//#include "../core/mining_phase.hpp"
//
//#define MAXSIZE 4
//
//using namespace RStream;
//
//
//class MC : public MPhase {
//public:
//	MC(Engine & e) : MPhase(e){};
//	~MC() {};
//
//	bool filter_join(std::vector<Element_In_Tuple> & update_tuple){
//		return get_num_vertices(update_tuple) > MAXSIZE;
//	}
//
//	bool filter_collect(std::vector<Element_In_Tuple> & update_tuple){
//		return !isClique(update_tuple);
//	}
//
//private:
//
//	bool isClique(std::vector<Element_In_Tuple> & update_tuple){
//		unsigned int num_vertices = get_num_vertices(update_tuple);
//		unsigned int num_edges = get_num_edges(update_tuple);
//		return num_edges == num_vertices * (num_vertices - 1) / 2;
//	}
//
//	unsigned int get_num_vertices(std::vector<Element_In_Tuple> & update_tuple){
//		std::unordered_set<VertexId> set;
//		for(auto it = update_tuple.cbegin(); it != update_tuple.cend(); ++it){
//			set.insert((*it).vertex_id);
//		}
//		return set.size();
//	}
//
//	unsigned int get_num_edges(std::vector<Element_In_Tuple> & update_tuple){
//		return update_tuple.size() - 1;
//	}
//};
//
//int main(int argc, char **argv) {
//	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_mining.txt", 3, 6);
//	std::cout << "finish preprocessing." << std::endl;
//
//	MC mPhase(e);
//
//	//init: get the edges stream
//	Update_Stream up_stream_shuffled = mPhase.init_shuffle_all_keys();
//	std::cout << "finish init-shuffling." << std::endl;
//
//	Update_Stream up_stream_non_shuffled;
//	Update_Stream clique_stream;
//
//	int max_iterations = MAXSIZE * (MAXSIZE - 1) / 2;
//	for(int i = 1; i < max_iterations; ++i){
//		std::cout << "at iteration " << i << std::endl;
//		//join on all keys
//		up_stream_non_shuffled = mPhase.join_mining(up_stream_shuffled);
//		std::cout << "finish joining." << std::endl;
//		//collect cliques
//		clique_stream = mPhase.collect(up_stream_non_shuffled);
//		std::cout << "finish collecting." << std::endl;
//		//print out cliques
//		mPhase.printout_upstream(clique_stream);
//		std::cout << "finish printing." << std::endl;
//		//shuffle for next join
//		up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled);
//		std::cout << "finish shuffling.\n\n" << std::endl;
//	}
//
//}
//
