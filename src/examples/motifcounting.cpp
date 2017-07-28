///*
// * motifcounting.cpp
// *
// *  Created on: Jul 7, 2017
// *      Author: icuzzq
// */
//
//#include "../core/engine.hpp"
//#include "../core/aggregation.hpp"
//
//#define MAXSIZE 3
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
//		return false;
//	}
//
//private:
//	static int get_num_vertices(std::vector<Element_In_Tuple> & update_tuple){
//		std::unordered_set<VertexId> set;
//		for(auto it = update_tuple.cbegin(); it != update_tuple.cend(); ++it){
//			set.insert((*it).vertex_id);
//		}
//		return set.size();
//	}
//
//};
//
//int main(int argc, char **argv) {
//	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_mining.txt", 3, 6);
////	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_mining.txt", 3, 1);
//	std::cout << generate_log_del(std::string("finish preprocessing"), 1) << std::endl;
//
//	MC mPhase(e);
//	Aggregation agg(e);
//
//	//init: get the edges stream
//	std::cout << generate_log_del(std::string("init-shuffling"), 1) << std::endl;
//	Update_Stream up_stream_shuffled = mPhase.init_shuffle_all_keys();
//
//	Update_Stream up_stream_non_shuffled;
//	Aggregation_Stream agg_stream;
//
//	int max_iterations = MAXSIZE * (MAXSIZE - 1) / 2;
//	for(int i = 1; i < max_iterations; ++i){
//		std::cout << "\n\n" << generate_log_del(std::string("Iteration ") + std::to_string(i), 1) << std::endl;
//
//		//join on all keys
//		std::cout << "\n" << generate_log_del(std::string("joining"), 2) << std::endl;
//		up_stream_non_shuffled = mPhase.join_mining(up_stream_shuffled);
//		//aggregate
//		std::cout << "\n" << generate_log_del(std::string("aggregating"), 2) << std::endl;
//		agg_stream = agg.aggregate(up_stream_non_shuffled, mPhase.sizeof_in_tuple);
//		//print out counts info
//		std::cout << "\n" << generate_log_del(std::string("printing"), 2) << std::endl;
//		agg.printout_aggstream(agg_stream);
//		//shuffle for next join
//		std::cout << "\n" << generate_log_del(std::string("shuffling"), 2) << std::endl;
//		up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled);
//	}
//
//}
//
//
//
//
//
