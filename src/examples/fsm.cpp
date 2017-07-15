/*
 * fsm.cpp
 *
 *  Created on: Jul 7, 2017
 *      Author: icuzzq
 */

//#include "../core/engine.hpp"
//#include "../core/mining_phase.hpp"
//
//#define MAXSIZE 3
//#define THRESHOLD 300
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
//		return false;
//	}
//
//	bool filter_collect(std::vector<Element_In_Tuple> & update_tuple){
//		return false;
//	}
//
//
//
//};

//class AG : public Aggregation {
//public:
//	AG(Engine & e) : MPhase(e){};
//	~AG() {};
//
//	bool filter_aggregate(std::vector<Element_In_Tuple> & update_tuple, Aggregation_Stream & agg_stream){
//		return readAggregate(update_tuple, agg_stream) >= THRESHOLD;
//	}
//};

//int main(int argc, char **argv) {
//	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_new.txt", 3, 6);
//
//	MC mPhase(e);
//	Aggregation agg(e);
//
//	//get the non-shuffled edges stream
//	Update_Stream up_stream_non_shuffled = mPhase.init();
//	//aggregate
//	Aggregation_Stream agg_stream = agg.aggregate(up_stream_non_shuffled, mPhase.sizeof_in_tuple);
//	//filter infrequent edges
//	up_stream_non_shuffled = agg.aggregate_filter(up_stream_non_shuffled, agg_stream);
//	//shuffle edges
//	Update_Stream up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled);
//
//	for(int i = 1; i <= MAXSIZE; ++i){
//		//join on all keys
//		up_stream_non_shuffled = mPhase.join_mining(up_stream_shuffled);
//		//aggregate
//		agg_stream = agg.aggregate(up_stream_non_shuffled, mPhase.sizeof_in_tuple);
//		//print out frequent patterns
//		agg.printout_aggstream(agg_stream);
//		//filter infrequent subgraphs
//		up_stream_non_shuffled = agg.aggregate_filter(up_stream_non_shuffled, agg_stream);
//		//shuffle
//		up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled);
//	}
//
//}
