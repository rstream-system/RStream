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
//	bool filter(std::vector<Element_In_Tuple> & update_tuple){
//		return false;
//	}
//
//	bool filter_aggregate(std::vector<Element_In_Tuple> & update_tuple, Aggregation_Stream & agg_stream){
//		return readAggregate(update_tuple, agg_stream) >= THRESHOLD;
//	}
//
//
//};
//
//int main(int argc, char **argv) {
//	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_new.txt", 3, 6);
//
//	MC mPhase(e);
//	Aggregation agg(e);
//
//	//init: get the edges stream
//	Update_Stream up_stream = mPhase.init();
//	Aggregation_Stream agg_stream = agg.aggregate(up_stream);
//	up_stream = mPhase.aggregate_filter(up_stream, agg_stream);
//
//	for(int i = 1; i <= MAXSIZE; ++i){
//		up_stream = mPhase.join_all_keys(up_stream);
//		agg_stream = agg.aggregate(up_stream);
//		up_stream = mPhase.aggregate_filter(up_stream, agg_stream);
//	}
//
//}


