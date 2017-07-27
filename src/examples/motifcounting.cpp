/*
 * motifcounting.cpp
 *
 *  Created on: Jul 7, 2017
 *      Author: icuzzq
 */

#include "../core/engine.hpp"
#include "../core/aggregation.hpp"

#define MAXSIZE 3

using namespace RStream;


class MC : public MPhase {
public:
	MC(Engine & e) : MPhase(e){};
	~MC() {};

	bool filter_join(std::vector<Element_In_Tuple> & update_tuple){
		return get_num_vertices(update_tuple) > MAXSIZE;
	}

	bool filter_collect(std::vector<Element_In_Tuple> & update_tuple){
		return false;
	}

private:
	static int get_num_vertices(std::vector<Element_In_Tuple> & update_tuple){
		std::unordered_set<VertexId> set;
		for(auto it = update_tuple.cbegin(); it != update_tuple.cend(); ++it){
			set.insert((*it).vertex_id);
		}
		return set.size();
	}

};

int main(int argc, char **argv) {
//	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_mining.txt", 3, 6);
	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_mining.txt", 3, 1);

	MC mPhase(e);
	Aggregation agg(e);

	//init: get the edges stream
	Update_Stream up_stream_shuffled = mPhase.init_shuffle_all_keys();
	std::cout << "finish init-shuffling." << std::endl;

	Update_Stream up_stream_non_shuffled;
	Aggregation_Stream agg_stream;

	int max_iterations = MAXSIZE * (MAXSIZE - 1) / 2;
	for(int i = 1; i < max_iterations; ++i){
		std::cout << "at iteration " << i << std::endl;
		//join on all keys
		up_stream_non_shuffled = mPhase.join_mining(up_stream_shuffled);
		std::cout << "finish joining." << std::endl;
		//aggregate
		agg_stream = agg.aggregate(up_stream_non_shuffled, mPhase.sizeof_in_tuple);
		std::cout << "finish aggregating." << std::endl;
		//print out counts info
		agg.printout_aggstream(agg_stream);
		std::cout << "finish printing." << std::endl;
		//shuffle for next join
		up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled);
		std::cout << "finish shuffling." << std::endl;
	}

}





