/*
 * motifcounting.cpp
 *
 *  Created on: Jul 7, 2017
 *      Author: icuzzq
 */

#include "../core/engine.hpp"
#include "../core/mining_phase.hpp"

#define MAXSIZE 3

using namespace RStream;


class MC : public MPhase {
public:
	MC(Engine & e) : MPhase(e){};
	~MC() {};

	bool filter(std::vector<Element_In_Tuple> & update_tuple){
		//TODO
		return get_num_vertices(update_tuple) > MAXSIZE;
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
	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_new.txt", 3, 6);

	MC mPhase(e);
	Aggregation agg(e);

	//init: get the edges stream
	Update_Stream up_stream = mPhase.init();

	int max_iterations = MAXSIZE * (MAXSIZE - 1) / 2;
	for(int i = 1; i <= max_iterations; ++i){
		up_stream = mPhase.join_all_keys(up_stream);
		Aggregation_Stream agg_stream = agg.aggregate(up_stream);
	}

}





