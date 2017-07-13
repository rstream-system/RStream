/*
 * cliquefinding.cpp
 *
 *  Created on: Jul 7, 2017
 *      Author: icuzzq
 */

#include "../core/engine.hpp"
#include "../core/mining_phase.hpp"

#define MAXSIZE 5

using namespace RStream;


class MC : public MPhase {
public:
	MC(Engine & e) : MPhase(e){};
	~MC() {};

	bool filter_join(std::vector<Element_In_Tuple> & update_tuple){
		return get_num_vertices(update_tuple) > MAXSIZE;
	}

	bool filter_collect(std::vector<Element_In_Tuple> & update_tuple){
		return !isClique(update_tuple);
	}

private:
	static unsigned get_num_vertices(std::vector<Element_In_Tuple> & update_tuple){
		std::unordered_set<VertexId> set;
		for(auto it = update_tuple.cbegin(); it != update_tuple.cend(); ++it){
			set.insert((*it).vertex_id);
		}
		return set.size();
	}

	static bool isClique(std::vector<Element_In_Tuple> & update_tuple){
		unsigned num_vertices = get_num_vertices(update_tuple);
		return update_tuple.size() == num_vertices * (num_vertices - 1) / 2;
	}

};

int main(int argc, char **argv) {
	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_new.txt", 3, 6);

	MC mPhase(e);

	//init: get the edges stream
	Update_Stream up_stream_shuffled = mPhase.init_shuffle_all_keys();
	Update_Stream up_stream_non_shuffled;
	Update_Stream clique_stream;

	int max_iterations = MAXSIZE * (MAXSIZE - 1) / 2;
	for(int i = 1; i < max_iterations; ++i){
		up_stream_non_shuffled = mPhase.join_mining(up_stream_shuffled);
		clique_stream = mPhase.collect(up_stream_non_shuffled);
		mPhase.printout_upstream(clique_stream);

		up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled);
	}

}


