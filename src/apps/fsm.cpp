/*
 * fsm.cpp
 *
 *  Created on: Jul 7, 2017
 *      Author: icuzzq
 */

#include "../core/engine.hpp"
#include "../core/aggregation.hpp"
#include "../utility/ResourceManager.hpp"

#define MAXSIZE 3
#define THRESHOLD 1000

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
	static int get_num_vertices(std::vector<Element_In_Tuple> & update_tuple) {
		std::unordered_set<VertexId> set;
		for (auto it = update_tuple.cbegin(); it != update_tuple.cend(); ++it) {
			set.insert((*it).vertex_id);
		}
		return set.size();
	}


};


int main(int argc, char **argv) {
	Engine e("/home/icuzzq/Workspace/git/RStream/input/citeseer.graph", 3, 1);
	std::cout << generate_log_del(std::string("finish preprocessing"), 1) << std::endl;

	ResourceManager rm;

	MC mPhase(e);
	Aggregation agg(e);

	//get the non-shuffled edges stream
	std::cout << generate_log_del(std::string("init"), 1) << std::endl;
	Update_Stream up_stream_non_shuffled = mPhase.init();
	//aggregate
	std::cout << "\n" << generate_log_del(std::string("aggregating"), 2) << std::endl;
	Aggregation_Stream agg_stream = agg.aggregate(up_stream_non_shuffled, mPhase.sizeof_in_tuple);
	//filter infrequent edges
	std::cout << "\n" << generate_log_del(std::string("filtering"), 2) << std::endl;
	up_stream_non_shuffled = agg.aggregate_filter(up_stream_non_shuffled, agg_stream, mPhase.sizeof_in_tuple, THRESHOLD);
	//shuffle edges
	std::cout << "\n" << generate_log_del(std::string("shuffling"), 2) << std::endl;
	Update_Stream up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled);

	for(int i = 1; i <= MAXSIZE; ++i){
		std::cout << "\n\n" << generate_log_del(std::string("Iteration ") + std::to_string(i), 1) << std::endl;

		//join on all keys
		std::cout << "\n" << generate_log_del(std::string("joining"), 2) << std::endl;
		up_stream_non_shuffled = mPhase.join_mining(up_stream_shuffled);
		//aggregate
		std::cout << "\n" << generate_log_del(std::string("aggregating"), 2) << std::endl;
		agg_stream = agg.aggregate(up_stream_non_shuffled, mPhase.sizeof_in_tuple);
		//print out frequent patterns
		std::cout << "\n" << generate_log_del(std::string("printing"), 2) << std::endl;
		agg.printout_aggstream(agg_stream);
		//filter infrequent subgraphs
		std::cout << "\n" << generate_log_del(std::string("filtering"), 2) << std::endl;
		up_stream_non_shuffled = agg.aggregate_filter(up_stream_non_shuffled, agg_stream, mPhase.sizeof_in_tuple, THRESHOLD);
		//shuffle
		std::cout << "\n" << generate_log_del(std::string("shuffling"), 2) << std::endl;
		up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled);
	}

	//print out resource usage
	std::cout << "\n\n";
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << rm.result() << std::endl;
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << "\n\n";

}
