/*
 * fsm.cpp
 *
 *  Created on: Jul 7, 2017
 *      Author: icuzzq
 */

#include "../core/engine.hpp"
#include "../core/aggregation.hpp"
#include "../utility/ResourceManager.hpp"

//#define MAXSIZE 4
//#define THRESHOLD 300

using namespace RStream;


class MC : public MPhase {
public:
	MC(Engine & e, unsigned int maxsize) : MPhase(e, maxsize){};
	~MC() {};

//	bool filter_join(std::vector<Element_In_Tuple> & update_tuple){
//		return get_num_vertices(update_tuple) > max_size;
//	}
//
//	bool filter_collect(std::vector<Element_In_Tuple> & update_tuple){
//		return false;
//	}

	bool filter_join(MTuple_join & update_tuple){
		return get_num_vertices(update_tuple) > max_size;
	}

	bool filter_collect(MTuple & update_tuple){
		return false;
	}

	bool filter_join_clique(MTuple_join_simple & update_tuple){
		return false;
	}

};


void main_nonshuffle(int argc, char **argv) {
	Engine e(std::string(argv[1]), atoi(argv[2]), 1);
	std::cout << Logger::generate_log_del(std::string("finish preprocessing"), 1) << std::endl;

	ResourceManager rm;

	MC mPhase(e, atoi(argv[3]));
	Aggregation agg(e, true);

	int threshold = atoi(argv[4]);

	//get the non-shuffled edges stream
	std::cout << Logger::generate_log_del(std::string("init"), 1) << std::endl;
	Update_Stream up_stream = mPhase.init();
	//aggregate
	std::cout << "\n" << Logger::generate_log_del(std::string("aggregating"), 2) << std::endl;
	Aggregation_Stream agg_stream = agg.aggregate(up_stream, mPhase.get_sizeof_in_tuple());
	//filter infrequent edges
	std::cout << "\n" << Logger::generate_log_del(std::string("filtering"), 2) << std::endl;
	Update_Stream up_stream_filtered = agg.aggregate_filter(up_stream, agg_stream, mPhase.get_sizeof_in_tuple(), threshold);
	mPhase.delete_upstream(up_stream);
	agg.delete_aggstream(agg_stream);

	unsigned int max_iterations = mPhase.get_max_size() * (mPhase.get_max_size() - 1) / 2;
	for(unsigned int i = 1; i <= max_iterations; ++i){
		std::cout << "\n\n" << Logger::generate_log_del(std::string("Iteration ") + std::to_string(i), 1) << std::endl;

		//join on all keys
		std::cout << "\n" << Logger::generate_log_del(std::string("joining"), 2) << std::endl;
		up_stream = mPhase.join_all_keys_nonshuffle(up_stream_filtered);
		mPhase.delete_upstream(up_stream_filtered);
		//aggregate
		std::cout << "\n" << Logger::generate_log_del(std::string("aggregating"), 2) << std::endl;
		agg_stream = agg.aggregate(up_stream, mPhase.get_sizeof_in_tuple());
		//print out frequent patterns
		std::cout << "\n" << Logger::generate_log_del(std::string("printing"), 2) << std::endl;
		agg.printout_aggstream(agg_stream);
		//filter infrequent subgraphs
		std::cout << "\n" << Logger::generate_log_del(std::string("filtering"), 2) << std::endl;
		up_stream_filtered = agg.aggregate_filter(up_stream, agg_stream, mPhase.get_sizeof_in_tuple(), threshold);
		mPhase.delete_upstream(up_stream);
		agg.delete_aggstream(agg_stream);
	}
	//clean remaining stream files
	std::cout << std::endl;
	mPhase.delete_upstream(up_stream_filtered);

	//delete all generated files
	std::cout << "\n\n" << Logger::generate_log_del(std::string("cleaning"), 1) << std::endl;
	e.clean_files();

	//print out resource usage
	std::cout << "\n\n";
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << rm.result() << std::endl;
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << "\n\n";

}

void main_shuffle(int argc, char **argv) {
	Engine e(std::string(argv[1]), atoi(argv[2]), 1);
	std::cout << Logger::generate_log_del(std::string("finish preprocessing"), 1) << std::endl;

	ResourceManager rm;

	MC mPhase(e, atoi(argv[3]));
	Aggregation agg(e, true);

	int threshold = atoi(argv[4]);

	//get the non-shuffled edges stream
	std::cout << Logger::generate_log_del(std::string("init"), 1) << std::endl;
	Update_Stream up_stream_non_shuffled = mPhase.init();
	//aggregate
	std::cout << "\n" << Logger::generate_log_del(std::string("aggregating"), 2) << std::endl;
	Aggregation_Stream agg_stream = agg.aggregate(up_stream_non_shuffled, mPhase.get_sizeof_in_tuple());
	//print out frequent patterns
	std::cout << "\n" << Logger::generate_log_del(std::string("printing"), 2) << std::endl;
	agg.printout_aggstream(agg_stream);
	//filter infrequent edges
	std::cout << "\n" << Logger::generate_log_del(std::string("filtering"), 2) << std::endl;
	Update_Stream up_stream_non_shuffled_filtered = agg.aggregate_filter(up_stream_non_shuffled, agg_stream, mPhase.get_sizeof_in_tuple(), threshold);
	mPhase.delete_upstream(up_stream_non_shuffled);
	agg.delete_aggstream(agg_stream);
	//shuffle edges
	std::cout << "\n" << Logger::generate_log_del(std::string("shuffling"), 2) << std::endl;
	Update_Stream up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled_filtered);
	mPhase.delete_upstream(up_stream_non_shuffled_filtered);

	unsigned int max_iterations = mPhase.get_max_size() * (mPhase.get_max_size() - 1) / 2;
	for(unsigned int i = 1; i <= max_iterations; ++i){
		std::cout << "\n\n" << Logger::generate_log_del(std::string("Iteration ") + std::to_string(i), 1) << std::endl;

		//join on all keys
		std::cout << "\n" << Logger::generate_log_del(std::string("joining"), 2) << std::endl;
		up_stream_non_shuffled = mPhase.join_mining(up_stream_shuffled);
		mPhase.delete_upstream(up_stream_shuffled);
		//aggregate
		std::cout << "\n" << Logger::generate_log_del(std::string("aggregating"), 2) << std::endl;
		agg_stream = agg.aggregate(up_stream_non_shuffled, mPhase.get_sizeof_in_tuple());
		//print out frequent patterns
		std::cout << "\n" << Logger::generate_log_del(std::string("printing"), 2) << std::endl;
		agg.printout_aggstream(agg_stream);
		//filter infrequent subgraphs
		std::cout << "\n" << Logger::generate_log_del(std::string("filtering"), 2) << std::endl;
		up_stream_non_shuffled_filtered = agg.aggregate_filter(up_stream_non_shuffled, agg_stream, mPhase.get_sizeof_in_tuple(), threshold);
		mPhase.delete_upstream(up_stream_non_shuffled);
		agg.delete_aggstream(agg_stream);
		//shuffle
		std::cout << "\n" << Logger::generate_log_del(std::string("shuffling"), 2) << std::endl;
		up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled_filtered);
		mPhase.delete_upstream(up_stream_non_shuffled_filtered);
	}
	//clean remaining stream files
	std::cout << std::endl;
	mPhase.delete_upstream(up_stream_shuffled);

	//delete all generated files
	std::cout << "\n\n" << Logger::generate_log_del(std::string("cleaning"), 1) << std::endl;
	e.clean_files();

	//print out resource usage
	std::cout << "\n\n";
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << rm.result() << std::endl;
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << "\n\n";

}

int main(int argc, char **argv){
//	main_shuffle(argc, argv);
	main_nonshuffle(argc, argv);
}
