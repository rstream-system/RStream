

/*
 * cliquefinding.cpp
 *
 *  Created on: Jul 7, 2017
 *      Author: icuzzq
 */

#include "../core/aggregation.hpp"
#include "../utility/ResourceManager.hpp"


//#define MAXSIZE 3

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
//		return !isClique(update_tuple);
//	}

	bool filter_join(MTuple_join & update_tuple){
		return false;
	}

	bool filter_collect(MTuple & update_tuple){
		return false;
	}

	bool filter_join_clique(MTuple_join_simple& update_tuple){
		//TODO
		return update_tuple.get_added_element()->id <= update_tuple.at(update_tuple.get_size() - 2).id;
//		return false;
	}


};

void main_nonshuffle(int argc, char **argv) {
	Engine e(std::string(argv[1]), atoi(argv[2]), 1);
	std::cout << Logger::generate_log_del(std::string("finish preprocessing"), 1) << std::endl;

	ResourceManager rm;

	MC mPhase(e, atoi(argv[3]));
	Aggregation agg(e, false);

	//init: get the edges stream
	std::cout << Logger::generate_log_del(std::string("init"), 1) << std::endl;
	Update_Stream up_stream = mPhase.init_clique();
	mPhase.printout_upstream(up_stream);

	Update_Stream up_stream_new;
	Update_Stream clique_stream;

	for(unsigned int i = 0; i < mPhase.get_max_size() - 2; ++i){
		std::cout << "\n\n" << Logger::generate_log_del(std::string("Iteration ") + std::to_string(i), 1) << std::endl;

		//join on all keys
		std::cout << "\n" << Logger::generate_log_del(std::string("joining"), 2) << std::endl;
		up_stream_new = mPhase.join_all_keys_nonshuffle_clique(up_stream);
		mPhase.delete_upstream(up_stream);
		mPhase.printout_upstream(up_stream_new);

		//collect cliques
		std::cout << "\n" << Logger::generate_log_del(std::string("collecting"), 2) << std::endl;
		clique_stream = agg.aggregate_filter_clique(up_stream_new, mPhase.get_sizeof_in_tuple());
		mPhase.delete_upstream(up_stream_new);
		mPhase.printout_upstream(clique_stream);

//		//print out cliques
//		std::cout << "\n" << Logger::generate_log_del(std::string("printing"), 2) << std::endl;
//		mPhase.printout_upstream(clique_stream);

		up_stream = clique_stream;
	}
	//clean remaining stream files
	std::cout << std::endl;
	mPhase.delete_upstream(up_stream);

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
	main_nonshuffle(argc, argv);
}
