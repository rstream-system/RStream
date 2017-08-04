

/*
 * cliquefinding.cpp
 *
 *  Created on: Jul 7, 2017
 *      Author: icuzzq
 */

#include "../core/engine.hpp"
#include "../core/mining_phase.hpp"
#include "../utility/ResourceManager.hpp"


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
		return !isClique(update_tuple);
	}

private:

	bool isClique(std::vector<Element_In_Tuple> & update_tuple){
		unsigned int num_vertices = get_num_vertices(update_tuple);
		unsigned int num_edges = get_num_edges(update_tuple);
		return num_edges == num_vertices * (num_vertices - 1) / 2;
	}

	unsigned int get_num_edges(std::vector<Element_In_Tuple> & update_tuple){
		return update_tuple.size() - 1;
	}
};

int main_nonshuffle(int argc, char **argv) {
	Engine e(std::string(argv[1]), atoi(argv[2]), 1);
	std::cout << generate_log_del(std::string("finish preprocessing"), 1) << std::endl;

	ResourceManager rm;

	MC mPhase(e);

	//init: get the edges stream
	std::cout << generate_log_del(std::string("init"), 1) << std::endl;
	Update_Stream up_stream = mPhase.init();

	Update_Stream up_stream_new;
	Update_Stream clique_stream;

	int max_iterations = MAXSIZE * (MAXSIZE - 1) / 2;
	for(int i = 1; i < max_iterations; ++i){
		std::cout << "\n\n" << generate_log_del(std::string("Iteration ") + std::to_string(i), 1) << std::endl;

		//join on all keys
		std::cout << "\n" << generate_log_del(std::string("joining"), 2) << std::endl;
		up_stream_new = mPhase.join_all_keys_nonshuffle(up_stream);
		mPhase.delete_upstream(up_stream);
		//collect cliques
		std::cout << "\n" << generate_log_del(std::string("collecting"), 2) << std::endl;
		clique_stream = mPhase.collect(up_stream_new);
		//print out cliques
		std::cout << "\n" << generate_log_del(std::string("printing"), 2) << std::endl;
		mPhase.printout_upstream(clique_stream);
		mPhase.delete_upstream(clique_stream);

		up_stream = up_stream_new;
	}
	//clean remaining stream files
	std::cout << std::endl;
	mPhase.delete_upstream(up_stream);

	//delete all generated files
	std::cout << "\n\n" << generate_log_del(std::string("cleaning"), 1) << std::endl;
	e.clean_files();

	//print out resource usage
	std::cout << "\n\n";
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << rm.result() << std::endl;
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << "\n\n";

	return 0;
}

int main_shuffle(int argc, char **argv) {
	Engine e(std::string(argv[1]), atoi(argv[2]), 1);
	std::cout << generate_log_del(std::string("finish preprocessing"), 1) << std::endl;

	ResourceManager rm;

	MC mPhase(e);

	//init: get the edges stream
	std::cout << generate_log_del(std::string("init-shuffling"), 1) << std::endl;
	Update_Stream up_stream_shuffled = mPhase.init_shuffle_all_keys();

	Update_Stream up_stream_non_shuffled;
	Update_Stream clique_stream;

	int max_iterations = MAXSIZE * (MAXSIZE - 1) / 2;
	for(int i = 1; i < max_iterations; ++i){
		std::cout << "\n\n" << generate_log_del(std::string("Iteration ") + std::to_string(i), 1) << std::endl;

		//join on all keys
		std::cout << "\n" << generate_log_del(std::string("joining"), 2) << std::endl;
		up_stream_non_shuffled = mPhase.join_mining(up_stream_shuffled);
		mPhase.delete_upstream(up_stream_shuffled);
		//collect cliques
		std::cout << "\n" << generate_log_del(std::string("collecting"), 2) << std::endl;
		clique_stream = mPhase.collect(up_stream_non_shuffled);
		//print out cliques
		std::cout << "\n" << generate_log_del(std::string("printing"), 2) << std::endl;
		mPhase.printout_upstream(clique_stream);
		mPhase.delete_upstream(clique_stream);
		//shuffle for next join
		std::cout << "\n" << generate_log_del(std::string("shuffling"), 2) << std::endl;
		up_stream_shuffled = mPhase.shuffle_all_keys(up_stream_non_shuffled);
		mPhase.delete_upstream(up_stream_non_shuffled);
	}
	//clean remaining stream files
	std::cout << std::endl;
	mPhase.delete_upstream(up_stream_shuffled);

	//delete all generated files
	std::cout << "\n\n" << generate_log_del(std::string("cleaning"), 1) << std::endl;
	e.clean_files();

	//print out resource usage
	std::cout << "\n\n";
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << rm.result() << std::endl;
	std::cout << "------------------------------ resource usage ------------------------------" << std::endl;
	std::cout << "\n\n";

	return 0;
}

int main(int argc, char **argv){
	main_nonshuffle(argc, argv);
//	main_shuffle(argc, argv);
}
