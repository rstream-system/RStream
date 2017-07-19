/*
 * aggregation.hpp
 *
 *  Created on: Jul 7, 2017
 *      Author: kai
 */

#ifndef CORE_AGGREGATION_HPP_
#define CORE_AGGREGATION_HPP_

#include "mining_phase.hpp"

typedef std::vector<Element_In_Tuple> Tuple;
typedef std::vector<Element_In_Tuple> Quick_Pattern;
//typedef std::vector<Element_In_Tuple> Canonical_Graph;

namespace RStream {
	class Aggregation {
		const Engine & context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

	public:

		virtual bool filter_aggregate(std::vector<Element_In_Tuple> & update_tuple, std::unordered_map<Canonical_Graph, int>& map) = 0;



		Aggregation(Engine & e) : context(e) {
			atomic_num_producers = 0;
			atomic_partition_id = 0;
			atomic_partition_number = context.num_partitions;
		}

		~Aggregation() {}

		/*
		 * do aggregation for update stream
		 * @param: in update stream
		 * @return: aggregation stream
		 * */
		Aggregation_Stream aggregate(Update_Stream in_update_stream, int sizeof_in_tuple) {
			Aggregation_Stream stream_local = aggregate_local(in_update_stream, sizeof_in_tuple);
			return aggregate_global(stream_local, sizeof_in_tuple);
		}


		Update_Stream aggregate_filter(Update_Stream up_stream, Aggregation_Stream agg_stream, int sizeof_in_tuple){
			Update_Stream up_stream_shuffled_on_canonical = shuffle_upstream_canonicalgraph(up_stream, sizeof_in_tuple);
			return aggregate_filter_local(up_stream_shuffled_on_canonical, agg_stream, sizeof_in_tuple);
		}

		void printout_aggstream(Aggregation_Stream agg_stream){

		}



	private:

		Update_Stream shuffle_upstream_canonicalgraph(Update_Stream in_update_stream, int sizeof_in_tuple){
			Update_Stream update_c = Engine::aggregation_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// output should be a pair of <tuples, count>
			// tuples -- canonical pattern
			// count -- counter for patterns
			int sizeof_output = get_out_size(sizeof_in_tuple);
			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_output);

			// exec threads will do aggregate and push result patterns into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->shuffle_on_canonical_producer(in_update_stream, buffers_for_shuffle, task_queue, sizeof_in_tuple); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&MPhase::consumer, this, update_c, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			return update_c;
		}

		void shuffle_on_canonical_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue, int sizeof_in_tuple) {
			atomic_num_producers++;
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)) {
				std::cout << partition_id << std::endl;

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				int streaming_counter = update_file_size / IO_SIZE + 1;

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - IO_SIZE * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE;

					assert(valid_io_size % sizeof_in_tuple == 0);

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// streaming tuples in, do aggregation
					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_tuple) {
						// get an in_update_tuple
						std::vector<Element_In_Tuple> in_update_tuple;
						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

						shuffle_on_canonical(in_update_tuple, buffers_for_shuffle);
					}

				}

				free(update_local_buf);
				close(fd_update);

			}
			atomic_num_producers--;

		}

		void shuffle_on_canonical(std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining ** buffers_for_shuffle){
			std::vector<Element_In_Tuple> quick_pattern(in_update_tuple.size());
			// turn tuple to quick pattern
			pattern::turn_quick_pattern_pure(in_update_tuple, quick_pattern);
			Canonical_Graph* cf = pattern::turn_canonical_graph(quick_pattern, false);

			int hash = cf->get_hash();
			int index = get_global_bucket_index(hash);
			delete cf;

			//get tuple
			insert_tuple_to_buffer(index, in_update_tuple, buffers_for_shuffle);
		}

		void insert_tuple_to_buffer(int partition_id, std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
			char* out_update = reinterpret_cast<char*>(in_update_tuple.data());
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			global_buf->insert(out_update);
		}


		Update_Stream aggregate_filter_local(Update_Stream up_stream_shuffled_on_canonical, Aggregation_Stream agg_stream, int sizeof_in_tuple){
			// each element in the tuple is 2 ints
			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_in_tuple);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->aggregate_filter_local_producer(up_stream_shuffled_on_canonical, buffers_for_shuffle, task_queue, sizeof_in_tuple, agg_stream); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&MPhase::consumer, this, update_c, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			return update_c;
		}

		void aggregate_filter_local_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue, int sizeof_in_tuple, Aggregation_Stream agg_stream){
			atomic_num_producers++;
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				std::cout << partition_id << std::endl;

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				int fd_agg = open((context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(agg_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0 && fd_agg > 0 );

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);
				long agg_file_size = io_manager::get_filesize(fd_agg);

				print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + "\n");

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				int streaming_counter = update_file_size / IO_SIZE + 1;

				// aggs are fully loaded into memory
				char * agg_local_buf = new char[agg_file_size];
				io_manager::read_from_file(fd_agg, agg_local_buf, agg_file_size, 0);


				//build hashmap for aggregation tuples
				std::unordered_map<Canonical_Graph, int> map;
				build_aggmap(map, agg_local_buf, agg_file_size);

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - IO_SIZE * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE;

					assert(valid_io_size % sizeof_in_tuple == 0);

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// streaming updates in, do hash join
					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_tuple) {
						// get an in_update_tuple
						std::vector<Element_In_Tuple> in_update_tuple;
						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

						if(!filter_aggregate(in_update_tuple, map)){
							insert_tuple_to_buffer(partition_id, in_update_tuple, buffers_for_shuffle);
						}

					}
				}

				free(update_local_buf);
				delete[] agg_local_buf;

				close(fd_update);
				close(fd_edge);
			}

			atomic_num_producers--;
		}

		void build_aggmap(std::unordered_map<Canonical_Graph, int>& map, char* agg_local_buf, int agg_file_size){

		}

		void get_an_in_update(char * update_local_buf, std::vector<Element_In_Tuple> & tuple, int sizeof_in_tuple) {
			for(int index = 0; index < sizeof_in_tuple; index += sizeof(Element_In_Tuple)) {
				Element_In_Tuple & element = *(Element_In_Tuple*)(update_local_buf + index);
				tuple.push_back(element);
			}
		}

		Aggregation_Stream aggregate_local(Update_Stream in_update_stream, int sizeof_in_tuple) {
			Aggregation_Stream aggreg_c = Engine::aggregation_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// output should be a pair of <tuples, count>
			// tuples -- canonical pattern
			// count -- counter for patterns
			int sizeof_output = get_out_size(sizeof_in_tuple);
			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_output);

			// exec threads will do aggregate and push result patterns into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->aggregate_local_producer(in_update_stream, buffers_for_shuffle, task_queue, sizeof_in_tuple); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&Aggregation::aggregate_consumer, this, aggreg_c, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			return aggreg_c;

		}

		int get_out_size(int sizeof_in_tuple){
			//TODO: sizeof(int) + ?
			return sizeof(int);
		}

		Aggregation_Stream aggregate_global(Aggregation_Stream in_agg_stream, int sizeof_in_tuple) {
			Aggregation_Stream aggreg_c = Engine::aggregation_count++;

			//TODO
			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// output should be a pair of <tuples, count>
			// tuples -- canonical pattern
			// count -- counter for patterns

			// exec threads will do aggregate and push result patterns into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads + context.num_write_threads; i++)
				exec_threads.push_back( std::thread([=] { this->aggregate_global_per_thread(in_agg_stream, task_queue, sizeof_in_tuple, aggreg_c); } ));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			delete task_queue;

			return aggreg_c;
		}

		void aggregate_global_per_thread(Aggregation_Stream in_agg_stream, concurrent_queue<int> * task_queue, int sizeof_in_tuple, Aggregation_Stream out_agg_stream) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)) {
				std::cout << partition_id << std::endl;

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(in_agg_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);

				// streaming updates
				char * agg_local_buf = (char *)malloc(update_file_size);

				// read tuples in, do aggregation
				std::vector<std::pair<Canonical_Graph*, int>> tmp_aggregation;
				for(long pos = 0; pos < update_file_size; pos += sizeof_in_tuple) {
					// get an in_update_tuple
					std::pair<Canonical_Graph*, int> in_agg_pair;
					get_an_in_agg_pair(agg_local_buf + pos, in_agg_pair, sizeof_in_tuple);
					tmp_aggregation.push_back(in_agg_pair);
				}

				// for all the canonical graphs, do local aggregation
				std::vector<std::pair<Canonical_Graph*, int>> canonical_graphs_aggregation;
				local_aggregate(tmp_aggregation, canonical_graphs_aggregation);

				const char * file_name = (context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(out_agg_stream)).c_str();
				write_canonical_aggregation(canonical_graphs_aggregation, file_name);

				free(agg_local_buf);
				close(fd_update);

			}
		}

		void get_an_in_agg_pair(char * update_local_buf, std::pair<Canonical_Graph*, int> & agg_pair, int sizeof_in_tuple){

		}

		void write_canonical_aggregation(std::vector<std::pair<Canonical_Graph*, int>>& canonical_graphs_aggregation, const char* file_name){

		}


		void aggregate_local_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue, int sizeof_in_tuple) {
			atomic_num_producers++;
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)) {
				std::cout << partition_id << std::endl;

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				int streaming_counter = update_file_size / IO_SIZE + 1;

				long valid_io_size = 0;
				long offset = 0;

				std::vector<std::pair<Quick_Pattern, int>> quick_patterns_aggregation;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - IO_SIZE * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE;

					assert(valid_io_size % sizeof_in_tuple == 0);

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					std::vector<std::pair<Quick_Pattern, int>> quick_patterns;

					// streaming tuples in, do aggregation
					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_tuple) {
						// get an in_update_tuple
						std::vector<Element_In_Tuple> in_update_tuple;
						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

						// turn tuple to quick pattern
						pattern::turn_quick_pattern_sideffect(in_update_tuple);
						quick_patterns.push_back(std::make_pair(in_update_tuple, 1));
					}

					// do aggregation on quick patterns
					local_aggregate(quick_patterns, quick_patterns_aggregation);

				}

				// for all the aggregated quick patterns, turn to canocail graphs
				std::vector<std::pair<Canonical_Graph*, int>> canonical_graphs;
				map_canonical(quick_patterns_aggregation, canonical_graphs);

				// for all the canonical graphs, do local aggregation
				std::vector<std::pair<Canonical_Graph*, int>> canonical_graphs_aggregation;
				local_aggregate(canonical_graphs, canonical_graphs_aggregation);

				// for each canonical graph, do map reduce, shuffle to corresponding buckets
				shuffle_canonical_aggregation(canonical_graphs_aggregation, buffers_for_shuffle);

				//clean memory for bliss::AbstractGraph occurred in two vectors
				clean(canonical_graphs, canonical_graphs_aggregation);

				delete[] update_local_buf;
				close(fd_update);

			}
			atomic_num_producers--;
		}

		void shuffle_canonical_aggregation(std::vector<std::pair<Canonical_Graph*, int>>& canonical_graphs_aggregation, global_buffer_for_mining ** buffers_for_shuffle){
			char* out_cg = nullptr;
			for(unsigned int i = 0; i < canonical_graphs_aggregation.size(); i++) {
				std::pair<Canonical_Graph *, int> one_canonical_graph = canonical_graphs_aggregation.at(i);
				int hash = one_canonical_graph.first->get_hash();
				int index = get_global_bucket_index(hash);

				//get out_cf
				//TODO

				// TODO: insert
				global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, index);
				global_buf->insert(out_cg);
			}
		}

		void clean(std::vector<std::pair<Canonical_Graph*, int>>& canonical_graphs, std::vector<std::pair<Canonical_Graph*, int>>& canonical_graphs_aggregation){

		}

		// each writer thread generates a join_consumer
		void aggregate_consumer(Aggregation_Stream aggregation_stream, global_buffer_for_mining ** buffers_for_shuffle) {
			while(atomic_num_producers != 0) {
				int i = (atomic_partition_id++) % context.num_partitions ;

				const char * file_name = (context.filename + "." + std::to_string(i) + ".aggregate_stream_" + std::to_string(aggregation_stream)).c_str();
				global_buffer_for_mining* g_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;

				if(i >= 0){

					const char * file_name = (context.filename + "." + std::to_string(i) + ".aggregate_stream_" + std::to_string(aggregation_stream)).c_str();
					global_buffer_for_mining* g_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, i);
					g_buf->flush_end(file_name, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}

//		void get_an_in_update(char * update_local_buf, std::vector<Element_In_Tuple> & tuple, int sizeof_in_tuple) {
//			for(int index = 0; index < sizeof_in_tuple; index += sizeof(Element_In_Tuple)) {
//				Element_In_Tuple & element = *(Element_In_Tuple*)(update_local_buf + index);
//				tuple.push_back(element);
//			}
//		}

		void map_canonical(std::vector<std::pair<Quick_Pattern, int>> & quick_patterns_aggregation, std::vector<std::pair<Canonical_Graph*, int>> & canonical_graphs){
			for (unsigned int i = 0; i < quick_patterns_aggregation.size(); i++) {
				std::pair<Quick_Pattern, int> one_quick_pair = quick_patterns_aggregation.at(i);
				Canonical_Graph* cf = pattern::turn_canonical_graph(one_quick_pair.first, false);
				canonical_graphs.push_back(std::make_pair(cf, one_quick_pair.second));
			}
		}

		// TODO:
		void local_aggregate(std::vector<std::pair<Quick_Pattern, int>> & in_tuples, std::vector<std::pair<Quick_Pattern, int>> & out_tuples) {

		}

		// TODO:
		void local_aggregate(std::vector<std::pair<Canonical_Graph*, int>> & in_graphs, std::vector<std::pair<Canonical_Graph*, int>> & out_graphs) {

		}

		int get_global_bucket_index(int hash_val) {
			return hash_val % context.num_partitions;
		}
	};
}



#endif /* CORE_AGGREGATION_HPP_ */
