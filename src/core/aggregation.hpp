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
typedef std::vector<Element_In_Tuple> Canonical_Graph;

namespace RStream {
	class Aggregation {
		const Engine & context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

		int sizeof_in_tuple;

	public:
		Aggregation(Engine & e) : context(e) {
			atomic_num_producers = 0;
			atomic_partition_id = 0;
			atomic_partition_number = context.num_partitions;
			sizeof_in_tuple = MPhase::sizeof_in_tuple;
		}

		~Aggregation() {}

		/*
		 * do aggregation for update stream
		 * @param: in update stream
		 * @return: aggregation stream
		 * */
		Aggregation_Stream aggregate(Update_Stream in_update_stream) {
			Aggregation_Stream aggreg_c = Engine::aggregation_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// output should be a pair of <tuples, count>
			// tuples -- canonical pattern
			// count -- counter for patterns
			int sizeof_output = sizeof_in_tuple + sizeof(int);
			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_output);

			// exec threads will do aggregate and push result patterns into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->aggregate_producer(in_update_stream, buffers_for_shuffle, task_queue); } ));

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


		void printout_aggstream(Aggregation_Stream agg_stream){

		}



	private:

		void aggregate_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
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
				std::vector<std::pair<bliss::AbstractGraph *, int>> canonical_graphs;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - IO_SIZE * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE;

					assert(valid_io_size % MPhase::sizeof_in_tuple == 0);

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					std::vector<Element_In_Tuple> in_update_tuple;
					std::vector<std::pair<Quick_Pattern, int>> quick_patterns;

					// streaming tuples in, do aggregation
					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_tuple) {
						// get an in_update_tuple
						in_update_tuple = get_an_in_update(update_local_buf + pos);

						Quick_Pattern quick_pattern;
						// turn tuple to quick pattern
						pattern::turn_quick_pattern_pure(in_update_tuple, quick_pattern);
						quick_patterns.push_back(std::make_pair(quick_pattern, 1));
					}

					// do aggregation on quick patterns
					local_aggregate(quick_patterns, quick_patterns_aggregation);

					quick_patterns.clear();
				}

				// for all the aggregated quick patterns, turn to canocail graphs
				for(int i = 0; i < quick_patterns_aggregation; i++) {
					std::pair<Quick_Pattern, int> one_quick_pair = quick_patterns_aggregation.at(i);
					bliss::AbstractGraph * ag = pattern::turn_canonical_graph(one_quick_pair.first, false);
					canonical_graphs.push_back(std::make_pair(ag, one_quick_pair.second));
				}

				quick_patterns_aggregation.clear();

				std::vector<std::pair<bliss::AbstractGraph *, int>> canonical_graphs_aggregation;
				// for all the canonical graphs, do local aggregation
				local_aggregate(canonical_graphs, canonical_graphs_aggregation);

				canonical_graphs.clear();

				char* out_cg = nullptr;
				// for each canonical graph, do map reduce, shuffle to corresponding buckets
				for(int i = 0; i < canonical_graphs_aggregation.size(); i++) {
					std::pair<bliss::AbstractGraph *, int> one_canonical_graph = canonical_graphs_aggregation.at(i);
					int hash = hash_canonical_graph(one_canonical_graph);
					int index = get_global_bucket_index(hash);

					// TODO: insert
					global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, index);
					global_buf->insert(out_cg);
				}
				canonical_graphs_aggregation.clear();

				delete[] update_local_buf;
				close(fd_update);

			}
			atomic_num_producers--;
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

		std::vector<Element_In_Tuple> & get_an_in_update(char * update_local_buf) {
			std::vector<Element_In_Tuple> tuple;

			for(int index = 0; index < sizeof_in_tuple; index += sizeof(Element_In_Tuple)) {
				Element_In_Tuple & element = *(Element_In_Tuple*)(update_local_buf + index);
				tuple.push_back(element);
			}
			return tuple;
		}

		// TODO:
		void local_aggregate(std::vector<std::pair<Quick_Pattern, int>> & in_tuples, std::vector<std::pair<Quick_Pattern, int>> & out_tuples) {

		}

		// TODO:
		void local_aggregate(std::vector<std::pair<bliss::AbstractGraph *, int>> & in_graphs, std::vector<std::pair<bliss::AbstractGraph *, int>> & out_graphs) {

		}

		// TODO:
		int hash_canonical_graph(std::pair<bliss::AbstractGraph * , int> ag_pair) {
			return 0;
		}

		// TODO:
		int get_global_bucket_index(int hash_val) {

			return hash_val;
//			int partition_id = hash_val / context.num_vertices_per_part;
//			return partition_id < (context.num_partitions - 1) ? partition_id : (context.num_partitions - 1);
		}
	};
}



#endif /* CORE_AGGREGATION_HPP_ */
