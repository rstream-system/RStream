/*
 * aggregation.hpp
 *
 *  Created on: Jul 7, 2017
 *      Author: kai
 */

#ifndef CORE_AGGREGATION_HPP_
#define CORE_AGGREGATION_HPP_

#include "mining_phase.hpp"

namespace RStream {

	class Aggregation {
		const Engine & context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

	public:

		//virtual functions
//		virtual bool filter_aggregate(std::vector<Element_In_Tuple> & update_tuple, std::unordered_map<Canonical_Graph, int>& map, int threshold) = 0;


		//constructor
		Aggregation(Engine & e) : context(e) {}

		virtual ~Aggregation() {}


		void atomic_init(){
			atomic_num_producers = context.num_exec_threads;
			atomic_partition_id = -1;
			atomic_partition_number = context.num_partitions;
		}


		/*
		 * do aggregation for update stream
		 * @param: in update stream
		 * @return: aggregation stream
		 * */
		Aggregation_Stream aggregate(Update_Stream in_update_stream, int sizeof_in_tuple) {
			Aggregation_Stream stream_local = aggregate_local(in_update_stream, sizeof_in_tuple);
			int sizeof_agg = get_out_size(sizeof_in_tuple);
			return aggregate_global(stream_local, sizeof_agg);
		}


		Update_Stream aggregate_filter(Update_Stream up_stream, Aggregation_Stream agg_stream, int sizeof_in_tuple, int threshold){
			Update_Stream up_stream_shuffled_on_canonical = shuffle_upstream_canonicalgraph(up_stream, sizeof_in_tuple);
			return aggregate_filter_local(up_stream_shuffled_on_canonical, agg_stream, sizeof_in_tuple, threshold);
		}

		void printout_aggstream(Aggregation_Stream agg_stream){

		}



	private:

		Update_Stream shuffle_upstream_canonicalgraph(Update_Stream in_update_stream, int sizeof_in_tuple){
			atomic_init();

			Update_Stream update_c = Engine::aggregation_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_in_tuple);

			// exec threads will do aggregate and push result patterns into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->shuffle_on_canonical_producer(in_update_stream, buffers_for_shuffle, task_queue, sizeof_in_tuple); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&Aggregation::update_consumer, this, update_c, buffers_for_shuffle));

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
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)) {
				std::cout << "partition: " << partition_id << std::endl;

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_tuple);
				int streaming_counter = update_file_size / real_io_size + 1;

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - real_io_size * (streaming_counter - 1);
					else
						valid_io_size = real_io_size;

					assert(valid_io_size % sizeof_in_tuple == 0);

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// streaming tuples in, do aggregation
					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_tuple) {
						// get an in_update_tuple
						std::vector<Element_In_Tuple> in_update_tuple;
						MPhase::get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

//						//for debugging only
//						std::cout << "tuple: " << in_update_tuple << std::endl;

						shuffle_on_canonical(in_update_tuple, buffers_for_shuffle);
					}

				}

				free(update_local_buf);
				close(fd_update);

			}
			atomic_num_producers--;

		}

		void shuffle_on_canonical(std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining ** buffers_for_shuffle){
			// turn tuple to quick pattern
			Quick_Pattern quick_pattern;
			Pattern::turn_quick_pattern_pure(in_update_tuple, quick_pattern);
			std::vector<Element_In_Tuple> sub_graph = quick_pattern.get_tuple();
			Canonical_Graph* cf = Pattern::turn_canonical_graph(sub_graph, false);

			unsigned int hash = cf->get_hash();
			unsigned int index = get_global_bucket_index(hash);
			delete cf;

			//get tuple
			insert_tuple_to_buffer(index, in_update_tuple, buffers_for_shuffle);

			//for debugging only
			std::cout << "tuple: " << in_update_tuple << " ==> " << index << std::endl;
		}

		void insert_tuple_to_buffer(int partition_id, std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
			char* out_update = reinterpret_cast<char*>(in_update_tuple.data());
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			global_buf->insert(out_update);
		}


		Update_Stream aggregate_filter_local(Update_Stream up_stream_shuffled_on_canonical, Aggregation_Stream agg_stream, int sizeof_in_tuple, int threshold){
			atomic_init();

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
				exec_threads.push_back( std::thread([=] { this->aggregate_filter_local_producer(up_stream_shuffled_on_canonical, buffers_for_shuffle, task_queue, sizeof_in_tuple, agg_stream, threshold); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&Aggregation::update_consumer, this, update_c, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			return update_c;
		}

		void aggregate_filter_local_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue, int sizeof_in_tuple, Aggregation_Stream agg_stream, int threshold){
			int partition_id = -1;
			int sizeof_agg = get_out_size(sizeof_in_tuple);

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				std::cout << "partition: " << partition_id << std::endl;
				print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				int fd_agg = open((context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(agg_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0 && fd_agg > 0 );

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);
				long agg_file_size = io_manager::get_filesize(fd_agg);

				// aggs are fully loaded into memory
				char * agg_local_buf = (char *)malloc(agg_file_size);
				io_manager::read_from_file(fd_agg, agg_local_buf, agg_file_size, 0);

				//build hashmap for aggregation tuples
				std::unordered_map<Canonical_Graph, int> map;
				build_aggmap(map, agg_local_buf, agg_file_size, sizeof_agg);

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_tuple);
				int streaming_counter = update_file_size / real_io_size + 1;

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - real_io_size * (streaming_counter - 1);
					else
						valid_io_size = real_io_size;

					assert(valid_io_size % sizeof_in_tuple == 0);

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// streaming updates in, do hash join
					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_tuple) {
						// get an in_update_tuple
						std::vector<Element_In_Tuple> in_update_tuple;
						MPhase::get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

						if(!filter_aggregate(in_update_tuple, map, threshold)){
							insert_tuple_to_buffer(partition_id, in_update_tuple, buffers_for_shuffle);
						}

					}
				}

				free(update_local_buf);
				free(agg_local_buf);

				close(fd_update);
				close(fd_agg);
			}

			atomic_num_producers--;
		}

		void build_aggmap(std::unordered_map<Canonical_Graph, int>& map, char* agg_local_buf, int agg_file_size, int sizeof_agg){

		}

		bool filter_aggregate(std::vector<Element_In_Tuple> & update_tuple, std::unordered_map<Canonical_Graph, int>& map, int threshold){
			std::vector<Element_In_Tuple> quick_pattern(update_tuple.size());
			// turn tuple to quick pattern
			Pattern::turn_quick_pattern_pure(update_tuple, quick_pattern);
			Canonical_Graph* cf = Pattern::turn_canonical_graph(quick_pattern, false);

			assert(map.find(*cf) != map.end());
			bool r = map[*cf] < threshold;
			delete cf;
			return r;
		}

		Aggregation_Stream aggregate_local(Update_Stream in_update_stream, int sizeof_in_tuple) {
			atomic_init();

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
			std::cout << "size_of_in_tuple = " << sizeof_in_tuple << ", size_of_agg = " << sizeof_output << std::endl;
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

		static int get_out_size(int sizeof_in_tuple){
			return sizeof_in_tuple + sizeof(unsigned int) * 2 + sizeof(int);
		}

		Aggregation_Stream aggregate_global(Aggregation_Stream in_agg_stream, int sizeof_in_agg) {
			Aggregation_Stream aggreg_c = Engine::aggregation_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// exec threads will do aggregate and push result patterns into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->aggregate_global_per_thread(in_agg_stream, task_queue, sizeof_in_agg, aggreg_c); } ));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			delete task_queue;

			return aggreg_c;
		}

		void aggregate_global_per_thread(Aggregation_Stream in_agg_stream, concurrent_queue<int> * task_queue, int sizeof_in_agg, Aggregation_Stream out_agg_stream) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)) {
				std::cout << partition_id << std::endl;

				int fd_agg = open((context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(in_agg_stream)).c_str(), O_RDONLY);
				assert(fd_agg > 0);

				// get file size
				long agg_file_size = io_manager::get_filesize(fd_agg);

				// streaming updates
				char * agg_local_buf = (char *)malloc(agg_file_size);

				io_manager::read_from_file(fd_agg, agg_local_buf, agg_file_size, 0);

				// read aggregation pair in, do aggregation
				std::unordered_map<Canonical_Graph, int> canonical_graphs_aggregation;

				for(long pos = 0; pos < agg_file_size; pos += sizeof_in_agg) {
					// get an in_update_tuple
					std::pair<Canonical_Graph, int> in_agg_pair;
					get_an_in_agg_pair(agg_local_buf + pos, in_agg_pair, sizeof_in_agg);
//					std::cout << "{" << in_agg_pair.first << " --> " << in_agg_pair.second << std::endl;

					// for all the canonical graph pair, do local aggregation
					aggregate_on_canonical_graph(canonical_graphs_aggregation, in_agg_pair);
				}

				const char * file_name = (context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(out_agg_stream)).c_str();
				write_canonical_aggregation(canonical_graphs_aggregation, file_name, sizeof_in_agg);

				free(agg_local_buf);
				close(fd_agg);

			}
		}

		void aggregate_on_canonical_graph(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, std::pair<Canonical_Graph, int>& in_agg_pair){
			if(canonical_graphs_aggregation.find(in_agg_pair.first) != canonical_graphs_aggregation.end()){
				canonical_graphs_aggregation[in_agg_pair.first] = canonical_graphs_aggregation[in_agg_pair.first] + in_agg_pair.second;
			}
			else{
				canonical_graphs_aggregation[in_agg_pair.first] = in_agg_pair.second;
			}
		}

		void get_an_in_agg_pair(char * update_local_buf, std::pair<Canonical_Graph, int> & agg_pair, int sizeof_in_agg){
			std::vector<Element_In_Tuple> tuple;
			for(unsigned int index = 0; index < sizeof_in_agg - sizeof(unsigned int) * 2 - sizeof(int); index += sizeof(Element_In_Tuple)){
				Element_In_Tuple element = *(Element_In_Tuple*)(update_local_buf + index);
				tuple.push_back(element);
			}
			unsigned int num_of_vertices = *(unsigned int*)(update_local_buf + (sizeof_in_agg - sizeof(unsigned int) * 2 - sizeof(int)));
			unsigned int hash_value = *(unsigned int*)(update_local_buf + (sizeof_in_agg - sizeof(unsigned int) - sizeof(int)));
			Canonical_Graph cg(tuple, num_of_vertices, hash_value);

			int support = *(unsigned int*)(update_local_buf + (sizeof_in_agg - sizeof(int)));

			agg_pair.first = cg;
			agg_pair.second = support;
		}

		void write_canonical_aggregation(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, const char* file_name, unsigned int sizeof_in_agg){
			printout_cg_aggmap(canonical_graphs_aggregation);

			//assert size equality
			if(!canonical_graphs_aggregation.empty()){
				assert(sizeof_in_agg == ((*canonical_graphs_aggregation.begin()).first.get_tuple().size() * sizeof(Element_In_Tuple) + sizeof(unsigned int) * 2 + sizeof(int)));
			}

			char * local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
			long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_agg);
			long offset = 0;
			long index = 0;

			for(auto it = canonical_graphs_aggregation.begin(); it != canonical_graphs_aggregation.end(); ++it){
				if(offset < real_io_size){
					char* out_agg_pair = convert_to_bytes(sizeof_in_agg, *it);
					std::memcpy(local_buf + index, out_agg_pair, sizeof_in_agg);
					free(out_agg_pair);
					index += sizeof_in_agg;
					offset += sizeof_in_agg;
				}
				else if (offset == real_io_size){
					offset = 0;

					//write to file
					write_buf_to_file(file_name, local_buf, real_io_size);
				}
				else{
					assert(false);
				}
			}

			//deal with remaining buffer
			if(offset != 0){
				write_buf_to_file(file_name, local_buf, offset);
			}

		}

		static void write_buf_to_file(const char* file_name, char * local_buf, size_t fsize){
			int perms = O_WRONLY | O_APPEND;
			int fd = open(file_name, perms, S_IRWXU);
			if(fd < 0){
				fd = creat(file_name, S_IRWXU);
			}
			// flush buffer to update out stream
			io_manager::write_to_file(fd, local_buf, fsize);
			close(fd);
		}


		void aggregate_local_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue, int sizeof_in_tuple) {
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
				long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_tuple);
				int streaming_counter = update_file_size / real_io_size + 1;

				long valid_io_size = 0;
				long offset = 0;

				std::unordered_map<Quick_Pattern, int> quick_patterns_aggregation;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - real_io_size * (streaming_counter - 1);
					else
						valid_io_size = real_io_size;
					assert(valid_io_size % sizeof_in_tuple == 0);

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// streaming tuples in, do aggregation
					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_tuple) {
						// get an in_update_tuple
						std::vector<Element_In_Tuple> in_update_tuple;
						MPhase::get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);
//						std::cout << "in_update: \t" << in_update_tuple << std::endl;

						// turn tuple to quick pattern
						Quick_Pattern quick_pattern;
						Pattern::turn_quick_pattern_pure(in_update_tuple, quick_pattern);
//						std::cout << "quick_pattern: \t" << quick_pattern << "\n" << std::endl;
						aggregate_on_quick_pattern(quick_patterns_aggregation, quick_pattern);
					}

//					//for debugging
//					printout_quickpattern_aggmap(quick_patterns_aggregation);

				}

				std::unordered_map<Canonical_Graph, int> canonical_graphs_aggregation;
				aggregate_on_canonical_graph(canonical_graphs_aggregation, quick_patterns_aggregation);

				// for each canonical graph, do map reduce, shuffle to corresponding buckets
				shuffle_canonical_aggregation(canonical_graphs_aggregation, buffers_for_shuffle);

				free(update_local_buf);
				close(fd_update);

			}
			atomic_num_producers--;
		}

		//for debugging only
		void printout_quickpattern_aggmap(std::unordered_map<Quick_Pattern, int>& quick_patterns_aggregation){
			std::cout << "quick pattern map: \n";
			for(auto it = quick_patterns_aggregation.begin(); it != quick_patterns_aggregation.end(); ++it){
				std::cout << it->first << " --> " << it->second << std::endl;
			}
			std::cout << std::endl;
		}

		//for debugging only
		void printout_cg_aggmap(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation){
			std::cout << "canonical graph map: \n";
			for(auto it = canonical_graphs_aggregation.begin(); it != canonical_graphs_aggregation.end(); ++it){
				std::cout << it->first << " --> " << it->second << std::endl;
			}
			std::cout << std::endl;
		}

		void aggregate_on_quick_pattern(std::unordered_map<Quick_Pattern, int>& quick_patterns_aggregation, Quick_Pattern& quick_pattern){
			if(quick_patterns_aggregation.find(quick_pattern) != quick_patterns_aggregation.end()){
				quick_patterns_aggregation[quick_pattern] = quick_patterns_aggregation[quick_pattern] + 1;
			}
			else{
				quick_patterns_aggregation[quick_pattern] = 1;
			}

		}

		void aggregate_on_canonical_graph(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, std::unordered_map<Quick_Pattern, int>& quick_patterns_aggregation){
			for(auto it = quick_patterns_aggregation.begin(); it != quick_patterns_aggregation.end(); ++it){
				std::vector<Element_In_Tuple> sub_graph = it->first.get_tuple();
//				std::cout << "quick_pattern: \t" << sub_graph << std::endl;
				int s = it->second;
				Canonical_Graph* cg = Pattern::turn_canonical_graph(sub_graph, false);
//				std::cout << "canonical_graph: \t" << *cg << std::endl;

//				std::cout << (canonical_graphs_aggregation.find(*cg) != canonical_graphs_aggregation.end()) << std::endl;
				if(canonical_graphs_aggregation.find(*cg) != canonical_graphs_aggregation.end()){
					canonical_graphs_aggregation[*cg] = canonical_graphs_aggregation[*cg] + s;
				}
				else{
					canonical_graphs_aggregation[*cg] = s;
				}


				delete cg;
			}

//			//for debugging only
//			printout_cg_aggmap(canonical_graphs_aggregation);
		}


		void shuffle_canonical_aggregation(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, global_buffer_for_mining ** buffers_for_shuffle){
			for(auto it = canonical_graphs_aggregation.begin(); it != canonical_graphs_aggregation.end(); ++it) {
				Canonical_Graph canonical_graph = it->first;

				unsigned int hash = canonical_graph.get_hash();
				unsigned int index = get_global_bucket_index(hash);
//				std::cout << "hash: \t" << hash << ", \tindex: \t" << index << std::endl;
				global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, index);

				char* out_cg = convert_to_bytes(global_buf->get_sizeoftuple(), (*it));
//				int s = it->second;
//				char* out_cg = (char *)malloc(global_buf->get_sizeoftuple());
//				size_t s_vector = global_buf->get_sizeoftuple()- sizeof(unsigned int) * 2 - sizeof(int);
//				std::memcpy(out_cg, reinterpret_cast<char*>(canonical_graph.get_tuple().data()), s_vector);
//				unsigned int num_vertices = canonical_graph.get_number_vertices();
//				std::memcpy(out_cg + s_vector, &num_vertices, sizeof(unsigned int));
//				unsigned int hash_value = canonical_graph.get_hash();
//				std::memcpy(out_cg + s_vector + sizeof(unsigned int), &hash_value, sizeof(unsigned int));
//
//				std::memcpy(out_cg + s_vector + sizeof(unsigned int) * 2, &s, sizeof(int));

				// TODO: insert
				global_buf->insert(out_cg);
				delete out_cg;
			}
		}

		static char* convert_to_bytes(size_t sizeof_agg_pair, std::pair<const Canonical_Graph, int>& it_pair){
			Canonical_Graph canonical_graph = it_pair.first;
			int s = it_pair.second;

			char* out_cg = (char *)malloc(sizeof_agg_pair);
			size_t s_vector = sizeof_agg_pair- sizeof(unsigned int) * 2 - sizeof(int);
			std::memcpy(out_cg, reinterpret_cast<char*>(canonical_graph.get_tuple().data()), s_vector);
			unsigned int num_vertices = canonical_graph.get_number_vertices();
			std::memcpy(out_cg + s_vector, &num_vertices, sizeof(unsigned int));
			unsigned int hash_value = canonical_graph.get_hash();
			std::memcpy(out_cg + s_vector + sizeof(unsigned int), &hash_value, sizeof(unsigned int));

			std::memcpy(out_cg + s_vector + sizeof(unsigned int) * 2, &s, sizeof(int));

			return out_cg;
		}

		// each writer thread generates a join_consumer
		void aggregate_consumer(Aggregation_Stream aggregation_stream, global_buffer_for_mining ** buffers_for_shuffle) {
			while(atomic_num_producers != 0) {
				int i = (++atomic_partition_id) % context.num_partitions;

				const char * file_name = (context.filename + "." + std::to_string(i) + ".aggregate_stream_" + std::to_string(aggregation_stream)).c_str();
				global_buffer_for_mining* g_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name, i);

				atomic_partition_id = atomic_partition_id % context.num_partitions;
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

		void update_consumer(Update_Stream out_update_stream, global_buffer_for_mining ** buffers_for_shuffle) {
			while(atomic_num_producers != 0) {
				int i = (++atomic_partition_id) % context.num_partitions;

				const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(out_update_stream)).c_str();
				global_buffer_for_mining* g_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name, i);

				atomic_partition_id = atomic_partition_id % context.num_partitions;
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;

				if(i >= 0){

					const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(out_update_stream)).c_str();
					global_buffer_for_mining* g_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, i);
					g_buf->flush_end(file_name, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}


//		void map_canonical(std::vector<std::pair<Quick_Pattern, int>> & quick_patterns_aggregation, std::vector<std::pair<Canonical_Graph*, int>> & canonical_graphs){
//			for (unsigned int i = 0; i < quick_patterns_aggregation.size(); i++) {
//				std::pair<Quick_Pattern, int> one_quick_pair = quick_patterns_aggregation.at(i);
//				Canonical_Graph* cf = pattern::turn_canonical_graph(one_quick_pair.first, false);
//				canonical_graphs.push_back(std::make_pair(cf, one_quick_pair.second));
//			}
//		}


		unsigned int get_global_bucket_index(unsigned int hash_val) {
			return hash_val % context.num_partitions;
		}
	};
}



#endif /* CORE_AGGREGATION_HPP_ */
