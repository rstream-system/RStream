/*
 * aggregation.cpp
 *
 *  Created on: Aug 4, 2017
 *      Author: icuzzq
 */

#include "aggregation.hpp"

namespace RStream {

		Aggregation::Aggregation(Engine & e, bool label_f) : context(e), label_flag(label_f) {}

		Aggregation::~Aggregation() {}


		void Aggregation::atomic_init(){
			atomic_num_producers = context.num_exec_threads;
			atomic_partition_id = -1;
			atomic_partition_number = context.num_partitions;
		}


		/*
		 * do aggregation for update stream
		 * @param: in update stream
		 * @return: aggregation stream
		 * */
		Aggregation_Stream Aggregation::aggregate(Update_Stream in_update_stream, int sizeof_in_tuple) {
			Aggregation_Stream stream_local = aggregate_local(in_update_stream, sizeof_in_tuple);
			int sizeof_agg = get_out_size(sizeof_in_tuple);
			Aggregation_Stream agg_stream = aggregate_global(stream_local, sizeof_agg);
			delete_aggstream(stream_local);
			return agg_stream;
		}


		Update_Stream Aggregation::aggregate_filter(Update_Stream up_stream, Aggregation_Stream agg_stream, int sizeof_in_tuple, int threshold){
			Update_Stream up_stream_shuffled_on_canonical = shuffle_upstream_canonicalgraph(up_stream, sizeof_in_tuple);
			Update_Stream up_stream_filtered = aggregate_filter_local(up_stream_shuffled_on_canonical, agg_stream, sizeof_in_tuple, threshold);
			MPhase::delete_upstream_static(up_stream_shuffled_on_canonical, context.num_partitions, context.filename);
			return up_stream_filtered;
		}

		void Aggregation::printout_aggstream(Aggregation_Stream agg_stream, int sizeof_in_tuple){
			int sizeof_agg = get_out_size(sizeof_in_tuple);
			std::cout << "Number of tuples in agg "<< agg_stream << ": \t" << get_count(agg_stream, sizeof_agg) << std::endl;
			std::cout << "Size of agg: \t" << sizeof_agg << std::endl;
		}

		unsigned int Aggregation::get_count(Aggregation_Stream in_update_stream, int sizeof_agg){
			unsigned int count_total = 0;
			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);

				count_total += update_file_size / sizeof_agg;
			}

			return count_total;
		}

		void Aggregation::delete_aggstream(Aggregation_Stream agg_stream){
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				std::string filename = context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(agg_stream);
				if (std::remove(filename.c_str()) != 0)
					perror("Error deleting file.\n");
				else
					std::cout << (filename + " successfully deleted.\n");
			}
		}

		Update_Stream Aggregation::aggregate_filter_clique(Update_Stream in_agg_stream, int sizeof_in_agg) {
			atomic_init();
			Update_Stream aggreg_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_in_agg);

			// exec threads will do aggregate and push result patterns into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->aggregate_filter_clique_per_thread(buffers_for_shuffle, in_agg_stream, task_queue, sizeof_in_agg, aggreg_c); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&Aggregation::update_consumer, this, aggreg_c, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

//			std::cout << "filter done." << std::endl;

			delete task_queue;

			return aggreg_c;
		}


		Update_Stream Aggregation::shuffle_upstream_canonicalgraph(Update_Stream in_update_stream, int sizeof_in_tuple){
			atomic_init();

			Update_Stream update_c = Engine::update_count++;

//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
//				task_queue->push(partition_id);
//			}

			concurrent_queue<std::tuple<int, long, long>>* task_queue = MPhase::divide_tasks(context.num_partitions, context.filename, in_update_stream, sizeof_in_tuple, CHUNK_SIZE);

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

		void Aggregation::shuffle_on_canonical_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, int sizeof_in_tuple) {
			std::tuple<int, long, long> task_id (-1, -1, -1);

			// pop from queue
			while(task_queue->test_pop_atomic(task_id)) {
				int partition_id = std::get<0>(task_id);
				long offset_task = std::get<1>(task_id);
				long size_task = std::get<2>(task_id);
				assert(partition_id != -1 && offset_task != -1 && size_task != -1);

				Logger::print_thread_info_locked("as a (shuffle-upstream-on-canonical) producer dealing with partition " + MPhase::get_string_task_tuple(task_id) + "\n");

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
//				long update_file_size = io_manager::get_filesize(fd_update);
				long update_file_size = size_task;

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_tuple);
				int streaming_counter = update_file_size / real_io_size + 1;

				long valid_io_size = 0;
//				long offset = 0;
				long offset = offset_task;

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
//						std::vector<Element_In_Tuple> in_update_tuple;
//						in_update_tuple.reserve(sizeof_in_tuple / sizeof(Element_In_Tuple));
//						MPhase::get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

						MTuple in_update_tuple(sizeof_in_tuple);
						MPhase::get_an_in_update(update_local_buf + pos, in_update_tuple);

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

		void Aggregation::shuffle_on_canonical(MTuple& in_update_tuple, global_buffer_for_mining ** buffers_for_shuffle){
//			// turn tuple to quick pattern
//			Quick_Pattern quick_pattern;
//			Pattern::turn_quick_pattern_pure(in_update_tuple, quick_pattern, label_flag);
//			std::vector<Element_In_Tuple> sub_graph = quick_pattern.get_tuple();


			// turn tuple to quick pattern
			Quick_Pattern quick_pattern(in_update_tuple.get_size() * sizeof(Element_In_Tuple));
			Pattern::turn_quick_pattern_pure(in_update_tuple, quick_pattern, label_flag);

			Canonical_Graph* cf = Pattern::turn_canonical_graph(quick_pattern, false);

			unsigned int hash = cf->get_hash();
			unsigned int index = get_global_bucket_index(hash);
			delete cf;

			//get tuple
			insert_tuple_to_buffer(index, in_update_tuple, buffers_for_shuffle);

//			//for debugging only
//			std::cout << "tuple: " << in_update_tuple << " ==> " << index << std::endl;
		}

		void Aggregation::insert_tuple_to_buffer(int partition_id, std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
			char* out_update = reinterpret_cast<char*>(in_update_tuple.data());
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			global_buf->insert(out_update);
		}

		void Aggregation::insert_tuple_to_buffer(int partition_id, MTuple& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
			char* out_update = reinterpret_cast<char*>(in_update_tuple.get_elements());
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			global_buf->insert(out_update);
		}


		Update_Stream Aggregation::aggregate_filter_local(Update_Stream up_stream_shuffled_on_canonical, Aggregation_Stream agg_stream, int sizeof_in_tuple, int threshold){
			atomic_init();

			Update_Stream update_c = Engine::update_count++;

//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
//				task_queue->push(partition_id);
//			}

			concurrent_queue<std::tuple<int, long, long>>* task_queue = MPhase::divide_tasks(context.num_partitions, context.filename, up_stream_shuffled_on_canonical, sizeof_in_tuple, CHUNK_SIZE);

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

		void Aggregation::aggregate_filter_local_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, int sizeof_in_tuple, Aggregation_Stream agg_stream, int threshold){
			int sizeof_agg = get_out_size(sizeof_in_tuple);
			std::tuple<int, long, long> task_id (-1, -1, -1);

			// pop from queue
			while(task_queue->test_pop_atomic(task_id)){
				int partition_id = std::get<0>(task_id);
				long offset_task = std::get<1>(task_id);
				long size_task = std::get<2>(task_id);
				assert(partition_id != -1 && offset_task != -1 && size_task != -1);

				Logger::print_thread_info_locked("as a (aggregate-filter) producer dealing with partition " + MPhase::get_string_task_tuple(task_id) + "\n");


				int fd_agg = open((context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(agg_stream)).c_str(), O_RDONLY);
				assert(fd_agg > 0);
				// get file size
				long agg_file_size = io_manager::get_filesize(fd_agg);

				// aggs are fully loaded into memory
				char * agg_local_buf = (char *)malloc(agg_file_size);
				io_manager::read_from_file(fd_agg, agg_local_buf, agg_file_size, 0);

				//build hashmap for aggregation tuples
				std::unordered_map<Canonical_Graph, int> map;
				build_aggmap(map, agg_local_buf, agg_file_size, sizeof_agg);
//				printout_cg_aggmap(map);


				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

//				long update_file_size = io_manager::get_filesize(fd_update);
				long update_file_size = size_task;

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_tuple);
				int streaming_counter = update_file_size / real_io_size + 1;

				long valid_io_size = 0;
//				long offset = 0;
				long offset = offset_task;

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
//						std::vector<Element_In_Tuple> in_update_tuple;
//						in_update_tuple.reserve(sizeof_in_tuple / sizeof(Element_In_Tuple));
//						MPhase::get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

						MTuple in_update_tuple(sizeof_in_tuple);
						MPhase::get_an_in_update(update_local_buf + pos, in_update_tuple);
//						std::cout << "in_update: \t" << in_update_tuple << std::endl;

//						//for debugging
//						std::cout << in_update_tuple << " --> " << filter_aggregate(in_update_tuple, map, threshold) << std::endl;

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

		void Aggregation::build_aggmap(std::unordered_map<Canonical_Graph, int>& map, char* agg_local_buf, long agg_file_size, int sizeof_agg){
			assert(agg_file_size % sizeof_agg == 0);
			for(long pos = 0; pos < agg_file_size; pos += sizeof_agg){
				//read aggregation pair
				std::pair<Canonical_Graph, int> in_agg_pair;
				get_an_in_agg_pair(agg_local_buf + pos, in_agg_pair, sizeof_agg);

				assert(map.find(in_agg_pair.first) == map.end());
				map.insert(in_agg_pair);
			}
		}

		bool Aggregation::filter_aggregate(MTuple & update_tuple, std::unordered_map<Canonical_Graph, int>& map, int threshold){
//			// turn tuple to quick pattern
//			Quick_Pattern quick_pattern;
//			Pattern::turn_quick_pattern_pure(update_tuple, quick_pattern, label_flag);
//			std::vector<Element_In_Tuple> sub_graph = quick_pattern.get_tuple();

			// turn tuple to quick pattern
			Quick_Pattern quick_pattern(update_tuple.get_size() * sizeof(Element_In_Tuple));
			Pattern::turn_quick_pattern_pure(update_tuple, quick_pattern, label_flag);

			Canonical_Graph* cf = Pattern::turn_canonical_graph(quick_pattern, false);


			assert(map.find(*cf) != map.end());
			bool r = (map[*cf] < threshold);
			delete cf;
			return r;
		}

		Aggregation_Stream Aggregation::aggregate_local(Update_Stream in_update_stream, int sizeof_in_tuple) {
			atomic_init();

			Aggregation_Stream aggreg_c = Engine::aggregation_count++;

//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
//				task_queue->push(partition_id);
//			}

			concurrent_queue<std::tuple<int, long, long>>* task_queue = MPhase::divide_tasks(context.num_partitions, context.filename, in_update_stream, sizeof_in_tuple, CHUNK_SIZE);

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

		Aggregation_Stream Aggregation::aggregate_global(Aggregation_Stream in_agg_stream, int sizeof_in_agg) {
			Aggregation_Stream aggreg_c = Engine::aggregation_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// exec threads will do aggregate and push result patterns into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_threads; i++)
				exec_threads.push_back( std::thread([=] { this->aggregate_global_per_thread(in_agg_stream, task_queue, sizeof_in_agg, aggreg_c); } ));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			delete task_queue;

			return aggreg_c;
		}

		void Aggregation::aggregate_global_per_thread(Aggregation_Stream in_agg_stream, concurrent_queue<int> * task_queue, int sizeof_in_agg, Aggregation_Stream out_agg_stream) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)) {
				Logger::print_thread_info_locked("as a (aggregate-global) worker dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_agg = open((context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(in_agg_stream)).c_str(), O_RDONLY);
				assert(fd_agg > 0);

				// read aggregation pair in, do aggregation
				std::unordered_map<Canonical_Graph, int> canonical_graphs_aggregation;

				// get file size
				long agg_file_size = io_manager::get_filesize(fd_agg);

				char * agg_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_agg);
				int streaming_counter = agg_file_size / real_io_size + 1;
//				std::cout << "streaming counter: " << streaming_counter << std::endl;

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = agg_file_size - real_io_size * (streaming_counter - 1);
					else
						valid_io_size = real_io_size;

					assert(valid_io_size % sizeof_in_agg == 0);


	//				// streaming updates
	//				char * agg_local_buf = (char *)malloc(agg_file_size);

					io_manager::read_from_file(fd_agg, agg_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_agg) {
						// get an in_update_tuple
						std::pair<Canonical_Graph, int> in_agg_pair;
						get_an_in_agg_pair(agg_local_buf + pos, in_agg_pair, sizeof_in_agg);
	//					std::cout << "{" << in_agg_pair.first << " --> " << in_agg_pair.second << std::endl;

						// for all the canonical graph pair, do local aggregation
						aggregate_on_canonical_graph(canonical_graphs_aggregation, in_agg_pair);
					}
				}


				std::string file_name (context.filename + "." + std::to_string(partition_id) + ".aggregate_stream_" + std::to_string(out_agg_stream));
				write_canonical_aggregation(canonical_graphs_aggregation, file_name, sizeof_in_agg);

				free(agg_local_buf);
				close(fd_agg);

			}
		}

		void shuffle(MTuple_simple& out_update_tuple, global_buffer_for_mining ** buffers_for_shuffle, int partition_id, int num_parts) {
//			unsigned int hash = out_update_tuple.get_hash();
//			unsigned int index = hash % context.num_partitions;
//			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, index);

			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, num_parts, partition_id);
			char* out_update = reinterpret_cast<char*>(out_update_tuple.get_elements());
//			std::cout << *((Base_Element*)out_update) << std::endl;
//			std::cout << *((Base_Element*)added) << std::endl;
			global_buf->insert(out_update);

		}

		void aggregate_clique(global_buffer_for_mining ** buffers_for_shuffle, std::unordered_map<MTuple_simple, unsigned int>& mtuple_simple_aggregation, MTuple_simple& in_update_tuple, int partition_id, int num_parts){
			auto it = mtuple_simple_aggregation.find(in_update_tuple);
			if(it != mtuple_simple_aggregation.end()){
				if(it->second == it->first.get_size() - 2){
					shuffle(in_update_tuple, buffers_for_shuffle, partition_id, num_parts);
					mtuple_simple_aggregation.erase(it);
				}
				else{
					mtuple_simple_aggregation[in_update_tuple] = mtuple_simple_aggregation[in_update_tuple] + 1;
				}
			}
			else{
				mtuple_simple_aggregation[in_update_tuple] = 1;
			}
		}

		void Aggregation::aggregate_filter_clique_per_thread(global_buffer_for_mining ** buffers_for_shuffle, Update_Stream in_agg_stream, concurrent_queue<int> * task_queue, int sizeof_in_mtuple, Update_Stream out_agg_stream) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)) {
				Logger::print_thread_info_locked("as a (aggregate-filter-clique) worker dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_agg = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_agg_stream)).c_str(), O_RDONLY);
				assert(fd_agg > 0);

				// read aggregation pair in, do aggregation
				std::unordered_map<MTuple_simple, unsigned int>* mtuple_simple_aggregation = new std::unordered_map<MTuple_simple, unsigned int>;

				// get file size
				long agg_file_size = io_manager::get_filesize(fd_agg);

				// streaming edges
//				int size_of_unit = context.edge_unit;
				char * agg_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_mtuple);
				int streaming_counter = agg_file_size / real_io_size + 1;

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = agg_file_size - real_io_size * (streaming_counter - 1);
					else
						valid_io_size = real_io_size;

					if(valid_io_size % sizeof_in_mtuple != 0){
						std::cout << valid_io_size << ", " << sizeof_in_mtuple << std::endl;
					}
					assert(valid_io_size % sizeof_in_mtuple == 0);

					io_manager::read_from_file(fd_agg, agg_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_mtuple) {
						// get an in_update_tuple
						MTuple_simple in_update_tuple(sizeof_in_mtuple);
						MPhase::get_an_in_update(agg_local_buf + pos, in_update_tuple);
	//					std::cout << in_update_tuple << std::endl;

						aggregate_clique(buffers_for_shuffle, *mtuple_simple_aggregation, in_update_tuple, partition_id, context.num_partitions);
					}
				}


//				for(auto it = mtuple_simple_aggregation->begin(); it != mtuple_simple_aggregation->end();){
//					if(it->second != it->first.get_size() - 1){
//						it = mtuple_simple_aggregation->erase(it);
//					}
//					else{
//						it++;
//					}
//				}
//
//				std::cout << "done aggregation at partition " << partition_id << std::endl;
//				std::cout << sizeof_in_mtuple << std::endl;
//
//				std::string file_name (context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(out_agg_stream));
//				write_aggregation_clique(*mtuple_simple_aggregation, file_name, sizeof_in_mtuple);

				delete mtuple_simple_aggregation;

//				std::cout << "done partition " << partition_id << std::endl;
				free(agg_local_buf);
				close(fd_agg);
			}
			atomic_num_producers--;
		}

		void printout_aggregation_clique(std::unordered_map<MTuple_simple, unsigned int>& mtuple_aggregation){
			std::cout << "mtuple aggregation map: \n";
			for(auto it = mtuple_aggregation.begin(); it != mtuple_aggregation.end(); ++it){
				std::cout << it->first << " --> " << it->second << std::endl;
			}
			std::cout << std::endl;
		}

		void Aggregation::write_aggregation_clique(std::unordered_map<MTuple_simple, unsigned int>& mtuple_aggregation, std::string& file_name, unsigned int sizeof_in_mtuple){
////			//for debugging
//			printout_aggregation_clique(mtuple_aggregation);

			//write empty buffer to an empty file
			if(mtuple_aggregation.empty()){
				char* buf = (char *)malloc(0);
				write_buf_to_file(file_name.c_str(), buf, 0);
				free(buf);
				return;
			}

			//assert size equality
			if(!mtuple_aggregation.empty()){
				assert(sizeof_in_mtuple == ((*mtuple_aggregation.begin()).first.get_size() * sizeof(Base_Element)));
			}

			char * local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
			long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_mtuple);
			long offset = 0;

			for(auto it = mtuple_aggregation.begin(); it != mtuple_aggregation.end(); ){
				if(offset < real_io_size){
					MTuple_simple tuple = it->first;
					char* out_agg_pair = reinterpret_cast<char*>(tuple.get_elements());
					std::memcpy(local_buf + offset, out_agg_pair, sizeof_in_mtuple);
					offset += sizeof_in_mtuple;
					++it;
				}
				else if (offset == real_io_size){
					offset = 0;

					//write to file
					std::cout << "write to file " << file_name << std::endl;
					write_buf_to_file(file_name.c_str(), local_buf, real_io_size);
				}
				else{
					assert(false);
				}
			}

			//deal with remaining buffer
			if(offset != 0){
				std::cout << "finally write to file " << file_name << std::endl;
				write_buf_to_file(file_name.c_str(), local_buf, offset);
			}

			free(local_buf);
		}

		void Aggregation::aggregate_on_canonical_graph(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, std::pair<Canonical_Graph, int>& in_agg_pair){
			if(canonical_graphs_aggregation.find(in_agg_pair.first) != canonical_graphs_aggregation.end()){
				canonical_graphs_aggregation[in_agg_pair.first] = canonical_graphs_aggregation[in_agg_pair.first] + in_agg_pair.second;
			}
			else{
				canonical_graphs_aggregation[in_agg_pair.first] = in_agg_pair.second;
			}
		}

		void Aggregation::get_an_in_agg_pair(char * update_local_buf, std::pair<Canonical_Graph, int> & agg_pair, int sizeof_in_agg){
			std::vector<Element_In_Tuple>& tuple = agg_pair.first.get_tuple();
			for(unsigned int index = 0; index < sizeof_in_agg - sizeof(unsigned int) * 2 - sizeof(int); index += sizeof(Element_In_Tuple)){
				Element_In_Tuple element = *(Element_In_Tuple*)(update_local_buf + index);
				tuple.push_back(element);
			}
			unsigned int num_of_vertices = *(unsigned int*)(update_local_buf + (sizeof_in_agg - sizeof(unsigned int) * 2 - sizeof(int)));
			agg_pair.first.set_number_vertices(num_of_vertices);
			unsigned int hash_value = *(unsigned int*)(update_local_buf + (sizeof_in_agg - sizeof(unsigned int) - sizeof(int)));
			agg_pair.first.set_hash_value(hash_value);

			int support = *(unsigned int*)(update_local_buf + (sizeof_in_agg - sizeof(int)));
			agg_pair.second = support;

		}



		void Aggregation::write_canonical_aggregation(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, std::string& file_name, unsigned int sizeof_in_agg){
			//for debugging
			printout_cg_aggmap(canonical_graphs_aggregation);


			//write empty buffer to an empty file
			if(canonical_graphs_aggregation.empty()){
				char* buf = (char *)malloc(0);
				write_buf_to_file(file_name.c_str(), buf, 0);
				free(buf);
				return;
			}

			//assert size equality
			if(!canonical_graphs_aggregation.empty()){
				assert(sizeof_in_agg == ((*canonical_graphs_aggregation.begin()).first.get_tuple_const().size() * sizeof(Element_In_Tuple) + sizeof(unsigned int) * 2 + sizeof(int)));
			}

			char * local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
			long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_agg);
			long offset = 0;
//			long index = 0;

			for(auto it = canonical_graphs_aggregation.begin(); it != canonical_graphs_aggregation.end(); ){
				if(offset < real_io_size){
					char* out_agg_pair = convert_to_bytes(sizeof_in_agg, *it);
					std::memcpy(local_buf + offset, out_agg_pair, sizeof_in_agg);
					free(out_agg_pair);
//					index += sizeof_in_agg;
					offset += sizeof_in_agg;
					++it;
				}
				else if (offset == real_io_size){
					offset = 0;

					//write to file
//					std::cout << "write to file " << file_name << std::endl;
					write_buf_to_file(file_name.c_str(), local_buf, real_io_size);
				}
				else{
					assert(false);
				}
			}

			//deal with remaining buffer
			if(offset != 0){
//				std::cout << "write to file " << file_name << std::endl;
				write_buf_to_file(file_name.c_str(), local_buf, offset);
			}

			free(local_buf);
		}



		void Aggregation::aggregate_local_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, int sizeof_in_tuple) {
			std::tuple<int, long, long> task_id (-1, -1, -1);

			// pop from queue
			while(task_queue->test_pop_atomic(task_id)) {
				int partition_id = std::get<0>(task_id);
				long offset_task = std::get<1>(task_id);
				long size_task = std::get<2>(task_id);
				assert(partition_id != -1 && offset_task != -1 && size_task != -1);

				Logger::print_thread_info_locked("as a (aggregate-local) producer dealing with partition " + MPhase::get_string_task_tuple(task_id) + "\n");

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
//				long update_file_size = io_manager::get_filesize(fd_update);
				long update_file_size = size_task;

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = MPhase::get_real_io_size(IO_SIZE, sizeof_in_tuple);
				int streaming_counter = update_file_size / real_io_size + 1;

				long valid_io_size = 0;
//				long offset = 0;
				long offset = offset_task;

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
//						std::vector<Element_In_Tuple> in_update_tuple;
//						in_update_tuple.reserve(sizeof_in_tuple / sizeof(Element_In_Tuple));
//						MPhase::get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);
						MTuple in_update_tuple(sizeof_in_tuple);
						MPhase::get_an_in_update(update_local_buf + pos, in_update_tuple);
//						std::cout << "in_update: \t" << in_update_tuple << std::endl;

						// turn tuple to quick pattern
						Quick_Pattern quick_pattern(sizeof_in_tuple);
						Pattern::turn_quick_pattern_pure(in_update_tuple, quick_pattern, label_flag);
//						std::cout << "quick_pattern: \t" << quick_pattern << std::endl;
//						std::cout << "in_update: \t" << in_update_tuple << std::endl;
//						std::cout << std::endl;
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
		void Aggregation::printout_quickpattern_aggmap(std::unordered_map<Quick_Pattern, int>& quick_patterns_aggregation){
			std::cout << "quick pattern map: \n";
			for(auto it = quick_patterns_aggregation.begin(); it != quick_patterns_aggregation.end(); ++it){
				std::cout << it->first << " --> " << it->second << std::endl;
			}
			std::cout << std::endl;
		}

		//for debugging only
		void Aggregation::printout_cg_aggmap(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation){
			std::cout << "canonical graph map: \n";
			for(auto it = canonical_graphs_aggregation.begin(); it != canonical_graphs_aggregation.end(); ++it){
				std::cout << it->first << " --> " << it->second << std::endl;
			}
			std::cout << std::endl;
		}

		void Aggregation::aggregate_on_quick_pattern(std::unordered_map<Quick_Pattern, int>& quick_patterns_aggregation, Quick_Pattern& quick_pattern){
			if(quick_patterns_aggregation.find(quick_pattern) != quick_patterns_aggregation.end()){
				quick_patterns_aggregation[quick_pattern] = quick_patterns_aggregation[quick_pattern] + 1;
				quick_pattern.clean();
			}
			else{
				quick_patterns_aggregation[quick_pattern] = 1;
			}

		}

		void Aggregation::aggregate_on_canonical_graph(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, std::unordered_map<Quick_Pattern, int>& quick_patterns_aggregation){
			for(auto it = quick_patterns_aggregation.begin(); it != quick_patterns_aggregation.end(); ++it){
				Quick_Pattern sub_graph = it->first;
//				std::cout << "quick_pattern: \t" << sub_graph << std::endl;
				int s = it->second;
				Canonical_Graph* cg = Pattern::turn_canonical_graph(sub_graph, false);
//				std::cout << "canonical_graph: \t" << *cg << std::endl;

				//clean quick_pattern
				sub_graph.clean();

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


		void Aggregation::shuffle_canonical_aggregation(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, global_buffer_for_mining ** buffers_for_shuffle){
			for(auto it = canonical_graphs_aggregation.begin(); it != canonical_graphs_aggregation.end(); ++it) {
				Canonical_Graph canonical_graph = it->first;

				unsigned int hash = canonical_graph.get_hash();
				unsigned int index = get_global_bucket_index(hash);
//				std::cout << "hash: \t" << hash << ", \tindex: \t" << index << std::endl;
				global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, index);

				char* out_agg_pair = convert_to_bytes(global_buf->get_sizeoftuple(), (*it));
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
				global_buf->insert(out_agg_pair);
				delete out_agg_pair;
			}
		}


		// each writer thread generates a join_consumer
		void Aggregation::aggregate_consumer(Aggregation_Stream aggregation_stream, global_buffer_for_mining ** buffers_for_shuffle) {
			while(atomic_num_producers != 0) {
				int i = (++atomic_partition_id) % context.num_partitions;

				std::string file_name (context.filename + "." + std::to_string(i) + ".aggregate_stream_" + std::to_string(aggregation_stream));
				global_buffer_for_mining* g_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name, i);

				atomic_partition_id = atomic_partition_id % context.num_partitions;
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;

				if(i >= 0){

					std::string file_name (context.filename + "." + std::to_string(i) + ".aggregate_stream_" + std::to_string(aggregation_stream));
					global_buffer_for_mining* g_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, i);
					g_buf->flush_end(file_name, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}

		void Aggregation::update_consumer(Update_Stream out_update_stream, global_buffer_for_mining ** buffers_for_shuffle) {
			while(atomic_num_producers != 0) {
				int i = (++atomic_partition_id) % context.num_partitions;

				std::string file_name (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(out_update_stream));
				global_buffer_for_mining* g_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name, i);

				atomic_partition_id = atomic_partition_id % context.num_partitions;
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;

				if(i >= 0){

					std::string file_name (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(out_update_stream));
					global_buffer_for_mining* g_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, i);
					g_buf->flush_end(file_name, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}


		unsigned int Aggregation::get_global_bucket_index(unsigned int hash_val) {
			return hash_val % context.num_partitions;
		}

}
