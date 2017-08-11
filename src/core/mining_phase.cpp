/*
 * mining_phase.cpp
 *
 *  Created on: Aug 3, 2017
 *      Author: icuzzq
 */


#include "mining_phase.hpp"


namespace RStream {

		MPhase::MPhase(Engine & e, unsigned int maxsize) : context(e) {
			max_size = maxsize;
			sizeof_in_tuple = 0;
		}

		MPhase::~MPhase() {}


		void MPhase::atomic_init(){
			atomic_num_producers = context.num_exec_threads;
			atomic_partition_id = -1;
			atomic_partition_number = context.num_partitions;
		}


		/** join update stream with edge stream to generate non-shuffled update stream
		 * @param in_update_stream: which is shuffled
		 * @param out_update_stream: which is non-shuffled
		 */
		Update_Stream MPhase::join_all_keys_nonshuffle(Update_Stream in_update_stream) {
			atomic_init();
			int sizeof_out_tuple = sizeof_in_tuple + sizeof(Element_In_Tuple);

			Update_Stream update_c = Engine::update_count++;


			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_out_tuple);

			//load all edges in memory
			concurrent_queue<int> * read_task_queue = new concurrent_queue<int>(context.num_partitions);
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				read_task_queue->push(partition_id);
			}
			std::vector<Element_In_Tuple>* edge_hashmap = new std::vector<Element_In_Tuple>[context.num_vertices];
			std::vector<std::thread> read_threads;
			for(int i = 0; i < context.num_threads; i++)
				read_threads.push_back( std::thread([=] { this->edges_loader(edge_hashmap, read_task_queue); } ));

			for(auto &t : read_threads)
				t.join();

			// exec threads will produce updates and push into shuffle buffers
			concurrent_queue<std::tuple<int, long, long>>* task_queue = divide_tasks(context.num_partitions, context.filename, in_update_stream, sizeof_in_tuple, CHUNK_SIZE);
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->join_allkeys_nonshuffle_tuple_producer(in_update_stream, buffers_for_shuffle, task_queue, edge_hashmap); } ));

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

			delete[] edge_hashmap;
			delete read_task_queue;

			sizeof_in_tuple = sizeof_out_tuple;

			return update_c;
		}

		Update_Stream MPhase::join_all_keys_nonshuffle_clique(Update_Stream in_update_stream) {
			atomic_init();
			int sizeof_out_tuple = sizeof_in_tuple + sizeof(Base_Element);

			Update_Stream update_c = Engine::update_count++;


			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_out_tuple);

			//load all edges in memory
			concurrent_queue<int> * read_task_queue = new concurrent_queue<int>(context.num_partitions);
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				read_task_queue->push(partition_id);
			}
			std::vector<Base_Element>* edge_hashmap = new std::vector<Base_Element>[context.num_vertices];
			std::vector<std::thread> read_threads;
			for(int i = 0; i < context.num_threads; i++)
				read_threads.push_back( std::thread([=] { this->edges_loader(edge_hashmap, read_task_queue); } ));

			for(auto &t : read_threads)
				t.join();

			// exec threads will produce updates and push into shuffle buffers
			concurrent_queue<std::tuple<int, long, long>>* task_queue = divide_tasks(context.num_partitions, context.filename, in_update_stream, sizeof_in_tuple, CHUNK_SIZE);
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->join_allkeys_nonshuffle_tuple_producer_clique(in_update_stream, buffers_for_shuffle, task_queue, edge_hashmap); } ));

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

			delete[] edge_hashmap;
			delete read_task_queue;

			sizeof_in_tuple = sizeof_out_tuple;

			return update_c;
		}

		void MPhase::edges_loader(std::vector<Base_Element>* edge_hashmap, concurrent_queue<int> * read_task_queue){
			int partition_id = -1;
			while(read_task_queue->test_pop_atomic(partition_id)){
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_edge > 0);
				long edge_file_size = io_manager::get_filesize(fd_edge);

				// edges are fully loaded into memory
				char * edge_local_buf = (char *)malloc(edge_file_size);
				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size, 0);

				// build edge hashmap
				build_edge_hashmap(edge_local_buf, edge_hashmap, edge_file_size, 0);

				free(edge_local_buf);
				close(fd_edge);
			}
		}

		void MPhase::edges_loader(std::vector<Element_In_Tuple>* edge_hashmap, concurrent_queue<int> * read_task_queue){
			int partition_id = -1;
			while(read_task_queue->test_pop_atomic(partition_id)){
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_edge > 0);
				long edge_file_size = io_manager::get_filesize(fd_edge);

				// edges are fully loaded into memory
				char * edge_local_buf = (char *)malloc(edge_file_size);
				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size, 0);

				// build edge hashmap
				build_edge_hashmap(edge_local_buf, edge_hashmap, edge_file_size, 0);

				free(edge_local_buf);
				close(fd_edge);
			}
		}

		/** join update stream with edge stream to generate non-shuffled update stream
		 * @param in_update_stream: which is shuffled
		 * @param out_update_stream: which is non-shuffled
		 */
		Update_Stream MPhase::join_mining(Update_Stream in_update_stream) {
			atomic_init();
			int sizeof_out_tuple = sizeof_in_tuple + sizeof(Element_In_Tuple);

			Update_Stream update_c = Engine::update_count++;

//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
//				task_queue->push(partition_id);
//			}

			concurrent_queue<std::tuple<int, long, long>>* task_queue = divide_tasks(context.num_partitions, context.filename, in_update_stream, sizeof_in_tuple, CHUNK_SIZE);

			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_out_tuple);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->join_mining_producer(in_update_stream, buffers_for_shuffle, task_queue); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&MPhase::consumer, this, update_c, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

//			std::cout << "finish producers." << std::endl;

			for(auto &t : write_threads)
				t.join();

//			std::cout << "finish consumers." << std::endl;

			delete[] buffers_for_shuffle;
			delete task_queue;

			sizeof_in_tuple = sizeof_out_tuple;

			return update_c;
		}

		Update_Stream MPhase::init() {
			atomic_init();
			sizeof_in_tuple = 2 * sizeof(Element_In_Tuple);

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
				exec_threads.push_back( std::thread([=] { this->init_producer(buffers_for_shuffle, task_queue); } ));

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

		Update_Stream MPhase::init_clique() {
			atomic_init();
			sizeof_in_tuple = 2 * sizeof(Base_Element);

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
				exec_threads.push_back( std::thread([=] { this->init_clique_producer(buffers_for_shuffle, task_queue); } ));

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


		// gen shuffled init update stream based on edge partitions
		Update_Stream MPhase::init_shuffle_all_keys() {
			atomic_init();
			sizeof_in_tuple = 2 * sizeof(Element_In_Tuple);

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
				exec_threads.push_back( std::thread([=] { this->shuffle_all_keys_producer_init(buffers_for_shuffle, task_queue); } ));

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


		/** shuffle all keys on input update stream (in_update_stream)
		 * @param in_update_stream: which is non-shuffled
		 * @param out_update_stream: which is shuffled
		 */
		Update_Stream MPhase::shuffle_all_keys(Update_Stream in_update_stream) {
			atomic_init();

			Update_Stream update_c = Engine::update_count++;

//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
//				task_queue->push(partition_id);
//			}

			concurrent_queue<std::tuple<int, long, long>>* task_queue = divide_tasks(context.num_partitions, context.filename, in_update_stream, sizeof_in_tuple, CHUNK_SIZE);

			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_in_tuple);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->shuffle_all_keys_producer(in_update_stream, buffers_for_shuffle, task_queue); } ));

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

		Update_Stream MPhase::collect(Update_Stream in_update_stream) {
			atomic_init();

			Update_Stream update_c = Engine::update_count++;

//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
//				task_queue->push(partition_id);
//			}

			concurrent_queue<std::tuple<int, long, long>>* task_queue = divide_tasks(context.num_partitions, context.filename, in_update_stream, sizeof_in_tuple, CHUNK_SIZE);

			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_in_tuple);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->collect_producer(in_update_stream, buffers_for_shuffle, task_queue); } ));

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


		/* join update stream with edge stream, shuffle on all keys
		 * @param in_update_stream -input file for update stream
		 * @param out_update_stream -output file for update stream
		 * */
		Update_Stream MPhase::join_all_keys(Update_Stream in_update_stream) {
			atomic_init();
			int sizeof_out_tuple = sizeof_in_tuple + sizeof(Element_In_Tuple);

			Update_Stream update_c = Engine::update_count++;

//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
//				task_queue->push(partition_id);
//			}

			concurrent_queue<std::tuple<int, long, long>>* task_queue = divide_tasks(context.num_partitions, context.filename, in_update_stream, sizeof_in_tuple, CHUNK_SIZE);

			// allocate global buffers for shuffling
			global_buffer_for_mining ** buffers_for_shuffle = buffer_manager_for_mining::get_global_buffers_for_mining(context.num_partitions, sizeof_out_tuple);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->join_all_keys_producer(in_update_stream, buffers_for_shuffle, task_queue); } ));

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

			sizeof_in_tuple = sizeof_out_tuple;

			return update_c;
		}


		void MPhase::printout_upstream(Update_Stream in_update_stream){
//			std::cout << "Number of tuples in update "<< in_update_stream << ": \t" << get_count(in_update_stream) << std::endl;
//			std::cout << "Size of tuple: \t" << sizeof_in_tuple << std::endl;
			unsigned int count = get_count(in_update_stream);
			std::cout << "u, " << in_update_stream << ", " << count << ", " << sizeof_in_tuple << ", " << (count * sizeof_in_tuple) << std::endl;
		}

		void MPhase::delete_upstream(Update_Stream in_update_stream){
			delete_upstream_static(in_update_stream, context.num_partitions, context.filename);
		}


		unsigned int MPhase::get_count(Update_Stream in_update_stream){
			unsigned int count_total = 0;
			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);

				count_total += update_file_size / sizeof_in_tuple;
			}

			return count_total;
		}


		// each exec thread generates a join producer
		void MPhase::join_all_keys_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue) {
			std::tuple<int, long, long> task_id (-1, -1, -1);

			// pop from queue
			while(task_queue->test_pop_atomic(task_id)){
				int partition_id = std::get<0>(task_id);
				long offset_task = std::get<1>(task_id);
				long size_task = std::get<2>(task_id);
				assert(partition_id != -1 && offset_task != -1 && size_task != -1);

				Logger::print_thread_info_locked("as a (join-all-keys) producer dealing with partition " + get_string_task_tuple(task_id) + "\n");


				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_edge > 0);
				long edge_file_size = io_manager::get_filesize(fd_edge);

				// edges are fully loaded into memory
				char * edge_local_buf = (char *)malloc(edge_file_size);
				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size, 0);

				// build edge hashmap
				const int n_vertices = context.vertex_intervals[partition_id].second - context.vertex_intervals[partition_id].first + 1;
				int vertex_start = context.vertex_intervals[partition_id].first;
				assert(n_vertices > 0 && vertex_start >= 0);
				std::vector<Element_In_Tuple> edge_hashmap[n_vertices];
				build_edge_hashmap(edge_local_buf, edge_hashmap, edge_file_size, vertex_start);


				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);
				// get file size
//				long update_file_size = io_manager::get_filesize(fd_update);
				long update_file_size = size_task;

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
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
//						// get an in_update_tuple
//						int cap = sizeof_in_tuple / sizeof(Element_In_Tuple);
//						std::unordered_set<VertexId> vertices_set;
//						vertices_set.reserve(cap);
//						std::vector<Element_In_Tuple> in_update_tuple;
//						in_update_tuple.reserve(cap + 1);
//						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple, vertices_set);
//
//						// get key index
//						BYTE key_index = get_key_index(in_update_tuple);
//						assert(key_index >= 0 && key_index < in_update_tuple.size());
//
//						// get vertex_id as the key to index edge hashmap
//						VertexId key = in_update_tuple.at(key_index).vertex_id;

						// get an in_update_tuple
						std::unordered_set<VertexId> vertices_set;
						MTuple_join in_update_tuple(sizeof_in_tuple);
						get_an_in_update(update_local_buf + pos, in_update_tuple, vertices_set);
//						std::cout << in_update_tuple << std::endl;

						// get key index
						BYTE key_index = get_key_index(in_update_tuple);
						assert(key_index >= 0 && key_index < in_update_tuple.get_size());

						// get vertex_id as the key to index edge hashmap
						VertexId key = in_update_tuple.at(key_index).vertex_id;

						for(Element_In_Tuple element : edge_hashmap[key - vertex_start]) {
							// generate a new out update tuple
							Element_In_Tuple new_element(element.vertex_id, (BYTE)0, element.edge_label, element.vertex_label, key_index);
							bool vertex_existed = gen_an_out_update(in_update_tuple, new_element, key_index, vertices_set);

							// remove automorphism, only keep one unique tuple.
							if(!filter_join(in_update_tuple) && !Pattern::is_automorphism(in_update_tuple, vertex_existed)){
//								assert(partition_id == get_global_buffer_index(key));
								shuffle_on_all_keys(in_update_tuple, buffers_for_shuffle);
							}

//							in_update_tuple.pop_back();
							in_update_tuple.pop();
						}

					}
				}

				free(update_local_buf);
				free(edge_local_buf);

				close(fd_update);
				close(fd_edge);
			}

			atomic_num_producers--;
		}



//		// each exec thread generates a join producer
//		void MPhase::join_allkeys_nonshuffle_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, std::vector<Element_In_Tuple> * edge_hashmap) {
//			std::tuple<int, long, long> task_id (-1, -1, -1);
//
//			// pop from queue
//			while(task_queue->test_pop_atomic(task_id)){
//				int partition_id = std::get<0>(task_id);
//				long offset_task = std::get<1>(task_id);
//				long size_task = std::get<2>(task_id);
//				assert(partition_id != -1 && offset_task != -1 && size_task != -1);
//
//				Logger::print_thread_info_locked("as a (join-all-keys-nonshuffle) producer dealing with partition " + get_string_task_tuple(task_id) + "\n");
//				int target_partition = 0;
//
//				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
//				assert(fd_update > 0);
//
//				// get file size
////				long update_file_size = io_manager::get_filesize(fd_update);
//				long update_file_size = size_task;
//
//				// streaming updates
//				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
//				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
//				int streaming_counter = update_file_size / real_io_size + 1;
////				std::cout << "streaming counter: " << streaming_counter << std::endl;
//
//				long valid_io_size = 0;
////				long offset = 0;
//				long offset = offset_task;
//
//				// for all streaming updates
//				for(int counter = 0; counter < streaming_counter; counter++) {
//					// last streaming
//					if(counter == streaming_counter - 1)
//						// TODO: potential overflow?
//						valid_io_size = update_file_size - real_io_size * (streaming_counter - 1);
//					else
//						valid_io_size = real_io_size;
//
//					assert(valid_io_size % sizeof_in_tuple == 0);
//
//					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
//					offset += valid_io_size;
//
//					// streaming updates in, do hash join
//					for(long pos = 0; pos < valid_io_size; pos += sizeof_in_tuple) {
//						// get an in_update_tuple
//						int cap = sizeof_in_tuple / sizeof(Element_In_Tuple);
//						std::unordered_set<VertexId> vertices_set;
//						vertices_set.reserve(cap);
//						std::vector<Element_In_Tuple> in_update_tuple;
//						in_update_tuple.reserve(cap + 1);
//						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple, vertices_set);
////						std::cout << in_update_tuple << std::endl;
//
//						std::unordered_set<VertexId> set;
//						for(unsigned int i = 0; i < in_update_tuple.size(); ++i){
//							VertexId id = in_update_tuple.at(i).vertex_id;
//
//							// check if vertex id exsited already
//							if(set.find(id) == set.end()){
//								set.insert(id);
//
//								for(Element_In_Tuple element : edge_hashmap[id]) {
//									// generate a new out update tuple
//									bool vertex_existed = gen_an_out_update(in_update_tuple, element, (BYTE)i, vertices_set);
//		//							std::cout << in_update_tuple  << " --> " << Pattern::is_automorphism(in_update_tuple)
//		//								<< ", " << filter_join(in_update_tuple) << std::endl;
//
//									// remove automorphism, only keep one unique tuple.
//									if(!filter_join(in_update_tuple) && !Pattern::is_automorphism(in_update_tuple, vertex_existed)){
////										insert_tuple_to_buffer(partition_id, in_update_tuple, buffers_for_shuffle);
//
//										insert_tuple_to_buffer(target_partition++, in_update_tuple, buffers_for_shuffle);
//										if(target_partition == context.num_partitions)
//											target_partition = 0;
//									}
//
//									in_update_tuple.pop_back();
//								}
//							}
//
//
//						}
//					}
//				}
//
//				free(update_local_buf);
//				close(fd_update);
//			}
//
//			atomic_num_producers--;
//
////			std::cout << "end producer." << std::endl;
//		}

		// each exec thread generates a join producer
		void MPhase::join_allkeys_nonshuffle_tuple_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, std::vector<Element_In_Tuple> * edge_hashmap) {
			std::tuple<int, long, long> task_id (-1, -1, -1);

			// pop from queue
			while(task_queue->test_pop_atomic(task_id)){
				int partition_id = std::get<0>(task_id);
				long offset_task = std::get<1>(task_id);
				long size_task = std::get<2>(task_id);
				assert(partition_id != -1 && offset_task != -1 && size_task != -1);

				Logger::print_thread_info_locked("as a (join-all-keys-nonshuffle-tuple) producer dealing with partition " + get_string_task_tuple(task_id) + "\n");
				int target_partition = 0;

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
//				long update_file_size = io_manager::get_filesize(fd_update);
				long update_file_size = size_task;

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
				int streaming_counter = update_file_size / real_io_size + 1;
//				std::cout << "streaming counter: " << streaming_counter << std::endl;

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
						std::unordered_set<VertexId> vertices_set;
						MTuple_join in_update_tuple(sizeof_in_tuple);
						get_an_in_update(update_local_buf + pos, in_update_tuple, vertices_set);
//						std::cout << in_update_tuple << std::endl;

						std::unordered_set<VertexId> set;
//						set.reserve(vertices_set.size());
						for(unsigned int i = 0; i < in_update_tuple.get_size(); ++i){
							VertexId id = in_update_tuple.at(i).vertex_id;

							// check if vertex id exsited already
							if(set.find(id) == set.end()){
								set.insert(id);

								for(Element_In_Tuple& element : edge_hashmap[id]) {
									// generate a new out update tuple
									Element_In_Tuple new_element(element.vertex_id, (BYTE)0, element.edge_label, element.vertex_label, (BYTE)i);
									bool vertex_existed = gen_an_out_update(in_update_tuple, new_element, (BYTE)i, vertices_set);
		//							std::cout << in_update_tuple  << " --> " << Pattern::is_automorphism(in_update_tuple)
		//								<< ", " << filter_join(in_update_tuple) << std::endl;

									// remove automorphism, only keep one unique tuple.
									if(!filter_join(in_update_tuple) && !Pattern::is_automorphism(in_update_tuple, vertex_existed)){
//										insert_tuple_to_buffer(partition_id, in_update_tuple, buffers_for_shuffle);

										insert_tuple_to_buffer(target_partition++, in_update_tuple, buffers_for_shuffle);
										if(target_partition == context.num_partitions)
											target_partition = 0;
									}

									in_update_tuple.pop();
								}
							}
						}
					}
				}

				free(update_local_buf);
				close(fd_update);
			}

			atomic_num_producers--;
		}


		void MPhase::join_allkeys_nonshuffle_tuple_producer_clique(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, std::vector<Base_Element> * edge_hashmap) {
			std::tuple<int, long, long> task_id (-1, -1, -1);

			// pop from queue
			while(task_queue->test_pop_atomic(task_id)){
				int partition_id = std::get<0>(task_id);
				long offset_task = std::get<1>(task_id);
				long size_task = std::get<2>(task_id);
				assert(partition_id != -1 && offset_task != -1 && size_task != -1);

				Logger::print_thread_info_locked("as a (join-all-keys-nonshuffle-tuple-clique) producer dealing with partition " + get_string_task_tuple(task_id) + "\n");
				int target_partition = 0;

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
//				long update_file_size = io_manager::get_filesize(fd_update);
				long update_file_size = size_task;

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
				int streaming_counter = update_file_size / real_io_size + 1;
//				std::cout << "streaming counter: " << streaming_counter << std::endl;

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
						MTuple_join_simple in_update_tuple(sizeof_in_tuple);
						get_an_in_update(update_local_buf + pos, in_update_tuple);

						for(unsigned int i = 0; i < in_update_tuple.get_size(); ++i){
							VertexId id = in_update_tuple.at(i).id;

							for(Base_Element& element : edge_hashmap[id]) {
								// generate a new out update tuple
//								Base_Element new_element(element.id);
								gen_an_out_update(in_update_tuple, element);
//								std::cout << in_update_tuple  << " --> " << filter_join_clique(in_update_tuple) << std::endl;

								// remove automorphism, only keep one unique tuple.
								if(!filter_join_clique(in_update_tuple)){
									shuffle(in_update_tuple, buffers_for_shuffle, partition_id);
								}

								in_update_tuple.pop();
							}
						}
					}
				}

				free(update_local_buf);
				close(fd_update);
			}

			atomic_num_producers--;
		}

		// each exec thread generates a join producer
		void MPhase::join_mining_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue) {
			std::tuple<int, long, long> task_id (-1, -1, -1);

			// pop from queue
			while(task_queue->test_pop_atomic(task_id)){
				int partition_id = std::get<0>(task_id);
				long offset_task = std::get<1>(task_id);
				long size_task = std::get<2>(task_id);
				assert(partition_id != -1 && offset_task != -1 && size_task != -1);

				Logger::print_thread_info_locked("as a (join-mining) producer dealing with partition " + get_string_task_tuple(task_id) + "\n");


				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_edge > 0);
				long edge_file_size = io_manager::get_filesize(fd_edge);

				// edges are fully loaded into memory
				char * edge_local_buf = (char *)malloc(edge_file_size);
				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size, 0);

				// build edge hashmap
				VertexId n_vertices = context.vertex_intervals[partition_id].second - context.vertex_intervals[partition_id].first + 1;
				VertexId vertex_start = context.vertex_intervals[partition_id].first;
				assert(n_vertices > 0 && vertex_start >= 0);
				std::vector<Element_In_Tuple> edge_hashmap[n_vertices];
				build_edge_hashmap(edge_local_buf, edge_hashmap, edge_file_size, vertex_start);
//				//for debugging
//				std::cout << "finish edge hash building" << std::endl;
//				printout_edgehashmap(edge_hashmap, n_vertices);


				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
//				long update_file_size = io_manager::get_filesize(fd_update);
				long update_file_size = size_task;

				// streaming updates
					char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
					long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
					int streaming_counter = update_file_size / real_io_size + 1;
	//				std::cout << "streaming counter: " << streaming_counter << std::endl;

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
						std::unordered_set<VertexId> vertices_set;
						MTuple_join in_update_tuple(sizeof_in_tuple);
						get_an_in_update(update_local_buf + pos, in_update_tuple, vertices_set);
//						std::cout << in_update_tuple << std::endl;

						// get key index
						BYTE key_index = get_key_index(in_update_tuple);
						assert(key_index >= 0 && key_index < in_update_tuple.get_size());

						// get vertex_id as the key to index edge hashmap
						VertexId key = in_update_tuple.at(key_index).vertex_id;

						for(Element_In_Tuple element : edge_hashmap[key - vertex_start]) {
							// generate a new out update tuple
							Element_In_Tuple new_element(element.vertex_id, (BYTE)0, element.edge_label, element.vertex_label, key_index);
							bool vertex_existed = gen_an_out_update(in_update_tuple, new_element, key_index, vertices_set);
//							std::cout << in_update_tuple  << " --> " << Pattern::is_automorphism(in_update_tuple)
//								<< ", " << filter_join(in_update_tuple) << std::endl;

							// remove automorphism, only keep one unique tuple.
							if(!filter_join(in_update_tuple) && !Pattern::is_automorphism(in_update_tuple, vertex_existed)){
//								assert(partition_id == get_global_buffer_index(key));
								insert_tuple_to_buffer(partition_id, in_update_tuple, buffers_for_shuffle);
							}

							in_update_tuple.pop();
						}
					}
				}

				free(update_local_buf);
				free(edge_local_buf);

				close(fd_update);
				close(fd_edge);
			}

			atomic_num_producers--;
		}


		void MPhase::insert_tuple_to_buffer(int partition_id, std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
			char* out_update = reinterpret_cast<char*>(in_update_tuple.data());
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			global_buf->insert(out_update);
		}

		void MPhase::insert_tuple_to_buffer_clique(int partition_id, std::vector<Base_Element>& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
			char* out_update = reinterpret_cast<char*>(in_update_tuple.data());
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			global_buf->insert(out_update);
		}

//		void MPhase::insert_tuple_to_buffer(int partition_id, std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
//			char* out_update = reinterpret_cast<char*>(in_update_tuple.data());
//			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
//			global_buf->insert(out_update);
//		}

		void MPhase::insert_tuple_to_buffer(int partition_id, MTuple& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
			char* out_update = reinterpret_cast<char*>(in_update_tuple.get_elements());
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			global_buf->insert(out_update);
		}

		void MPhase::insert_tuple_to_buffer(int partition_id, MTuple_join& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
			char* out_elements = reinterpret_cast<char*>(in_update_tuple.get_elements());
			char* out_added = reinterpret_cast<char*>(in_update_tuple.get_added_element());
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			global_buf->insert(out_elements, out_added);
		}


		void MPhase::shuffle_all_keys_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue) {
			std::tuple<int, long, long> task_id (-1, -1, -1);

			// pop from queue
			while(task_queue->test_pop_atomic(task_id)){
				int partition_id = std::get<0>(task_id);
				long offset_task = std::get<1>(task_id);
				long size_task = std::get<2>(task_id);
				assert(partition_id != -1 && offset_task != -1 && size_task != -1);

				Logger::print_thread_info_locked("as a (shuffle-all-keys) producer dealing with partition " + get_string_task_tuple(task_id) + "\n");

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
//				long update_file_size = io_manager::get_filesize(fd_update);
				long update_file_size = size_task;

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
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
						MTuple in_update_tuple(sizeof_in_tuple);
						get_an_in_update(update_local_buf + pos, in_update_tuple);

						shuffle_on_all_keys(in_update_tuple, buffers_for_shuffle);
					}
				}

				free(update_local_buf);
				close(fd_update);
			}

			atomic_num_producers--;

		}


		void MPhase::collect_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue) {
			std::tuple<int, long, long> task_id (-1, -1, -1);

			// pop from queue
			while(task_queue->test_pop_atomic(task_id)){
				int partition_id = std::get<0>(task_id);
				long offset_task = std::get<1>(task_id);
				long size_task = std::get<2>(task_id);
				assert(partition_id != -1 && offset_task != -1 && size_task != -1);

				Logger::print_thread_info_locked("as a (collect) producer dealing with partition " + get_string_task_tuple(task_id) + "\n");

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
//				long update_file_size = io_manager::get_filesize(fd_update);
				long update_file_size = size_task;

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
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
//						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);
						MTuple in_update_tuple(sizeof_in_tuple);
						get_an_in_update(update_local_buf + pos, in_update_tuple);

						if(!filter_collect(in_update_tuple)){
							insert_tuple_to_buffer(partition_id, in_update_tuple, buffers_for_shuffle);
//							std::cerr << "remained: " << in_update_tuple << std::endl;
						}
					}
				}

				free(update_local_buf);
				close(fd_update);
			}

			atomic_num_producers--;

		}

		void MPhase::init_producer(global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				Logger::print_thread_info_locked("as a (init) producer dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_edge > 0 );

				// get edge file size
				long edge_file_size = io_manager::get_filesize(fd_edge);

				// streaming edges
				char * edge_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				int size_of_unit = context.edge_unit;
				long real_io_size = get_real_io_size(IO_SIZE, size_of_unit);
				int streaming_counter = edge_file_size / real_io_size + 1;

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = edge_file_size - real_io_size * (streaming_counter - 1);
					else
						valid_io_size = real_io_size;

//					std::cout << real_io_size << std::endl;
//					std::cout << edge_file_size << std::endl;
//					std::cout << size_of_unit << std::endl;
//					std::cout << valid_io_size << std::endl;
					assert(valid_io_size % size_of_unit == 0);

					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// for each streaming
					for(long pos = 0; pos < valid_io_size; pos += size_of_unit) {
						// get an labeled edge
						LabeledEdge e = *(LabeledEdge*)(edge_local_buf + pos);
//						std::cout << e << std::endl;

						std::vector<Element_In_Tuple> out_update_tuple;
						out_update_tuple.push_back(Element_In_Tuple(e.src, 0, e.src_label));
						out_update_tuple.push_back(Element_In_Tuple(e.target, 0, e.target_label));

						// shuffle on both src and target
						if(!Pattern::is_automorphism_init(out_update_tuple)){
							insert_tuple_to_buffer(partition_id, out_update_tuple, buffers_for_shuffle);
						}
					}
				}

				free(edge_local_buf);
				close(fd_edge);
			}

			atomic_num_producers--;
		}

		void MPhase::init_clique_producer(global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				Logger::print_thread_info_locked("as a (init-clique) producer dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_edge > 0 );

				// get edge file size
				long edge_file_size = io_manager::get_filesize(fd_edge);

				// streaming edges
				char * edge_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				int size_of_unit = context.edge_unit;
				long real_io_size = get_real_io_size(IO_SIZE, size_of_unit);
				int streaming_counter = edge_file_size / real_io_size + 1;

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = edge_file_size - real_io_size * (streaming_counter - 1);
					else
						valid_io_size = real_io_size;

//					std::cout << real_io_size << std::endl;
//					std::cout << edge_file_size << std::endl;
//					std::cout << size_of_unit << std::endl;
//					std::cout << valid_io_size << std::endl;
					assert(valid_io_size % size_of_unit == 0);

					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// for each streaming
					for(long pos = 0; pos < valid_io_size; pos += size_of_unit) {
						// get an labeled edge
						LabeledEdge e = *(LabeledEdge*)(edge_local_buf + pos);
//						std::cout << e << std::endl;

						std::vector<Base_Element> out_update_tuple;
						out_update_tuple.push_back(Base_Element(e.src));
						out_update_tuple.push_back(Base_Element(e.target));

						// shuffle on both src and target
						if(!Pattern::is_automorphism_init_clique(out_update_tuple)){
							insert_tuple_to_buffer_clique(partition_id, out_update_tuple, buffers_for_shuffle);
						}

					}
				}

				free(edge_local_buf);
				close(fd_edge);
			}

			atomic_num_producers--;
		}

		void MPhase::shuffle_all_keys_producer_init(global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				Logger::print_thread_info_locked("as a (shuffle-all-keys-init) producer dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_edge > 0 );

				// get edge file size
				long edge_file_size = io_manager::get_filesize(fd_edge);

				// streaming edges
				int size_of_unit = context.edge_unit;
				char * edge_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, size_of_unit);
				int streaming_counter = edge_file_size / real_io_size + 1;

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = edge_file_size - real_io_size * (streaming_counter - 1);
					else
						valid_io_size = real_io_size;

//					std::cout << real_io_size << std::endl;
//					std::cout << edge_file_size << std::endl;
//					std::cout << size_of_unit << std::endl;
//					std::cout << valid_io_size << std::endl;
					assert(valid_io_size % size_of_unit == 0);

					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// for each streaming
					for(long pos = 0; pos < valid_io_size; pos += size_of_unit) {
						// get an labeled edge
						LabeledEdge e = *(LabeledEdge*)(edge_local_buf + pos);
//						std::cout << e << std::endl;

						std::vector<Element_In_Tuple> out_update_tuple;
						out_update_tuple.push_back(Element_In_Tuple(e.src, 0, e.src_label));
						out_update_tuple.push_back(Element_In_Tuple(e.target, 0, e.target_label));

						// shuffle on both src and target
						if(!Pattern::is_automorphism_init(out_update_tuple)){
							shuffle_on_all_keys(out_update_tuple, buffers_for_shuffle);
						}

					}
				}

				free(edge_local_buf);
				close(fd_edge);
			}

			atomic_num_producers--;
		}


		// each writer thread generates a join_consumer
		void MPhase::consumer(Update_Stream out_update_stream, global_buffer_for_mining ** buffers_for_shuffle) {
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

		void MPhase::shuffle_on_all_keys(std::vector<Element_In_Tuple> & out_update_tuple, global_buffer_for_mining ** buffers_for_shuffle) {
			std::unordered_set<VertexId> vertex_set;
			// shuffle on all other keys
			for(unsigned i = 0; i < out_update_tuple.size(); i++) {
				VertexId key = out_update_tuple[i].vertex_id;

				// check if vertex id exsited already
				// DO NOT shuffle if vertex exsited
				if(vertex_set.find(key) == vertex_set.end()){
					vertex_set.insert(key);

					set_key_index(out_update_tuple, i);
					char* out_update = reinterpret_cast<char*>(out_update_tuple.data());

					int index = get_global_buffer_index(key);
					global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, index);
					global_buf->insert(out_update);

				}
			}
		}


		void MPhase::shuffle_on_all_keys(MTuple& out_update_tuple, global_buffer_for_mining ** buffers_for_shuffle) {
			std::unordered_set<VertexId> vertex_set;
			// shuffle on all other keys
			for(unsigned i = 0; i < out_update_tuple.get_size(); i++) {
				VertexId key = out_update_tuple.at(i).vertex_id;

				// check if vertex id exsited already
				// DO NOT shuffle if vertex exsited
				if(vertex_set.find(key) == vertex_set.end()){
					vertex_set.insert(key);

					set_key_index(out_update_tuple, i);
					char* out_update = reinterpret_cast<char*>(out_update_tuple.get_elements());

					int index = get_global_buffer_index(key);
					global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, index);
					global_buf->insert(out_update);

				}
			}
		}

		void MPhase::shuffle(MTuple_join_simple& out_update_tuple, global_buffer_for_mining ** buffers_for_shuffle, int partition_id) {
			unsigned int hash = out_update_tuple.get_hash();
			unsigned int index = hash % context.num_partitions;
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, index);

//			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			char* out_update = reinterpret_cast<char*>(out_update_tuple.get_elements());
			char* added = reinterpret_cast<char*>(out_update_tuple.get_added_element());
//			std::cout << *((Base_Element*)out_update) << std::endl;
//			std::cout << *((Base_Element*)added) << std::endl;
			global_buf->insert_simple(out_update, added);

		}

		void MPhase::gen_an_out_update(MTuple_join_simple & in_update_tuple, Base_Element & element) {
//			Element_In_Tuple* new_element = new Element_In_Tuple(element.vertex_id, (BYTE)0, element.edge_label, element.vertex_label, history);
			in_update_tuple.push(&element);
		}

		bool MPhase::gen_an_out_update(MTuple_join & in_update_tuple, Element_In_Tuple & element, BYTE history, std::unordered_set<VertexId>& vertices_set) {
			bool vertex_existed = true;
			auto num_vertices = vertices_set.size();
			if(vertices_set.find(element.vertex_id) == vertices_set.end()){
				num_vertices += 1;
				vertex_existed = false;
			}

//			Element_In_Tuple* new_element = new Element_In_Tuple(element.vertex_id, (BYTE)0, element.edge_label, element.vertex_label, history);
			in_update_tuple.push(&element);
			in_update_tuple.set_num_vertices(num_vertices);
			return vertex_existed;
		}


		bool MPhase::gen_an_out_update(std::vector<Element_In_Tuple> & in_update_tuple, Element_In_Tuple & element, BYTE history, std::unordered_set<VertexId>& vertices_set) {
			bool vertex_existed = true;
			auto num_vertices = vertices_set.size();
			if(vertices_set.find(element.vertex_id) == vertices_set.end()){
				num_vertices += 1;
				vertex_existed = false;
			}
			Element_In_Tuple new_element(element.vertex_id, (BYTE)num_vertices, element.edge_label, element.vertex_label, history);
			in_update_tuple.push_back(new_element);

			return vertex_existed;
		}


		// key index is always stored in the first element of the vector
		BYTE MPhase::get_key_index(std::vector<Element_In_Tuple> & in_update_tuple) {
			return in_update_tuple.at(0).key_index;
		}

		BYTE MPhase::get_key_index(MTuple_join & in_update_tuple) {
			return in_update_tuple.at(0).key_index;
		}

		void MPhase::set_key_index(std::vector<Element_In_Tuple> & out_update_tuple, int new_key_index) {
			out_update_tuple.at(0).key_index = new_key_index;
		}

		void MPhase::set_key_index(MTuple & out_update_tuple, int new_key_index) {
			out_update_tuple.at(0).key_index = new_key_index;
		}

		void MPhase::build_edge_hashmap(char * edge_buf, std::vector<Base_Element> * edge_hashmap, size_t edge_file_size, int start_vertex) {
			// for each edge
			for(size_t pos = 0; pos < edge_file_size; pos += context.edge_unit) {
				// get a labeled edge
				LabeledEdge e = *(LabeledEdge*)(edge_buf + pos);
				// e.src is the key
				if(e.src < e.target){
					edge_hashmap[e.src - start_vertex].push_back(Base_Element(e.target));
				}
			}
		}

		// TODO: do we need to store src.label?
		void MPhase::build_edge_hashmap(char * edge_buf, std::vector<Element_In_Tuple> * edge_hashmap, size_t edge_file_size, int start_vertex) {
			// for each edge
			for(size_t pos = 0; pos < edge_file_size; pos += context.edge_unit) {
				// get a labeled edge
				LabeledEdge e = *(LabeledEdge*)(edge_buf + pos);
				// e.src is the key
				edge_hashmap[e.src - start_vertex].push_back(Element_In_Tuple(e.target, (BYTE)0, e.target_label));
			}
		}

		int MPhase::get_global_buffer_index(VertexId key) {
			return meta_info::get_index(key, context);
		}


		unsigned int MPhase::get_num_vertices(std::vector<Element_In_Tuple> & update_tuple){
			return update_tuple.back().key_index;
		}

		unsigned int MPhase::get_num_vertices(MTuple & update_tuple){
			return update_tuple.get_num_vertices();
		}

}




