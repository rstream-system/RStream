/*
 * mining_phase.hpp
 *
 *  Created on: Jun 15, 2017
 *      Author: kai
 */

#ifndef CORE_MINING_PHASE_HPP_
#define CORE_MINING_PHASE_HPP_

#include "io_manager.hpp"
#include "buffer_manager.hpp"
#include "concurrent_queue.hpp"
#include "constants.hpp"

#include "meta_info.hpp"
#include "pattern.hpp"

namespace RStream {


	class MPhase {
		const Engine & context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

	public:
		// num of bytes for in_update_tuple
		int sizeof_in_tuple;

		virtual bool filter_join(std::vector<Element_In_Tuple> & update_tuple) = 0;
		virtual bool filter_collect(std::vector<Element_In_Tuple> & update_tuple) = 0;


		MPhase(Engine & e) : context(e) {
			sizeof_in_tuple = 0;
		}

		virtual ~MPhase() {}


		void atomic_init(){
			atomic_num_producers = context.num_exec_threads;
			atomic_partition_id = -1;
			atomic_partition_number = context.num_partitions;
		}

		/** join update stream with edge stream to generate non-shuffled update stream
		 * @param in_update_stream: which is shuffled
		 * @param out_update_stream: which is non-shuffled
		 */
		Update_Stream join_mining(Update_Stream in_update_stream) {
			atomic_init();
			int sizeof_out_tuple = sizeof_in_tuple + sizeof(Element_In_Tuple);

			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

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

		Update_Stream init() {
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

		// gen shuffled init update stream based on edge partitions
		Update_Stream init_shuffle_all_keys() {
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
		Update_Stream shuffle_all_keys(Update_Stream in_update_stream) {
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

		Update_Stream collect(Update_Stream in_update_stream) {
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
		Update_Stream join_all_keys(Update_Stream in_update_stream) {
			atomic_init();
			int sizeof_out_tuple = sizeof_in_tuple + sizeof(Element_In_Tuple);

			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

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


		void printout_upstream(Update_Stream in_update_stream){
			std::cout << "Number of tuples in "<< in_update_stream << ": \t" << get_count(in_update_stream) << std::endl;
		}

		int get_count(Update_Stream in_update_stream){
			int count_total = 0;
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


	private:
		// each exec thread generates a join producer
		void join_all_keys_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				std::cout << partition_id << std::endl;
				print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_update > 0 && fd_edge > 0 );

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);
				long edge_file_size = io_manager::get_filesize(fd_edge);

				// edges are fully loaded into memory
//				char * edge_local_buf = new char[edge_file_size];
				char * edge_local_buf = (char *)malloc(edge_file_size);
				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size, 0);

				// build edge hashmap
				const int n_vertices = context.vertex_intervals[partition_id].second - context.vertex_intervals[partition_id].first + 1;
				int vertex_start = context.vertex_intervals[partition_id].first;
				assert(n_vertices > 0 && vertex_start >= 0);
				std::vector<Element_In_Tuple> edge_hashmap[n_vertices];
				build_edge_hashmap(edge_local_buf, edge_hashmap, edge_file_size, vertex_start);

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
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
						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

						// get key index
						BYTE key_index = get_key_index(in_update_tuple);
						assert(key_index >= 0 && key_index < in_update_tuple.size());

						// get vertex_id as the key to index edge hashmap
						VertexId key = in_update_tuple.at(key_index).vertex_id;

						for(Element_In_Tuple& element : edge_hashmap[key - vertex_start]) {
							// generate a new out update tuple
							gen_an_out_update(in_update_tuple, element, key_index);

							// remove automorphism, only keep one unique tuple.
							if(Pattern::is_automorphism(in_update_tuple))
								continue;

							shuffle_on_all_keys(in_update_tuple, buffers_for_shuffle);

							in_update_tuple.pop_back();
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

		static void printout_edgehashmap(std::vector<Element_In_Tuple>* edge_hashmap, VertexId n_vertices){
			for(int i = 0; i < n_vertices; ++i){
				std::cout << i << ": " << edge_hashmap[i] << std::endl;
			}
		}

		// each exec thread generates a join producer
		void join_mining_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				std::cout << "partition at join-mining: " << partition_id << std::endl;
				print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_update > 0 && fd_edge > 0 );

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);
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

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
				int streaming_counter = update_file_size / real_io_size + 1;
//				std::cout << "streaming counter: " << streaming_counter << std::endl;

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
						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);
//						std::cout << in_update_tuple << std::endl;

						// get key index
						BYTE key_index = get_key_index(in_update_tuple);
						assert(key_index >= 0 && key_index < in_update_tuple.size());

						// get vertex_id as the key to index edge hashmap
						VertexId key = in_update_tuple.at(key_index).vertex_id;

						for(Element_In_Tuple element : edge_hashmap[key - vertex_start]) {
							// generate a new out update tuple
							gen_an_out_update(in_update_tuple, element, key_index);
//							std::cout << in_update_tuple  << " --> " << Pattern::is_automorphism(in_update_tuple)
//								<< ", " << filter_join(in_update_tuple) << std::endl;

							// remove automorphism, only keep one unique tuple.
							if(!Pattern::is_automorphism(in_update_tuple) && !filter_join(in_update_tuple)){
								assert(partition_id == get_global_buffer_index(key));
								insert_tuple_to_buffer(partition_id, in_update_tuple, buffers_for_shuffle);
							}

							in_update_tuple.pop_back();
						}

//						std::cout << std::endl;
					}
				}

				free(update_local_buf);
				free(edge_local_buf);

				close(fd_update);
				close(fd_edge);
			}

			atomic_num_producers--;

//			std::cout << "end producer." << std::endl;
		}


		void insert_tuple_to_buffer(int partition_id, std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle) {
			char* out_update = reinterpret_cast<char*>(in_update_tuple.data());
			global_buffer_for_mining* global_buf = buffer_manager_for_mining::get_global_buffer_for_mining(buffers_for_shuffle, context.num_partitions, partition_id);
			global_buf->insert(out_update);
		}


		void shuffle_all_keys_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				std::cout << partition_id << std::endl;
				print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
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
						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

						shuffle_on_all_keys(in_update_tuple, buffers_for_shuffle);
					}
				}

				free(update_local_buf);
				close(fd_update);
			}

			atomic_num_producers--;

		}


		void collect_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				std::cout << partition_id << std::endl;
				print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + "\n");

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				long real_io_size = get_real_io_size(IO_SIZE, sizeof_in_tuple);
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
						get_an_in_update(update_local_buf + pos, in_update_tuple, sizeof_in_tuple);

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

		void init_producer(global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
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
						LabeledEdge & e = *(LabeledEdge*)(edge_local_buf + pos);
//						std::cout << e << std::endl;

						std::vector<Element_In_Tuple> out_update_tuple;
						out_update_tuple.push_back(Element_In_Tuple(e.src, 0, e.src_label));
						out_update_tuple.push_back(Element_In_Tuple(e.target, 0, e.target_label));

						// shuffle on both src and target
						if(!Pattern::is_automorphism(out_update_tuple)){
							insert_tuple_to_buffer(partition_id, out_update_tuple, buffers_for_shuffle);
						}
					}
				}

				free(edge_local_buf);
				close(fd_edge);
			}

			atomic_num_producers--;
		}

		void shuffle_all_keys_producer_init(global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
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
						LabeledEdge & e = *(LabeledEdge*)(edge_local_buf + pos);
//						std::cout << e << std::endl;

						std::vector<Element_In_Tuple> out_update_tuple;
						out_update_tuple.push_back(Element_In_Tuple(e.src, 0, e.src_label));
						out_update_tuple.push_back(Element_In_Tuple(e.target, 0, e.target_label));

						// shuffle on both src and target
						if(!Pattern::is_automorphism(out_update_tuple)){
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
		void consumer(Update_Stream out_update_stream, global_buffer_for_mining ** buffers_for_shuffle) {
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

		void shuffle_on_all_keys(std::vector<Element_In_Tuple> & out_update_tuple, global_buffer_for_mining ** buffers_for_shuffle) {
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


		void gen_an_out_update(std::vector<Element_In_Tuple> & in_update_tuple, Element_In_Tuple & element, BYTE history) {
			Element_In_Tuple new_element(element.vertex_id, element.edge_label, element.vertex_label, history);
			in_update_tuple.push_back(new_element);
		}




		// key index is always stored in the first element of the vector
		BYTE get_key_index(std::vector<Element_In_Tuple> & in_update_tuple) {
			return in_update_tuple.at(0).key_index;
		}

		void set_key_index(std::vector<Element_In_Tuple> & out_update_tuple, int new_key_index) {
			out_update_tuple.at(0).key_index = new_key_index;
		}


		// TODO: do we need to store src.label?
		void build_edge_hashmap(char * edge_buf, std::vector<Element_In_Tuple> * edge_hashmap, size_t edge_file_size, int start_vertex) {
			// for each edge
			for(size_t pos = 0; pos < edge_file_size; pos += context.edge_unit) {
				// get a labeled edge
				LabeledEdge e = *(LabeledEdge*)(edge_buf + pos);
				// e.src is the key
				edge_hashmap[e.src - start_vertex].push_back(Element_In_Tuple(e.target, (BYTE)0, e.target_label));
			}
		}

		int get_global_buffer_index(VertexId key) {
//			int partition_id = (key - 1) / context.num_vertices_per_part;
//			return partition_id < (context.num_partitions - 1) ? partition_id : (context.num_partitions - 1);
			return meta_info::get_index(key, context);
		}

	public:
		static long get_real_io_size(long io_size, int size_of_unit){
			long real_io_size = io_size - io_size % size_of_unit;
			assert(real_io_size % size_of_unit == 0);
			return real_io_size;
		}

		static void get_an_in_update(char * update_local_buf, std::vector<Element_In_Tuple> & tuple, int sizeof_in_tuple) {
			for(int index = 0; index < sizeof_in_tuple; index += sizeof(Element_In_Tuple)) {
				Element_In_Tuple & element = *(Element_In_Tuple*)(update_local_buf + index);
				tuple.push_back(element);
			}
		}


};
}



#endif /* CORE_MINING_PHASE_HPP_ */
