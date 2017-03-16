/*
 * engine.hpp
 *
 *  Created on: Mar 3, 2017
 *      Author: kai
 */

#ifndef CORE_ENGINE_HPP_
#define CORE_ENGINE_HPP_

//#include "../common/RStreamCommon.hpp"

#include "io_manager.hpp"
#include "buffer_manager.hpp"
#include "concurrent_queue.hpp"
#include "type.hpp"
#include "constants.hpp"

namespace RStream {
	enum class EdgeType {
		NO_WEIGHT,
		WITH_WEIGHT,
	};

//	template <typename VertexDataType, typename EdgeDataType, typename T>

	class engine {

		std::string filename;
		int num_threads;
		int num_write_threads;
		int num_exec_threads;

		int num_partitions;

		EdgeType edge_type;
		// sizeof each edge
		int edge_unit;


		std::atomic<int> atomic_num_producers;

		std::atomic<int> atomic_partition_id;

		std::atomic<int> atomic_partition_number;

	public:

		engine(std::string _filename) : filename(_filename), atomic_num_producers(0), atomic_partition_id(0) {
			num_threads = std::thread::hardware_concurrency();
//			num_threads = 4;

			// to be decided ?
			num_write_threads = num_threads > 2 ? 2 : 1;
			num_exec_threads = num_threads > 2 ? num_threads - 2 : 1;

			// read meta file, contains num of partitions, etc.
			FILE *meta_file = fopen((filename + ".meta").c_str(), "r");
			if(!meta_file) {
				std::cout << "meta file does not exit!" << std::endl;
				assert(false);
			}

			fscanf(meta_file, "%d %d", &num_partitions, &edge_type);


			fclose(meta_file);

			// size of each edge
			if(edge_type == EdgeType::NO_WEIGHT) {
				edge_unit = sizeof(VertexId) * 2;
			} else if(edge_type == EdgeType::WITH_WEIGHT) {
				edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
			}

			std::cout << "Number of partitions: " << num_partitions << std::endl;
//			std::cout << edge_type << std::endl;
			std::cout << "Number of bytes per edge: " << edge_unit << std::endl << std::endl;

			atomic_partition_number = num_partitions - 1;
		}

		void scatter(std::function<T*(Edge&)> generate_one_update) {
			concurrent_queue<int> * task_queue = new concurrent_queue<int>(num_partitions);

			// allocate global buffers for shuffling
			global_buffer<T> ** buffers_for_shuffle = buffer_manager<T>::get_global_buffers(num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
				int fd = open((filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				task_queue->push(fd);
			}

//			//for debugging only
//			scatter_producer(generate_one_update, buffers_for_shuffle, task_queue);
//			std::cout << "scatter done!" << std::endl;
//			scatter_consumer(buffers_for_shuffle);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < num_exec_threads; i++)
				exec_threads.push_back(std::thread(&engine::scatter_producer, this, generate_one_update, buffers_for_shuffle, task_queue));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < num_write_threads; i++)
				write_threads.push_back(std::thread(&engine::scatter_consumer, this, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
		}

//		void gather(std::function<void(Edge&)> apply_one_update) {
//			// a pair of <vertex, update_stream> for each partition
//			concurrent_queue<std::pair<int, int>> * task_queue = new concurrent_queue<std::pair<int, int>>(num_partitions);
//
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
//				int fd_vertex = open((filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDONLY);
//				int fd_update = open((filename + "." + std::to_string(partition_id) + ".update_stream").c_str(), O_RDONLY);
//				task_queue->push(std::make_pair(fd_vertex, fd_update));
//			}
//
//			// threads will load vertex and update, and apply update one by one
//			std::vector<std::thread> threads;
//			for(int i = 0; i < num_threads; i++)
//				threads.push_back(std::thread(&engine::gather_producer, this, apply_one_update, task_queue));
//
//			// join all threads
//			for(auto & t : threads)
//				t.join();
//		}

		void join() {

		}

	protected:

		// each exec thread generates a scatter_producer
		void scatter_producer(std::function<T*(Edge&)> generate_one_update,
				global_buffer<T> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			atomic_num_producers++;
			int fd = -1;
			// pop from queue
			while(task_queue->test_pop_atomic(fd)){
				assert(fd > 0);

				// get file size
				size_t file_size = io_manager::get_filesize(fd);
				print_thread_info_locked("as a producer dealing with " + std::to_string(fd) + " of size " + std::to_string(file_size) + "\n");

				// read from file to thread local buffer
				char * local_buf = new char[file_size];
				io_manager::read_from_file(fd, local_buf, file_size);

				// for each edge
				for(size_t pos = 0; pos < file_size; pos += edge_unit) {
					// get an edge
					Edge e = *(Edge*)(local_buf + pos);
//					std::cout << e << std::endl;

					// gen one update
					T * update_info = generate_one_update(e);
//					std::cout << update_info->target << std::endl;


					// insert into shuffle buffer accordingly
					int index = get_global_buffer_index(update_info);
					global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, num_partitions, index);
					global_buf->insert(update_info, index);
				}

//				std::cout << std::endl;
				close(fd);

			}
			atomic_num_producers--;

		}

		// each writer thread generates a scatter_consumer
		void scatter_consumer(global_buffer<T> ** buffers_for_shuffle) {
			while(atomic_num_producers != 0) {
				int i = (atomic_partition_id++) % num_partitions ;

//				//debugging info
//				print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");

				const char * file_name = (filename + "." + std::to_string(i) + ".update_stream").c_str();
				global_buffer<T>* g_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, num_partitions, i);
				g_buf->flush(file_name, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = atomic_partition_number--;
//				std::cout << i << std::endl;
				if(i >= 0){
//					//debugging info
//					print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");

					const char * file_name = (filename + "." + std::to_string(i) + ".update_stream").c_str();
					global_buffer<T>* g_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, num_partitions, i);
					g_buf->flush_end(file_name, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}

//		void gather_producer(std::function<void(T&)> apply_one_update,
//				concurrent_queue<std::pair<int, int>> * task_queue) {
//
//			// pop from queue
//			std::pair<int, int> fd_pair = task_queue->pop();
//			int fd_vertex = fd_pair.first;
//			int fd_update = fd_pair.second;
//
//			// get file size
//			size_t vertex_file_size = io_manager::get_filesize(fd_vertex);
//			size_t update_file_size = io_manager::get_filesize(fd_update);
//
//			// read from files to thread local buffer
//			char * vertex_local_buf = new char[vertex_file_size];
//			io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size);
//			char * update_local_buf = new char[update_file_size];
//			io_manager::read_from_file(fd_update, update_local_buf, update_file_size);
//
//			// for each update
//			for(long pos = 0; pos <= update_file_size; pos += sizeof(T)) {
//				// get an update
//				T & update = *(T*)(update_local_buf + pos);
////				apply_one_update(update, vertex_local_buf);
//			}
//
//			// write updated vertex value to disk
//			io_manager::write_to_file(fd_vertex, vertex_local_buf, vertex_file_size);
//
//			// delete
//			delete[] vertex_local_buf;
//			delete[] update_local_buf;
//		}

		void join_producer() {

		}

		void join_consumer() {

		}

		int get_global_buffer_index(T* update_info) {
//			return update_info->target;
			return 0;
		}

	};
}



#endif /* CORE_ENGINE_HPP_ */

