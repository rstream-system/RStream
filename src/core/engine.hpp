/*
 * engine.hpp
 *
 *  Created on: Mar 3, 2017
 *      Author: kai
 */

#ifndef CORE_ENGINE_HPP_
#define CORE_ENGINE_HPP_

#include <string>
#include <thread>
#include <fcntl.h>

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

	template <typename VertexDataType, typename EdgeDataType, typename T>

	class engine {

		std::string filename;
		int num_threads;
		int num_write_threads;
		int num_exec_threads;

		int num_partitions;

		EdgeType edge_type;
		// sizeof each edge
		int edge_unit;


	public:

		engine(std::string _filename) : filename(_filename) {
			num_threads = std::thread::hardware_concurrency();
			// to be decided ?
			num_write_threads = 2;
			num_exec_threads = num_threads - 2;

			// read meta file, contains num of partitions, etc.
			FILE *meta_file = fopen((filename + ".meta").c_str(), "r");
			if(!meta_file) {
				std::cout << "meta file does not exit!" << std::endl;
				assert(false);
			}

			fscanf(meta_file, "%d %d", &num_partitions, &edge_type);

			// size of each edge
			if(edge_type == EdgeType::NO_WEIGHT) {
				edge_unit = sizeof(VertexId) * 2;
			} else if(edge_type == EdgeType::WITH_WEIGHT) {
				edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
			}

		}

		void scatter(std::function<T*(Edge&)> generate_one_update) {
			concurrent_queue<int> * task_queue = new concurrent_queue<int>(65536);

			// allocate global buffers for shuffling
			global_buffer<T> ** buffers_for_shuffle = buffer_manager<T>::get_global_buffers(num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
				int fd = open((filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				task_queue->push(fd);
			}

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

		}

		void gather() {

		}

		void join() {

		}

	protected:

		// each exec thread generates a scatter_producer
		void scatter_producer(std::function<T*(Edge&)> generate_one_update,
				global_buffer<T> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {

			// pop from queue
			int fd = task_queue->pop();
			// get file size
			size_t file_size = io_manager::get_filesize(fd);

			// read from file to thread local buffer
			char * local_buf = (char *)malloc(file_size);
			io_manager::read_from_file(fd, local_buf, file_size);

			// for each edge
			for(long pos = 0; pos <= file_size; pos += edge_unit) {
				// get an edge
				Edge & e = *(Edge*)(local_buf + pos);
				// gen one update
				T * update_info = generate_one_update(e);

				// insert into shuffle buffer accordingly
				int index = get_global_buffer_index(update_info);
				global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, num_partitions, index);
				global_buf->insert(update_info);

			}
		}

		// each writer thread generates a scatter_consumer
		void scatter_consumer(global_buffer<T> ** buffers_for_shuffle) {
			while(true) {
				for(int i = 0; i < num_partitions; i++) {
					int fd = open((filename + "." + std::to_string(i) + ".update_stream").c_str(), O_WRONLY);
					global_buffer<T>* g_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, num_partitions, i);
					g_buf->flush(fd);
				}
			}
		}

		void gather_producer() {

		}

		void join_producer() {

		}

		void join_consumer() {

		}

		int get_global_buffer_index(T* update_info) {
			return 0;
//			return update_info->target;
		}

	};
}



#endif /* CORE_ENGINE_HPP_ */

