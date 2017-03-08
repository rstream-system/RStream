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

		// io manager
		io_manager *io_mgr;

		// buffer manager
		buffer_manager<T> *buffer_mgr;
		// buffers for shuffling
		T** global_buffers;

		concurrent_queue<int> task_queue;

	public:

		engine(std::string _filename) : filename(_filename), buffer_mgr(nullptr), global_buffers(nullptr) {
			num_threads = std::thread::hardware_concurrency();
			// to be decided ?
			num_write_threads = 2;
			num_exec_threads = num_threads - 2;

			// read meta file, contains num of partitions, etc.
			FILE *meta_file = fopen((filename + ".meta").c_str(), "r");
			if(!meta_file) {
				std::cout("meta file does not exit!");
				assert(false);
			}

			fscanf(meta_file, "%d %d", &num_partitions, &edge_type);

			// size of each edge
			if(edge_type == EdgeType::NO_WEIGHT) {
				edge_unit = sizeof(VertexId) * 2;
			} else if(edge_type == EdgeType::WITH_WEIGHT) {
				edge_unit == sizeof(VertexId) * 2 + sizeof(Weight);
			}

			io_mgr = new io_manager();

			task_queue(65536);
		}

		void scatter(std::function<T(Edge&)> generate_one_update) {
			buffer_mgr = new buffer_manager<T>(num_partitions);
			// buffers for shuffling
			global_buffers = buffer_mgr->get_global_buffers();

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
				int fd = open((filename + "." + partition_id).c_str(), O_RDONLY);
				task_queue.push(fd);
			}

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < num_exec_threads; i++)
				exec_threads.push_back(std::thread(scatter_producer, generate_one_update));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < num_write_threads; i++)
				write_threads.push_back(std::thread(scatter_consumer));

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

	private:

		// each exec thread generates a scatter_producer
		void scatter_producer(std::function<T(Edge&)> generate_one_update) {

			// pop from queue
			int fd = task_queue.pop();
			// get file size
			size_t file_size = io_mgr->get_filesize(fd);
			// read from file
			char * buf = malloc(file_size);
			io_mgr->read_from_file(fd, buf, file_size);

			// for each edge
			for(long pos = 0; pos <= file_size; pos += edge_unit) {
				// get an edge
				Edge & e = *(Edge*)(buf + pos);
				// gen one update
				T * update_info = generate_one_update(e);

				// insert into shuffle buffer accordingly
				int index = get_buffer_index(update_info);
				buffer<T> *b = buffer_mgr->get_global_buffer(index);
				b->insert(update_info);

			}
		}

		// each writer thread generates a scatter_consumer
		void scatter_consumer() {
			while(true) {
				for(int i = 0; i < num_partitions; i++) {
					int fd = open((filename + "." + i + ".update_stream").c_str(), O_WRONLY);
					buffer<T> * b = buffer_mgr->get_global_buffer(i);
					b->flush(io_mgr, fd);
				}
			}
		}

		void gather_producer() {

		}

		void join_producer() {

		}

		void join_consumer() {

		}

		int get_buffer_index(T* update_info) {
			return update_info->target;
		}

	};
}



#endif /* CORE_ENGINE_HPP_ */

