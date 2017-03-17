/*
 * gather.hpp
 *
 *  Created on: Mar 16, 2017
 *      Author: kai
 */

#ifndef CORE_GATHER_HPP_
#define CORE_GATHER_HPP_

#include "io_manager.hpp"
#include "buffer_manager.hpp"
#include "concurrent_queue.hpp"
#include "type.hpp"
#include "constants.hpp"


namespace RStream {
	template <typename VertexDataType, typename UpdateType>
	class Gather {
		static_assert(
					std::is_base_of<BaseUpdate, UpdateType>::value,
					"UpdateType must be a subclass of BaseUpdate."
		);

		const Engine & context;

	public:

		Gather(Engine& e) : context(e) {};

		void gather(std::function<void(UpdateType&, char*)> apply_one_update) {
			// a pair of <vertex, update_stream> for each partition
			concurrent_queue<std::pair<int, int>> * task_queue = new concurrent_queue<std::pair<int, int>>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				int fd_vertex = open((context.filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDONLY);
				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream").c_str(), O_RDONLY);
				assert(fd_vertex > 0 && fd_update > 0);
				task_queue->push(std::make_pair(fd_vertex, fd_update));
			}

			// threads will load vertex and update, and apply update one by one
			std::vector<std::thread> threads;
			for(int i = 0; i < context.num_threads; i++)
				threads.push_back(std::thread(&Gather::gather_producer, this, apply_one_update, task_queue));

			// join all threads
			for(auto & t : threads)
				t.join();
		}

	private:
		void gather_producer(std::function<void(UpdateType&, char*)> apply_one_update,
						concurrent_queue<std::pair<int, int>> * task_queue) {

			std::pair<int, int> fd_pair(-1, -1);
			while(task_queue->test_pop_atomic(fd_pair)) {
				int fd_vertex = fd_pair.first;
				int fd_update = fd_pair.second;
				assert(fd_vertex > 0 && fd_update > 0 );

				// get file size
				size_t vertex_file_size = io_manager::get_filesize(fd_vertex);
				size_t update_file_size = io_manager::get_filesize(fd_update);

				// read from files to thread local buffer
				char * vertex_local_buf = new char[vertex_file_size];
				io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size);
				char * update_local_buf = new char[update_file_size];
				io_manager::read_from_file(fd_update, update_local_buf, update_file_size);

				// for each update
				// size_t is unsigend int, too small for file size?
				for(size_t pos = 0; pos < update_file_size; pos += sizeof(UpdateType)) {
					// get an update
					UpdateType & update = *(UpdateType*)(update_local_buf + pos);
					apply_one_update(update, vertex_local_buf);
				}

				// write updated vertex value to disk
				io_manager::write_to_file(fd_vertex, vertex_local_buf, vertex_file_size);

				// delete
				delete[] vertex_local_buf;
				delete[] update_local_buf;

				close(fd_vertex);
				close(fd_update);
			}
		}
	};
}



#endif /* CORE_GATHER_HPP_ */
