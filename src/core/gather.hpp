/*
 * gather.hpp
 *
 *  Created on: Mar 16, 2017
 *      Author: kai
 */

#ifndef CORE_GATHER_HPP_
#define CORE_GATHER_HPP_

//#include "io_manager.hpp"
//#include "buffer_manager.hpp"
//#include "concurrent_queue.hpp"
//#include "type.hpp"
//#include "constants.hpp"
#include "scatter.hpp"


namespace RStream {
	template <typename VertexDataType, typename UpdateType>
	class Gather {
		static_assert(
			std::is_base_of<BaseVertex, VertexDataType>::value,
			"VertexDataType must be a subclass of BaseVertex."
		);

		static_assert(
			std::is_base_of<BaseUpdate, UpdateType>::value,
			"UpdateType must be a subclass of BaseUpdate."
		);

		const Engine & context;

	public:

		Gather(Engine& e) : context(e) {};

		void gather(std::function<void(UpdateType&, VertexDataType*)> apply_one_update) {
			// a pair of <vertex, update_stream> for each partition
			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// threads will load vertex and update, and apply update one by one
			std::vector<std::thread> threads;
			for(int i = 0; i < context.num_threads; i++)
				threads.push_back(std::thread(&Gather::gather_producer, this, apply_one_update, task_queue));

			// join all threads
			for(auto & t : threads)
				t.join();

			delete task_queue;
		}

	private:
		void load_vertices_hashMap(char* vertex_local_buf, const int vertex_file_size, std::unordered_map<VertexId, VertexDataType*> & vertex_map){
			for(size_t off = 0; off < vertex_file_size; off += context.vertex_unit){
				VertexDataType* v = reinterpret_cast<VertexDataType*>(vertex_local_buf + off);
				vertex_map[v->id] = v;
			}
		}

		void gather_producer(std::function<void(UpdateType&, VertexDataType*)> apply_one_update,
						concurrent_queue<int> * task_queue) {
			int partition_id = -1;
			while(task_queue->test_pop_atomic(partition_id)) {
				int fd_vertex = open((context.filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDWR);
				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream").c_str(), O_RDONLY);
//				int fd_vertex = fd_pair.first;
//				int fd_update = fd_pair.second;
				assert(fd_vertex > 0 && fd_update > 0 );

				// get file size
				long vertex_file_size = io_manager::get_filesize(fd_vertex);
				long update_file_size = io_manager::get_filesize(fd_update);

				// vertex data fully loaded into memory
				char * vertex_local_buf = new char[vertex_file_size];
				io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size, 0);
				std::unordered_map<VertexId, VertexDataType*> vertex_map;
				load_vertices_hashMap(vertex_local_buf, vertex_file_size, vertex_map);

//				char * update_local_buf = new char[update_file_size];
//				io_manager::read_from_file(fd_update, update_local_buf, update_file_size);

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				int streaming_counter = update_file_size / IO_SIZE + 1;

				// for each update
				// size_t is unsigned int, too small for file size?
//				for(size_t pos = 0; pos < update_file_size; pos += sizeof(UpdateType)) {
//					// get an update
//					UpdateType & update = *(UpdateType*)(update_local_buf + pos);
//					assert(vertex_map.find(update.target) != vertex_map.end());
//					VertexDataType * dst_vertex = vertex_map.find(update.target)->second;
//					apply_one_update(update, dst_vertex);
//				}

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {

					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - IO_SIZE * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE;

					assert(valid_io_size % sizeof(UpdateType) == 0);

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					for(long pos = 0; pos < valid_io_size; pos += sizeof(UpdateType)) {
						// get an update
						UpdateType & update = *(UpdateType*)(update_local_buf + pos);
						assert(vertex_map.find(update.target) != vertex_map.end());
						VertexDataType * dst_vertex = vertex_map.find(update.target)->second;
						apply_one_update(update, dst_vertex);
					}

				}

				//for debugging
				for(size_t off = 0; off < vertex_file_size; off += context.vertex_unit){
					VertexDataType* v = reinterpret_cast<VertexDataType*>(vertex_local_buf + off);
					std::cout << *v << std::endl;
				}

				// write updated vertex value to disk
//				store_updatedvertices(vertex_map, vertex_local_buf, vertex_file_size);
				io_manager::write_to_file(fd_vertex, vertex_local_buf, vertex_file_size);

				// delete
				delete[] vertex_local_buf;
				delete[] update_local_buf;

//				//clear vertex_map
//				for(auto it = vertex_map.cbegin(); it != vertex_map.cend(); ++it){
//					delete it->second;
//				}

				close(fd_vertex);
				close(fd_update);
			}
		}
	};
}



#endif /* CORE_GATHER_HPP_ */
