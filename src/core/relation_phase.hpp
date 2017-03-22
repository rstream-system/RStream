/*
 * relation_phase.hpp
 *
 *  Created on: Mar 16, 2017
 *      Author: kai
 */

#ifndef CORE_RELATION_PHASE_HPP_
#define CORE_RELATION_PHASE_HPP_

#include "scatter.hpp"

namespace RStream {
	template<typename UpdateType>
	class RPhase {
		static_assert(
			std::is_base_of<BaseUpdate, UpdateType>::value,
			"UpdateType must be a subclass of BaseUpdate."
		);

		const Engine & context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

	public:
		virtual bool filter();
		virtual int * project_columns();
		virtual int new_key();

		RPhase(Engine & e) : context(e) {
			atomic_num_producers = 0;
			atomic_partition_id = 0;
			atomic_partition_number = context.num_partitions;
		};

		void join(Update_Stream update_stream) {
			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}
		}

	private:
		void join_producer(Update_Stream update_stream, concurrent_queue<int> * task_queue) {
			atomic_num_producers++;
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				int fd_update = open((context.filename + "." + std::to_string(partition_id) + "." + update_stream.update_filename).c_str(), O_RDONLY);
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_update > 0 && fd_edge > 0 );

				// get file size
				size_t update_file_size = io_manager::get_filesize(fd_update);
				size_t edge_file_size = io_manager::get_filesize(fd_edge);

				// read from files to thread local buffer
				char * update_local_buf = new char[update_file_size];
				io_manager::read_from_file(fd_update, update_local_buf, update_file_size);

				// edges are fully loaded into memory
				char * edge_local_buf = new char[edge_file_size];
				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size);

				// build edge hashmap
				int num_vertices = context.vertex_intervals[partition_id].end - context.vertex_intervals[partition_id].start + 1;
				int start_vertex = context.vertex_intervals[partition_id].start;
				assert(num_vertices > 0 && start_vertex >= 0);

				std::array<std::vector<VertexId>, num_vertices> edge_hashmap;
				build_edge_hashmap(edge_local_buf, edge_hashmap, edge_file_size, start_vertex);

				// straming updates in, do hash join
				for(size_t pos = 0; pos < update_file_size; pos += sizeof(UpdateType)) {
					// get an update
					UpdateType & update = *(UpdateType*)(update_local_buf + pos);
				}
			}
		}

		template<std::size_t SIZE>
		void build_edge_hashmap(char * edge_buf, std::array<std::vector<VertexId>, SIZE> & edge_hashmap, size_t edge_file_size, int start_vertex) {
			// for each edge
			for(size_t pos = 0; pos < edge_file_size; pos += context.edge_unit) {
				// get an edge
				Edge e = *(Edge*)(edge_buf + pos);
				assert(e.src >= start_vertex);
				edge_hashmap[e.src - start_vertex].push_back(e.target);
			}
		}

	};
}



#endif /* CORE_RELATION_PHASE_HPP_ */
