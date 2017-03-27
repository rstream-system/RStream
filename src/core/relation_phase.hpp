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
	template<typename InUpdateType, typename OutUpdateType>
	class RPhase {
		static_assert(
			std::is_base_of<BaseUpdate, InUpdateType>::value,
			"OldUpdateType must be a subclass of BaseUpdate."
		);

		static_assert(
			std::is_base_of<BaseUpdate, OutUpdateType>::value,
			"NewUpdateType must be a subclass of BaseUpdate."
		);

		const Engine & context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

	public:
		struct JoinResultType {
			InUpdateType old_update;
			VertexId target;

			JoinResultType() : target(0) {};
			JoinResultType(InUpdateType u, VertexId t) : old_update(u), target(t) {};
		};

		virtual bool filter(InUpdateType & update, VertexId edge_src, VertexId edge_targets);
		virtual void project_columns(char* join_result, OutUpdateType * new_update);
//		virtual int new_key();

		RPhase(Engine & e) : context(e) {
			atomic_num_producers = 0;
			atomic_partition_id = 0;
			atomic_partition_number = context.num_partitions;
		};

		/* join update stream with edge stream
		 * @param in_update_stream -input file for update stream
		 * @param out_update_stream -output file for update stream
		 * */
		void join(Update_Stream in_update_stream, Update_Stream out_update_stream) {
			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// allocate global buffers for shuffling
			global_buffer<OutUpdateType> ** buffers_for_shuffle = buffer_manager<OutUpdateType>::get_global_buffers(context.num_partitions);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_partitions; i++)
				exec_threads.push_back( std::thread([=] { this->join_producer(in_update_stream, buffers_for_shuffle, task_queue); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&RPhase::join_consumer, this, out_update_stream, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;
		}

	private:
		// each exec thread generates a join producer
		void join_producer(Update_Stream in_update_stream, global_buffer<OutUpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			atomic_num_producers++;
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				int fd_update = open((context.filename + "." + std::to_string(partition_id) + "." + in_update_stream.update_filename).c_str(), O_RDONLY);
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_update > 0 && fd_edge > 0 );

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);
				long edge_file_size = io_manager::get_filesize(fd_edge);

				// read from files to thread local buffer
//				char * update_local_buf = new char[update_file_size];
//				io_manager::read_from_file(fd_update, update_local_buf, update_file_size);

				// streaming updates
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				int streaming_counter = update_file_size / IO_SIZE + 1;

				// edges are fully loaded into memory
				char * edge_local_buf = new char[edge_file_size];
				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size, 0);

				// build edge hashmap
				const int num_vertices = context.vertex_intervals[partition_id].end - context.vertex_intervals[partition_id].start + 1;
				int start_vertex = context.vertex_intervals[partition_id].start;
				assert(num_vertices > 0 && start_vertex >= 0);

//				std::array<std::vector<VertexId>, num_vertices> edge_hashmap;
				std::vector<VertexId> edge_hashmap[num_vertices];
				build_edge_hashmap(edge_local_buf, edge_hashmap, edge_file_size, start_vertex);

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {

					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - IO_SIZE * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE;

					assert(valid_io_size % sizeof(InUpdateType) == 0);

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// streaming updates in, do hash join
					for(long pos = 0; pos < valid_io_size; pos += sizeof(InUpdateType)) {
						// get an update
						InUpdateType & update = *(InUpdateType*)(update_local_buf + pos);

						// update.target is edge.src, the key to index edge_hashmap
						for(VertexId target : edge_hashmap[update.target - start_vertex]) {
							if(!filter(update, update.target, target)) {
	//							NewUpdateType * new_update = new NewUpdateType(update, target);

								//TODO: generate join result
								char* join_result = reinterpret_cast<char*>(&update);
								OutUpdateType * out_update = nullptr;
								project_columns(join_result, out_update);

								// insert into shuffle buffer accordingly
								int index = get_global_buffer_index(out_update);
								global_buffer<OutUpdateType>* global_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
								global_buf->insert(out_update, index);

							}
						}

					}

				}

				// streaming updates in, do hash join
//				for(size_t pos = 0; pos < update_file_size; pos += sizeof(InUpdateType)) {
//					// get an update
//					InUpdateType & update = *(InUpdateType*)(update_local_buf + pos);
//
//					// update.target is edge.src, the key to index edge_hashmap
//					for(VertexId target : edge_hashmap[update.target - start_vertex]) {
//						if(!filter(update, update.target, target)) {
////							NewUpdateType * new_update = new NewUpdateType(update, target);
//
//							//TODO: generate join result
//							char* join_result = reinterpret_cast<char*>(&update);
//							OutUpdateType * out_update = nullptr;
//							project_columns(join_result, out_update);
//
//							// insert into shuffle buffer accordingly
//							int index = get_global_buffer_index(out_update);
//							global_buffer<OutUpdateType>* global_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
//							global_buf->insert(out_update, index);
//
//						}
//					}
//
//				}

				delete[] update_local_buf;
				delete[] edge_local_buf;

				close(fd_update);
				close(fd_edge);
			}

			atomic_num_producers--;
		}

		// each writer thread generates a join_consumer
		void join_consumer(Update_Stream out_update_stream, global_buffer<OutUpdateType> ** buffers_for_shuffle) {
			while(atomic_num_producers != 0) {
				int i = (atomic_partition_id++) % context.num_partitions ;

				const char * file_name = (context.filename + "." + std::to_string(i) + "." + out_update_stream.update_filename).c_str();
				global_buffer<OutUpdateType>* g_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;
//				std::cout << i << std::endl;
				if(i >= 0){
//					//debugging info
//					print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");

					const char * file_name = (context.filename + "." + std::to_string(i) + "." + out_update_stream.update_filename).c_str();
					global_buffer<OutUpdateType>* g_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
					g_buf->flush_end(file_name, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}

		void build_edge_hashmap(char * edge_buf, std::vector<VertexId> * edge_hashmap, size_t edge_file_size, int start_vertex) {
			// for each edge
			for(size_t pos = 0; pos < edge_file_size; pos += context.edge_unit) {
				// get an edge
				Edge e = *(Edge*)(edge_buf + pos);
				assert(e.src >= start_vertex);
				// e.src is the key
				edge_hashmap[e.src - start_vertex].push_back(e.target);
			}
		}

		int get_global_buffer_index(OutUpdateType* new_update) {
			int target = new_update->target;

			int lb = 0, ub = context.num_partitions;
			int i = (lb + ub) / 2;

			while(true){
//				int c = context.vertex_intervals[i];
				int c = context.vertex_intervals[i].end - context.vertex_intervals[i].start + 1;
				if(i == 0){
					return 0;
				}
//				int p = context.vertex_intervals[i - 1];
				int p = context.vertex_intervals[i - 1].end - context.vertex_intervals[i - 1].start + 1;
				if(c >= target && p < target){
					return i;
				}
				else if(c > target){
					ub = i;
				}
				else if(c < target){
					lb = i;
				}
				i = (lb + ub) / 2;
			}
		}

	};
}



#endif /* CORE_RELATION_PHASE_HPP_ */
