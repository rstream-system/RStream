/*
 * scatter.hpp
 *
 *  Created on: Mar 16, 2017
 *      Author: kai
 */

#ifndef CORE_SCATTER_HPP_
#define CORE_SCATTER_HPP_


#include "io_manager.hpp"
#include "buffer_manager.hpp"
#include "concurrent_queue.hpp"
#include "type.hpp"
#include "constants.hpp"

namespace RStream {
	template <typename VertexDataType, typename UpdateType>

	class Scatter {
		static_assert(
			std::is_base_of<BaseVertex, VertexDataType>::value,
			"VertexDataType must be a subclass of BaseVertex."
		);

		static_assert(
			std::is_base_of<BaseUpdate, UpdateType>::value,
			"UpdateType must be a subclass of BaseUpdate."
		);

		const Engine& context;

		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

	public:

		Scatter(Engine & e) : context(e) {
			atomic_num_producers = 0;
			atomic_partition_id = 0;
			atomic_partition_number = context.num_partitions;
		};

		/* scatter with vertex data (for graph computation use)*/
		Update_Stream scatter_with_vertex(std::function<UpdateType*(Edge*, VertexDataType*)> generate_one_update) {
			Update_Stream update_c = Engine::update_count++;

			// a pair of <vertex, edge_stream> for each partition
			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// allocate global buffers for shuffling
			global_buffer<UpdateType> ** buffers_for_shuffle = buffer_manager<UpdateType>::get_global_buffers(context.num_partitions);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->scatter_producer_with_vertex(generate_one_update, buffers_for_shuffle, task_queue); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&Scatter::scatter_consumer, this, buffers_for_shuffle, update_c));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			return update_c;
		}

		/* scatter without vertex data (for relational algebra use)*/
		Update_Stream scatter_no_vertex(std::function<UpdateType*(Edge*)> generate_one_update) {
			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// allocate global buffers for shuffling
			global_buffer<UpdateType> ** buffers_for_shuffle = buffer_manager<UpdateType>::get_global_buffers(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

//			//for debugging only
//			scatter_producer(generate_one_update, buffers_for_shuffle, task_queue);
//			std::cout << "scatter done!" << std::endl;
//			scatter_consumer(buffers_for_shuffle);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back(std::thread([=] { this->scatter_producer_no_vertex(generate_one_update, buffers_for_shuffle, task_queue); }));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&Scatter::scatter_consumer, this, buffers_for_shuffle, update_c));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			return update_c;
		}


	private:


		void load_vertices_hashMap(char* vertex_local_buf, const int vertex_file_size, std::unordered_map<VertexId, VertexDataType*> & vertex_map){
			for(size_t off = 0; off < vertex_file_size; off += context.vertex_unit){
				VertexDataType* v = reinterpret_cast<VertexDataType*>(vertex_local_buf + off);
				vertex_map[v->id] = v;
			}
		}

		/* scatter producer with vertex data*/
		//each exec thread generates a scatter_producer
		void scatter_producer_with_vertex(std::function<UpdateType*(Edge*, VertexDataType*)> generate_one_update,
				global_buffer<UpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {

			atomic_num_producers++;
			int partition_id = -1;
			VertexId vertex_start = -1;
			assert(context.vertex_unit == sizeof(VertexDataType));

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				int fd_vertex = open((context.filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDONLY);
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_vertex > 0 && fd_edge > 0 );

				// get start vertex id
				vertex_start = context.vertex_intervals[partition_id].first;
				assert(vertex_start >= 0 && vertex_start < context.num_vertices);

				// get file size
				long vertex_file_size = io_manager::get_filesize(fd_vertex);
				long edge_file_size = io_manager::get_filesize(fd_edge);

				print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + " of size " + std::to_string(edge_file_size) + "\n");

				// vertex data fully loaded into memory
				char * vertex_local_buf = new char[vertex_file_size];
				io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size, 0);
				std::unordered_map<VertexId, VertexDataType*> vertex_map;
				load_vertices_hashMap(vertex_local_buf, vertex_file_size, vertex_map);

//				char * edge_local_buf = new char[edge_file_size];
//				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size);

				// streaming edges
				char * edge_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				int streaming_counter = edge_file_size / IO_SIZE + 1;

				long valid_io_size = 0;
				long offset = 0;
				int edge_unit = context.edge_unit;
				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {

					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = edge_file_size - IO_SIZE * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE;

					assert(valid_io_size % edge_unit == 0);

//					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size);
					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// for each streaming
					for(long pos = 0; pos < valid_io_size; pos += edge_unit) {
						// get an edge
						Edge * e = (Edge*)(edge_local_buf + pos);
	//					std::cout << e << std::endl;

						// get src vertex in vertex buf
						size_t offset = (e->src - vertex_start) * sizeof(VertexDataType);
						VertexDataType* src_vertex = reinterpret_cast<VertexDataType*>(vertex_local_buf + offset);

						// gen one update
//						assert(vertex_map.find(e->src) != vertex_map.end());
//						VertexDataType * src_vertex = vertex_map.find(e->src)->second;
						UpdateType * update_info = generate_one_update(e, src_vertex);
	//					std::cout << update_info->target << std::endl;

						// insert into shuffle buffer accordingly
						int index = get_global_buffer_index(update_info);
						global_buffer<UpdateType>* global_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
						global_buf->insert(update_info, index);
					}
				}

				// for each edge
//				for(size_t pos = 0; pos < edge_file_size; pos += context.edge_unit) {
//					// get an edge
//					Edge e = *(Edge*)(edge_local_buf + pos);
////					std::cout << e << std::endl;
//
//					// gen one update
//					assert(vertex_map.find(e.src) != vertex_map.end());
//					VertexDataType * src_vertex = vertex_map.find(e.src)->second;
//					UpdateType * update_info = generate_one_update(e, src_vertex);
////					std::cout << update_info->target << std::endl;
//
//					// insert into shuffle buffer accordingly
//					int index = get_global_buffer_index(update_info);
//					global_buffer<UpdateType>* global_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
//					global_buf->insert(update_info, index);
//				}

//				std::cout << std::endl;

				// delete
				delete[] vertex_local_buf;
				delete[] edge_local_buf;

//				//clear vertex_map
//				for(auto it = vertex_map.cbegin(); it != vertex_map.cend(); ++it){
//					delete it->second;
//				}

				close(fd_vertex);
				close(fd_edge);

			}
			atomic_num_producers--;

		}

		/* scatter producer without vertex data*/
		// each exec thread generates a scatter_producer
		void scatter_producer_no_vertex(std::function<UpdateType*(Edge*)> generate_one_update,
				global_buffer<UpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			atomic_num_producers++;
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				int fd = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd > 0);

				// get file size
				long file_size = io_manager::get_filesize(fd);
				print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + " of size " + std::to_string(file_size) + "\n");

				// read from file to thread local buffer
//				char * local_buf = new char[file_size];
//				io_manager::read_from_file(fd, local_buf, file_size);

				// streaming edges
				char * local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
				int streaming_counter = file_size / IO_SIZE + 1;

				long valid_io_size = 0;
				long offset = 0;
				int edge_unit = context.edge_unit;

				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {

					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = file_size - IO_SIZE * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE;

					assert(valid_io_size % edge_unit == 0);

					io_manager::read_from_file(fd, local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// for each streaming
					for(long pos = 0; pos < valid_io_size; pos += edge_unit) {
						// get an edge
						Edge * e = (Edge*)(local_buf + pos);
	//					std::cout << e << std::endl;

						// gen one update
						UpdateType * update_info = generate_one_update(e);
	//					std::cout << update_info->target << std::endl;

						// insert into shuffle buffer accordingly
						int index = get_global_buffer_index(update_info);
						global_buffer<UpdateType>* global_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
						global_buf->insert(update_info, index);
					}
				}

				// for each edge
//				for(size_t pos = 0; pos < file_size; pos += context.edge_unit) {
//					// get an edge
//					Edge e = *(Edge*)(local_buf + pos);
////					std::cout << e << std::endl;
//
//					// gen one update
//					UpdateType * update_info = generate_one_update(e);
////					std::cout << *update_info << std::endl;
//
//
//					// insert into shuffle buffer accordingly
//					int index = get_global_buffer_index(update_info);
//					global_buffer<UpdateType>* global_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
//					global_buf->insert(update_info, index);
//				}

//				std::cout << std::endl;
				delete[] local_buf;
				close(fd);

			}
			atomic_num_producers--;

		}

		// each writer thread generates a scatter_consumer
		void scatter_consumer(global_buffer<UpdateType> ** buffers_for_shuffle, Update_Stream update_count) {
			while(atomic_num_producers != 0) {
				int i = (atomic_partition_id++) % context.num_partitions ;

//				//debugging info
//				print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");

				const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count)).c_str();
				global_buffer<UpdateType>* g_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;
//				std::cout << i << std::endl;
				if(i >= 0){
//					//debugging info
//					print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");

					const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count)).c_str();
					global_buffer<UpdateType>* g_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
					g_buf->flush_end(file_name, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}

		int get_global_buffer_index(UpdateType* update_info) {
//			return update_info->target / context.num_vertices_per_part;

			int partition_id = update_info->target / context.num_vertices_per_part;
			return partition_id < (context.num_partitions - 1) ? partition_id : (context.num_partitions - 1);

//			int target = update_info->target;
//
//			int lb = 0, ub = context.num_partitions;
//			int i = (lb + ub) / 2;
//
//			while(true){
////				int c = context.vertex_intervals[i];
//				if(i == 0){
//					return 0;
//				}
////				int p = context.vertex_intervals[i - 1];
//				if(c >= target && p < target){
//					return i;
//				}
//				else if(c > target){
//					ub = i;
//				}
//				else if(c < target){
//					lb = i;
//				}
//				i = (lb + ub) / 2;
//			}
		}

	};


}



#endif /* CORE_SCATTER_HPP_ */
