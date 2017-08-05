/*
 * scatter.cpp
 *
 *  Created on: Aug 4, 2017
 *      Author: kai
 */

#include "scatter.hpp"

namespace RStream {
	template <typename VertexDataType, typename UpdateType>
	Scatter<VertexDataType, UpdateType>::Scatter(Engine & e) : context(e) {};

	template <typename VertexDataType, typename UpdateType>
	void Scatter<VertexDataType, UpdateType>::atomic_init() {
		atomic_num_producers = context.num_exec_threads;
		atomic_partition_number = context.num_partitions;
	}

	template <typename VertexDataType, typename UpdateType>
	Update_Stream Scatter<VertexDataType, UpdateType>::scatter_with_vertex(std::function<UpdateType*(Edge*, VertexDataType*)> generate_one_update) {
		atomic_init();

		Logger::print_thread_info_locked("--------------------Start Scatter Phase--------------------\n\n");

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

		Logger::print_thread_info_locked("--------------------Finish Scatter Phase-------------------\n\n");

		return update_c;
	}

	template <typename VertexDataType, typename UpdateType>
	Update_Stream Scatter<VertexDataType, UpdateType>::scatter_no_vertex(std::function<UpdateType*(Edge*)> generate_one_update) {
		atomic_init();

		Logger::print_thread_info_locked("--------------------Start Scatter Phase--------------------\n\n");

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

		Logger::print_thread_info_locked("--------------------Finish Scatter Phase-------------------\n\n");

		return update_c;
	}

	template <typename VertexDataType, typename UpdateType>
	void Scatter<VertexDataType, UpdateType>::load_vertices_hashMap(char* vertex_local_buf, const int vertex_file_size,
			std::unordered_map<VertexId, VertexDataType*> & vertex_map) {
		for(size_t off = 0; off < vertex_file_size; off += context.vertex_unit){
			VertexDataType* v = reinterpret_cast<VertexDataType*>(vertex_local_buf + off);
			vertex_map[v->id] = v;
		}
	}

	template <typename VertexDataType, typename UpdateType>
	void Scatter<VertexDataType, UpdateType>::scatter_producer_with_vertex(std::function<UpdateType*(Edge*, VertexDataType*)> generate_one_update,
			global_buffer<UpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {

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

			Logger::print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + " of size " + std::to_string(edge_file_size) + "\n");

			// vertex data fully loaded into memory
			char * vertex_local_buf = new char[vertex_file_size];
			io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size, 0);
			std::unordered_map<VertexId, VertexDataType*> vertex_map;
			load_vertices_hashMap(vertex_local_buf, vertex_file_size, vertex_map);

			// streaming edges
			char * edge_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE * sizeof(Edge));
			int streaming_counter = edge_file_size / (IO_SIZE * sizeof(Edge)) + 1;

			long valid_io_size = 0;
			long offset = 0;
			int edge_unit = context.edge_unit;

			assert(edge_unit == sizeof(Edge));

			// for all streaming
			for(int counter = 0; counter < streaming_counter; counter++) {

				// last streaming
				if(counter == streaming_counter - 1)
					// TODO: potential overflow?
//						valid_io_size = edge_file_size - IO_SIZE * (streaming_counter - 1);
					valid_io_size = edge_file_size - IO_SIZE * sizeof(Edge) * (streaming_counter - 1);
				else
//						valid_io_size = IO_SIZE;
					valid_io_size = IO_SIZE * sizeof(Edge);

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

					UpdateType * update_info = generate_one_update(e, src_vertex);
//					std::cout << update_info->target << std::endl;

					// insert into shuffle buffer accordingly
					int index = get_global_buffer_index(update_info);
					global_buffer<UpdateType>* global_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
					global_buf->insert(update_info, index);
				}
			}

			// delete
			delete[] vertex_local_buf;
			free(edge_local_buf);

//				//clear vertex_map
//				for(auto it = vertex_map.cbegin(); it != vertex_map.cend(); ++it){
//					delete it->second;
//				}

			close(fd_vertex);
			close(fd_edge);

		}
		atomic_num_producers--;
	}

	template <typename VertexDataType, typename UpdateType>
	void Scatter<VertexDataType, UpdateType>::scatter_producer_no_vertex(std::function<UpdateType*(Edge*)> generate_one_update,
			global_buffer<UpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {

		int partition_id = -1;

		for(unsigned int i = 0; i < context.num_partitions; i++) {
			assert(buffers_for_shuffle[i]->get_capacity() == BUFFER_CAPACITY);
		}

		// pop from queue
		while(task_queue->test_pop_atomic(partition_id)){
			int fd = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
			assert(fd > 0);

			// get file size
			long file_size = io_manager::get_filesize(fd);
			Logger::print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + " of size " + std::to_string(file_size) + "\n");

			// streaming edges
			char * local_buf = (char*)memalign(PAGE_SIZE, IO_SIZE * sizeof(Edge));
			int streaming_counter = file_size / (IO_SIZE * sizeof(Edge)) + 1;

			assert((file_size % sizeof(Edge)) == 0);

			long valid_io_size = 0;
			long offset = 0;
			int edge_unit = context.edge_unit;

			// for all streaming
			for(int counter = 0; counter < streaming_counter; counter++) {

				// last streaming
				if(counter == streaming_counter - 1)
					// TODO: potential overflow?
//						valid_io_size = file_size - IO_SIZE * (streaming_counter - 1);
					valid_io_size = file_size - IO_SIZE * sizeof(Edge) * (streaming_counter - 1);
				else
//						valid_io_size = IO_SIZE;
					valid_io_size = IO_SIZE * sizeof(Edge);

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

					int index = meta_info::get_index(update_info->target, context);
					assert(index >= 0);

					global_buffer<UpdateType>* global_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
					global_buf->insert(update_info, index);
				}
			}

			free(local_buf);
			close(fd);

		}
		atomic_num_producers--;

	}

	template <typename VertexDataType, typename UpdateType>
	void Scatter<VertexDataType, UpdateType>::scatter_consumer(global_buffer<UpdateType> ** buffers_for_shuffle, Update_Stream update_count) {
		int counter = 0;

		while(atomic_num_producers != 0) {
			if(counter == context.num_partitions)
				counter = 0;

			int i = counter++;

			const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count)).c_str();
			std::string file_name_str = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count));

			global_buffer<UpdateType>* g_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
			g_buf->flush(file_name_str, i);
		}

		//the last run - deal with all remaining content in buffers
		while(true){
			int i = --atomic_partition_number;
			if(i >= 0){

				std::string file_name_str = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count));

				global_buffer<UpdateType>* g_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush_end(file_name_str, i);

				delete g_buf;
			}
			else{
				break;
			}
		}
	}
}


