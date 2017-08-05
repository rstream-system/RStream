///*
// * gather.cpp
// *
// *  Created on: Aug 4, 2017
// *      Author: kai
// */
//
//
//#include "gather.hpp"
//
//namespace RStream {
//	template <typename VertexDataType, typename UpdateType>
//	Gather<VertexDataType, UpdateType>::Gather(Engine & e) : context(e) {};
//
//	template <typename VertexDataType, typename UpdateType>
//	void Gather<VertexDataType, UpdateType>::gather(Update_Stream in_update_stream, std::function<void(UpdateType*, VertexDataType*)> apply_one_update) {
//		// a pair of <vertex, update_stream> for each partition
//		concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
//
//		// push task into concurrent queue
//		for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
//			task_queue->push(partition_id);
//		}
//
//		// threads will load vertex and update, and apply update one by one
//		std::vector<std::thread> threads;
//		for(int i = 0; i < context.num_threads; i++)
//			threads.push_back(std::thread(&Gather::gather_producer, this, in_update_stream, apply_one_update, task_queue));
//
//		// join all threads
//		for(auto & t : threads)
//			t.join();
//
//		delete task_queue;
//	}
//
//	template <typename VertexDataType, typename UpdateType>
//	void Gather<VertexDataType, UpdateType>::load_vertices_hashMap(char* vertex_local_buf, const int vertex_file_size,
//			std::unordered_map<VertexId, VertexDataType*> & vertex_map){
//
//		for(size_t off = 0; off < vertex_file_size; off += context.vertex_unit){
//			VertexDataType* v = reinterpret_cast<VertexDataType*>(vertex_local_buf + off);
//			vertex_map[v->id] = v;
//		}
//	}
//
//	template <typename VertexDataType, typename UpdateType>
//	void Gather<VertexDataType, UpdateType>::gather_producer(Update_Stream in_update_stream,
//			std::function<void(UpdateType*, VertexDataType*)> apply_one_update,concurrent_queue<int> * task_queue) {
//
//		int partition_id = -1;
//		int vertex_start = -1;
//		assert(context.vertex_unit == sizeof(VertexDataType));
//
//		while(task_queue->test_pop_atomic(partition_id)) {
//			int fd_vertex = open((context.filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDWR);
//			int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
//			assert(fd_vertex > 0 && fd_update > 0 );
//
//			// get start vertex id
//			vertex_start = context.vertex_intervals[partition_id].first;
//			assert(vertex_start >= 0 && vertex_start < context.num_vertices);
//
//			// get file size
//			long vertex_file_size = io_manager::get_filesize(fd_vertex);
//			long update_file_size = io_manager::get_filesize(fd_update);
//
//			// vertex data fully loaded into memory
//			char * vertex_local_buf = new char[vertex_file_size];
//			io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size, 0);
//			std::unordered_map<VertexId, VertexDataType*> vertex_map;
//			load_vertices_hashMap(vertex_local_buf, vertex_file_size, vertex_map);
//
//			// streaming updates
//			char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE * sizeof(UpdateType));
//			int streaming_counter = update_file_size / (IO_SIZE * sizeof(UpdateType)) + 1;
//
//			long valid_io_size = 0;
//			long offset = 0;
//
//			// for all streaming
//			for(int counter = 0; counter < streaming_counter; counter++) {
//
//				// last streaming
//				if(counter == streaming_counter - 1)
//					// TODO: potential overflow?
////						valid_io_size = update_file_size - IO_SIZE * (streaming_counter - 1);
//					valid_io_size = update_file_size - IO_SIZE * sizeof(UpdateType) * (streaming_counter - 1);
//				else
////						valid_io_size = IO_SIZE;
//					valid_io_size = IO_SIZE * sizeof(UpdateType);
//
//				assert(valid_io_size % sizeof(UpdateType) == 0);
//
//				io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
//				offset += valid_io_size;
//
//				for(long pos = 0; pos < valid_io_size; pos += sizeof(UpdateType)) {
//					// get an update
//					UpdateType * update = (UpdateType*)(update_local_buf + pos);
//
//					// get target vertex in vertex buf
//					size_t offset = (update->target - vertex_start) * sizeof(VertexDataType);
//					VertexDataType* dst_vertex = reinterpret_cast<VertexDataType*>(vertex_local_buf + offset);
//
////						assert(vertex_map.find(update->target) != vertex_map.end());
////						VertexDataType * dst_vertex = vertex_map.find(update->target)->second;
//					apply_one_update(update, dst_vertex);
//				}
//
//			}
//
//			//for debugging
////				for(size_t off = 0; off < vertex_file_size; off += context.vertex_unit){
////					VertexDataType* v = reinterpret_cast<VertexDataType*>(vertex_local_buf + off);
////					std::cout << *v << std::endl;
////				}
//
//			// write updated vertex value to disk
////				store_updatedvertices(vertex_map, vertex_local_buf, vertex_file_size);
//			io_manager::write_to_file(fd_vertex, vertex_local_buf, vertex_file_size);
//
//			// delete
//			delete[] vertex_local_buf;
//			delete[] update_local_buf;
//
////				//clear vertex_map
////				for(auto it = vertex_map.cbegin(); it != vertex_map.cend(); ++it){
////					delete it->second;
////				}
//
//			close(fd_vertex);
//			close(fd_update);
//		}
//	}
//}
//
