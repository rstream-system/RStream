/*
 * engine.hpp
 *
 *  Created on: Mar 3, 2017
 *      Author: kai
 */

#ifndef CORE_ENGINE_HPP_
#define CORE_ENGINE_HPP_

#include "io_manager.hpp"
#include "buffer_manager.hpp"
#include "concurrent_queue.hpp"
#include "type.hpp"
#include "constants.hpp"

namespace RStream {
	enum class EdgeType {
		NO_WEIGHT = 0,
		WITH_WEIGHT = 1,
	};

	std::ostream& operator<<(std::ostream& o, EdgeType c)
	{
	    if(c == EdgeType::NO_WEIGHT){
	    	o << "NO_WEIGHT";
	    }
	    else if(c == EdgeType::WITH_WEIGHT){
	    	o << "WITH_WEIGHT";
	    }
	    else{
	    	std::cout << "wrong edge type!!!" << std::endl;
	    	throw std::exception();
	    }
	    return o;
	}

//	template <typename VertexDataType, typename UpdateType>

	class Engine {
	public:
		int num_threads;
		int num_write_threads;
		int num_exec_threads;

		std::string filename;
		int num_partitions;
//		std::vector<int> num_vertices;

		EdgeType edge_type;
		// sizeof each edge
		int edge_unit;

		int vertex_unit;

		int* vertex_intervals;


		Engine(std::string _filename) : filename(_filename) {
//			num_threads = std::thread::hardware_concurrency();
			num_threads = 4;
			num_write_threads = num_threads > 2 ? 2 : 1;
			num_exec_threads = num_threads > 2 ? num_threads - 2 : 1;

			// read meta file, contains num of partitions, etc.
			FILE *meta_file = fopen((filename + ".meta").c_str(), "r");
			if(!meta_file) {
				std::cout << "meta file does not exit!" << std::endl;
				assert(false);
			}

			int edge_t;
			fscanf(meta_file, "%d %d", &num_partitions, &edge_t);
			edge_type = static_cast<EdgeType>(edge_t);
			fclose(meta_file);

			// size of each edge
			if(edge_type == EdgeType::NO_WEIGHT) {
				edge_unit = sizeof(VertexId) * 2;
			}
			else if(edge_type == EdgeType::WITH_WEIGHT) {
				edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
			}

			//
			preprocess();
			vertex_unit = 12;

			std::cout << "Number of partitions: " << num_partitions << std::endl;
			std::cout << "Edge type: " << edge_type << std::endl;
			std::cout << "Number of bytes per edge: " << edge_unit << std::endl;
			std::cout << "Number of exec threads: " << num_exec_threads << std::endl;
			std::cout << "Number of write threads: " << num_write_threads << std::endl;
			std::cout << std::endl;
		}

		~Engine(){
			delete[] vertex_intervals;
		}

		void preprocess(){
			//TODO

			vertex_intervals = new int[num_partitions];
			for(int i = 0; i < num_partitions; ++i){
				vertex_intervals[i] = (i + 1) * 2;
			}

		}

		/* init vertex data*/
//		void init_vertex(std::function<void(char*)> init) {
//			// a pair of <vertex_file, num_vertices>
//			concurrent_queue<std::pair<int, int>> * task_queue = new concurrent_queue<std::pair<int, int>>(num_partitions);
//
//			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
//				int perms = O_WRONLY;
//				std::string vertex_file = filename + "." + std::to_string(partition_id) + ".vertex";
//				int fd = open(vertex_file.c_str(), perms, S_IRWXU);
//				if(fd < 0) {
//					fd = creat(vertex_file.c_str(), S_IRWXU);
//				}
//				task_queue->push(std::make_pair(fd, num_vertices[partition_id]));
//
//			}
//
//			// threads will load vertex and update, and apply update one by one
//			std::vector<std::thread> threads;
//			for(int i = 0; i < num_threads; i++)
//				threads.push_back(std::thread(&engine::init_produer, this, init, task_queue));
//
//			// join all threads
//			for(auto & t : threads)
//				t.join();
//		}

		/* scatter with vertex data (for graph computation use)*/
//		void scatter_with_vertex(std::function<T*(Edge&, char*)> generate_one_update) {
//			// a pair of <vertex, edge_stream> for each partition
//			concurrent_queue<std::pair<int, int>> * task_queue = new concurrent_queue<std::pair<int, int>>(num_partitions);
//
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
//				int fd_vertex = open((filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDONLY);
//				int fd_edge = open((filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
//				assert(fd_vertex > 0 && fd_edge > 0);
//				task_queue->push(std::make_pair(fd_vertex, fd_edge));
//			}
//
//			// allocate global buffers for shuffling
//			global_buffer<T> ** buffers_for_shuffle = buffer_manager<T>::get_global_buffers(num_partitions);
//
//			// exec threads will produce updates and push into shuffle buffers
//			std::vector<std::thread> exec_threads;
//			for(int i = 0; i < num_exec_threads; i++)
//				exec_threads.push_back( std::thread([=] { this->scatter_producer_with_vertex(generate_one_update, buffers_for_shuffle, task_queue); } ));
//
//			// write threads will flush shuffle buffer to update out stream file as long as it's full
//			std::vector<std::thread> write_threads;
//			for(int i = 0; i < num_write_threads; i++)
//				write_threads.push_back(std::thread(&engine::scatter_consumer, this, buffers_for_shuffle));
//
//			// join all threads
//			for(auto & t : exec_threads)
//				t.join();
//
//			for(auto &t : write_threads)
//				t.join();
//
//			delete[] buffers_for_shuffle;
//		}
//
//		/* scatter without vertex data (for relational algebra use)*/
//		void scatter_no_vertex(std::function<T*(Edge&)> generate_one_update) {
//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(num_partitions);
//
//			// allocate global buffers for shuffling
//			global_buffer<T> ** buffers_for_shuffle = buffer_manager<T>::get_global_buffers(num_partitions);
//
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
//				int fd = open((filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
//				assert(fd > 0);
//				task_queue->push(fd);
//			}
//
////			//for debugging only
////			scatter_producer(generate_one_update, buffers_for_shuffle, task_queue);
////			std::cout << "scatter done!" << std::endl;
////			scatter_consumer(buffers_for_shuffle);
//
//			// exec threads will produce updates and push into shuffle buffers
//			std::vector<std::thread> exec_threads;
//			for(int i = 0; i < num_exec_threads; i++)
//				exec_threads.push_back(std::thread([=] { this->scatter_producer_no_vertex(generate_one_update, buffers_for_shuffle, task_queue); }));
//
//
//			// write threads will flush shuffle buffer to update out stream file as long as it's full
//			std::vector<std::thread> write_threads;
//			for(int i = 0; i < num_write_threads; i++)
//				write_threads.push_back(std::thread(&engine::scatter_consumer, this, buffers_for_shuffle));
//
//			// join all threads
//			for(auto & t : exec_threads)
//				t.join();
//
//			for(auto &t : write_threads)
//				t.join();
//
//			delete[] buffers_for_shuffle;
//		}
//
//		void gather(std::function<void(T&, char*)> apply_one_update) {
//			// a pair of <vertex, update_stream> for each partition
//			concurrent_queue<std::pair<int, int>> * task_queue = new concurrent_queue<std::pair<int, int>>(num_partitions);
//
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
//				int fd_vertex = open((filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDONLY);
//				int fd_update = open((filename + "." + std::to_string(partition_id) + ".update_stream").c_str(), O_RDONLY);
//				assert(fd_vertex > 0 && fd_update > 0);
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
//
//		void join() {
//
//		}

	protected:

//		void init_produer(std::function<void(char*)> init, concurrent_queue<std::pair<int, int>> * task_queue) {
//			std::pair<int, int> pair(-1, -1);
//			while(task_queue->test_pop_atomic(pair)) {
//				int fd = pair.first;
//				int num_vertex = pair.second;
//				assert(fd > 0 && num_vertex > 0 );
//
//				// size_t ok??
//				size_t vertex_file_size = num_vertex * sizeof(VertexDataType);
//				char * vertex_local_buf = new char[vertex_file_size];
//
//				// for each vertex
//				for(size_t pos = 0; pos < vertex_file_size; pos += sizeof(int)) {
//					init(vertex_local_buf + pos);
//				}
//
//				io_manager::write_to_file(fd, vertex_local_buf, vertex_file_size);
//
//				delete[] vertex_local_buf;
//				close(fd);
//			}
//		}
//
//		/* scatter producer with vertex data*/
//		//each exec thread generates a scatter_producer
//		void scatter_producer_with_vertex(std::function<T*(Edge&, char*)> generate_one_update,
//				global_buffer<T> ** buffers_for_shuffle, concurrent_queue<std::pair<int, int>> * task_queue) {
//			atomic_num_producers++;
//			std::pair<int, int> fd_pair(-1, -1);
//			// pop from queue
//			while(task_queue->test_pop_atomic(fd_pair)){
//				int fd_vertex = fd_pair.first;
//				int fd_edge = fd_pair.second;
//				assert(fd_vertex > 0 && fd_edge > 0 );
//
//				// get file size
//				size_t vertex_file_size = io_manager::get_filesize(fd_vertex);
//				size_t edge_file_size = io_manager::get_filesize(fd_edge);
//
//				// read from files to thread local buffer
//				char * vertex_local_buf = new char[vertex_file_size];
//				io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size);
//				char * edge_local_buf = new char[edge_file_size];
//				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size);
//
//				// for each edge
//				for(size_t pos = 0; pos < edge_file_size; pos += edge_unit) {
//					// get an edge
//					Edge e = *(Edge*)(edge_local_buf + pos);
////					std::cout << e << std::endl;
//
//					// gen one update
//					T * update_info = generate_one_update(e, vertex_local_buf);
////					std::cout << update_info->target << std::endl;
//
//
//					// insert into shuffle buffer accordingly
//					int index = get_global_buffer_index(update_info);
//					global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, num_partitions, index);
//					global_buf->insert(update_info, index);
//				}
//
////				std::cout << std::endl;
//
//				// delete
//				delete[] vertex_local_buf;
//				delete[] edge_local_buf;
//				close(fd_vertex);
//				close(fd_edge);
//
//			}
//			atomic_num_producers--;
//
//		}
//
//		/* scatter producer without vertex data*/
//		// each exec thread generates a scatter_producer
//		void scatter_producer_no_vertex(std::function<T*(Edge&)> generate_one_update,
//				global_buffer<T> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
//			atomic_num_producers++;
//			int fd = -1;
//			// pop from queue
//			while(task_queue->test_pop_atomic(fd)){
//				assert(fd > 0);
//
//				// get file size
//				size_t file_size = io_manager::get_filesize(fd);
//				print_thread_info_locked("as a producer dealing with " + std::to_string(fd) + " of size " + std::to_string(file_size) + "\n");
//
//				// read from file to thread local buffer
//				char * local_buf = new char[file_size];
//				io_manager::read_from_file(fd, local_buf, file_size);
//
//				// for each edge
//				for(size_t pos = 0; pos < file_size; pos += edge_unit) {
//					// get an edge
//					Edge e = *(Edge*)(local_buf + pos);
////					std::cout << e << std::endl;
//
//					// gen one update
//					T * update_info = generate_one_update(e);
////					std::cout << update_info->target << std::endl;
//
//
//					// insert into shuffle buffer accordingly
//					int index = get_global_buffer_index(update_info);
//					global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, num_partitions, index);
//					global_buf->insert(update_info, index);
//				}
//
////				std::cout << std::endl;
//				delete[] local_buf;
//				close(fd);
//
//			}
//			atomic_num_producers--;
//
//		}
//
//		// each writer thread generates a scatter_consumer
//		void scatter_consumer(global_buffer<T> ** buffers_for_shuffle) {
//			while(atomic_num_producers != 0) {
//				int i = (atomic_partition_id++) % num_partitions ;
//
////				//debugging info
////				print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");
//
//				const char * file_name = (filename + "." + std::to_string(i) + ".update_stream").c_str();
//				global_buffer<T>* g_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, num_partitions, i);
//				g_buf->flush(file_name, i);
//			}
//
//			//the last run - deal with all remaining content in buffers
//			while(true){
//				int i = atomic_partition_number--;
////				std::cout << i << std::endl;
//				if(i >= 0){
////					//debugging info
////					print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");
//
//					const char * file_name = (filename + "." + std::to_string(i) + ".update_stream").c_str();
//					global_buffer<T>* g_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, num_partitions, i);
//					g_buf->flush_end(file_name, i);
//
//					delete g_buf;
//				}
//				else{
//					break;
//				}
//			}
//		}
//
//		void gather_producer(std::function<void(T&, char*)> apply_one_update,
//				concurrent_queue<std::pair<int, int>> * task_queue) {
//
//			std::pair<int, int> fd_pair(-1, -1);
//			while(task_queue->test_pop_atomic(fd_pair)) {
//				int fd_vertex = fd_pair.first;
//				int fd_update = fd_pair.second;
//				assert(fd_vertex > 0 && fd_update > 0 );
//
//				// get file size
//				size_t vertex_file_size = io_manager::get_filesize(fd_vertex);
//				size_t update_file_size = io_manager::get_filesize(fd_update);
//
//				// read from files to thread local buffer
//				char * vertex_local_buf = new char[vertex_file_size];
//				io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size);
//				char * update_local_buf = new char[update_file_size];
//				io_manager::read_from_file(fd_update, update_local_buf, update_file_size);
//
//				// for each update
//				// size_t is unsigend int, too small for file size?
//				for(size_t pos = 0; pos < update_file_size; pos += sizeof(T)) {
//					// get an update
//					T & update = *(T*)(update_local_buf + pos);
//					apply_one_update(update, vertex_local_buf);
//				}
//
//				// write updated vertex value to disk
//				io_manager::write_to_file(fd_vertex, vertex_local_buf, vertex_file_size);
//
//				// delete
//				delete[] vertex_local_buf;
//				delete[] update_local_buf;
//
//				close(fd_vertex);
//				close(fd_update);
//			}
//		}
//
//		void join_producer() {
//
//		}
//
//		void join_consumer() {
//
//		}
//
//		int get_global_buffer_index(T* update_info) {
////			return update_info->target;
//			return 0;
//		}

	};
}



#endif /* CORE_ENGINE_HPP_ */

