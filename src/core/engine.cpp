/*
 * engine.cpp
 *
 *  Created on: Aug 4, 2017
 *      Author: icuzzq
 */

#include "engine.hpp"


namespace RStream {

		unsigned Engine::update_count = 0;
		unsigned Engine::aggregation_count = 0;

		unsigned Engine::tuple_total = 0;
		unsigned Engine::tuple_auto = 0;
		unsigned Engine::tuple_long = 0;
		unsigned Engine::tuple_filter = 0;

		Engine::Engine(std::string _filename, int num_parts, int input_format) : filename(_filename) {
//			num_threads = std::thread::hardware_concurrency();
			num_threads = 1;
			num_write_threads = 1;
			num_exec_threads = num_threads;

			num_partitions = num_parts;

//			num_vertices = _num_vertices;
//			num_partitions = num_parts;
//			num_vertices_per_part = num_vertices / num_partitions;
//			Preprocessing proc(_filename, num_parts, num_vertices);

			const std::string meta_file = _filename + ".meta";
			if(!file_exists(meta_file)) {
//				Preproc proc(_filename, num_vertices, num_partitions, false, false);
//				Preprocessing proc(_filename, num_partitions, num_vertices);
				Preprocessing_new proc(filename, num_parts, input_format);
			}

			// get meta data from .meta file
			read_meta_file(meta_file);

//			edge_type = static_cast<EdgeType>(proc.getEdgeType());
//			edge_unit = proc.getEdgeUnit();
			vertex_unit = 0;
//			vertex_unit = 8;
//			num_vertices_per_part = proc.getNumVerPerPartition();

//			std::cout << "Input format: " << (input_format) << std::endl;
			std::cout << "Number of vertices: " << num_vertices << std::endl;
			std::cout << "Number of partitions: " << num_partitions << std::endl;
			std::cout << "Edge type: " << edge_type << std::endl;
			std::cout << "Number of bytes per edge: " << edge_unit << std::endl;
			std::cout << "Number of exec threads: " << num_exec_threads << std::endl;
			std::cout << "Number of write threads: " << num_write_threads << std::endl;
			std::cout << std::endl;

//			for(int i = 0; i < num_partitions; i++)
//				std::cout << "partition " << i << " , start: " << vertex_intervals[i].first << " , end: " << vertex_intervals[i].second << std::endl;
		}

		Engine::~Engine(){
//			delete[] vertex_intervals;
		}

		//clean files added by Zhiqiang
		void Engine::clean_files(){
			//delete .binary
			FileUtil::delete_file(filename + ".binary");

			//delete .meta
			FileUtil::delete_file(filename + ".meta");

			//delete partitions
			for(int i = 0; i < num_partitions; ++i){
				FileUtil::delete_file(filename + "." + std::to_string(i));
			}
		}

//		void preprocess(){
//			//TODO
//
////			vertex_intervals = new int[num_partitions];
////			for(int i = 0; i < num_partitions; ++i){
////				vertex_intervals[i] = (i + 1) * 2;
////			}
//
//			vertex_intervals = new struct Vertex_Interval[num_partitions];
//			for(int i = 0; i < num_partitions; ++i) {
//				vertex_intervals[i] = {0, 1};
//			}
//
//		}

		/* init vertex data*/
//		template <typename VertexDataType>
//		void Engine::init_vertex(std::function<void(char*, VertexId)> init) {
//			vertex_unit = sizeof(VertexDataType);
//
//			// a pair of <vertex_file, num_vertices>
////			concurrent_queue<std::pair<int, int>> * task_queue = new concurrent_queue<std::pair<int, int>>(num_partitions);
//
//			// <vertex_file, num_vertices, start_vertex_id>
//			concurrent_queue<std::tuple<int, VertexId, VertexId>> * task_queue = new concurrent_queue<std::tuple<int, VertexId, VertexId>>(num_partitions);
//
//			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
//				int perms = O_WRONLY;
//				std::string vertex_file = filename + "." + std::to_string(partition_id) + ".vertex";
//				int fd = open(vertex_file.c_str(), perms, S_IRWXU);
//				if(fd < 0) {
//					fd = creat(vertex_file.c_str(), S_IRWXU);
//				}
//
//				VertexId n_vertices = vertex_intervals[partition_id].second - vertex_intervals[partition_id].first + 1;
////				task_queue->push(std::make_pair(fd, n_vertices));
//				task_queue->push(std::make_tuple(fd, n_vertices, vertex_intervals[partition_id].first));
//
//			}
//
//			// threads will load vertex and update, and apply update one by one
//			std::vector<std::thread> threads;
//			for(int i = 0; i < num_threads; i++)
//				threads.push_back(std::thread(&Engine::init_produer<VertexDataType>, this, init, task_queue));
//
//			// join all threads
//			for(auto & t : threads)
//				t.join();
//		}

		/*compute out degree for each vertex*/
//		template <typename VertexDataType>
//		void Engine::compute_degree() {
//
//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(num_partitions);
//
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
//				task_queue->push(partition_id);
//			}
//
//			std::vector<std::thread> threads;
//			for(int i = 0; i < num_threads; i++)
//				threads.push_back(std::thread(&Engine::compute_degree_producer<VertexDataType>, this, task_queue));
//
//			// join all threads
//			for(auto & t : threads)
//				t.join();
//
//			delete task_queue;
//		}



//		template <typename VertexDataType>
////		void Engine::init_produer(std::function<void(char*, VertexId)> init, concurrent_queue<std::pair<int, int>> * task_queue) {
//		void Engine::init_produer(std::function<void(char*, VertexId)> init, concurrent_queue<std::tuple<int, VertexId, VertexId>> * task_queue) {
////			std::pair<int, int> pair(-1, -1);
//			int fd = -1;
//			VertexId num_vertex = 0, start_vertex = -1;
//			auto one_task = std::make_tuple(fd, num_vertex, start_vertex);
//
////			while(task_queue->test_pop_atomic(pair)) {
//			while(task_queue->test_pop_atomic(one_task)) {
////				int fd = pair.first;
////				int num_vertex = pair.second;
////				assert(fd > 0 && num_vertex > 0 );
//
//				fd = std::get<0>(one_task);
//				num_vertex = std::get<1>(one_task);
//				start_vertex = std::get<2>(one_task);
//				assert(fd > 0 && num_vertex > 0 && start_vertex >= 0);
//
//				// size_t ok??
//				size_t vertex_file_size = num_vertex * sizeof(VertexDataType);
//				char * vertex_local_buf = new char[vertex_file_size];
//
//				// for each vertex
////				for(size_t pos = 0; pos < vertex_file_size; pos += sizeof(VertexDataType)) {
////					init(vertex_local_buf + pos);
////				}
//
//				VertexId counter = 0;
//				// for each vertex
//				for(size_t pos = 0; pos < vertex_file_size; pos += sizeof(VertexDataType)) {
//					init(vertex_local_buf + pos, start_vertex + counter);
//					counter++;
//				}
//
//				io_manager::write_to_file(fd, vertex_local_buf, vertex_file_size);
//
//				delete[] vertex_local_buf;
//				close(fd);
//			}
//		}

//		template <typename VertexDataType>
//		void Engine::load_vertices_hashMap(char* vertex_local_buf, const int vertex_file_size, std::unordered_map<VertexId, VertexDataType*> & vertex_map){
//			for(size_t off = 0; off < vertex_file_size; off += vertex_unit){
//				VertexDataType* v = reinterpret_cast<VertexDataType*>(vertex_local_buf + off);
//				vertex_map[v->id] = v;
//			}
//		}

//		template <typename VertexDataType>
//		void Engine::compute_degree_producer(concurrent_queue<int> * task_queue) {
//			int partition_id = -1;
//			while(task_queue->test_pop_atomic(partition_id)) {
//				int fd_vertex = open((filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDWR);
//				int fd_edge = open((filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
//				assert(fd_vertex > 0 && fd_edge > 0 );
//
//				// get file size
//				long vertex_file_size = io_manager::get_filesize(fd_vertex);
//				long edge_file_size = io_manager::get_filesize(fd_edge);
//
//				// vertex data fully loaded into memory
//				char * vertex_local_buf = new char[vertex_file_size];
//				io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size, 0);
//				std::unordered_map<VertexId, VertexDataType*> vertex_map;
//				load_vertices_hashMap(vertex_local_buf, vertex_file_size, vertex_map);
//
//				// streaming edges
////				char * edge_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
////				int streaming_counter = edge_file_size / IO_SIZE + 1;
//				char * edge_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE * sizeof(Edge));
//				int streaming_counter = edge_file_size / (IO_SIZE * sizeof(Edge)) + 1;
//
//				long valid_io_size = 0;
//				long offset = 0;
//
//				assert(edge_unit == sizeof(Edge));
//				// for all streaming
//				for(int counter = 0; counter < streaming_counter; counter++) {
//
//					// last streaming
//					if(counter == streaming_counter - 1)
//						// TODO: potential overflow?
////						valid_io_size = edge_file_size - IO_SIZE * (streaming_counter - 1);
//						valid_io_size = edge_file_size - IO_SIZE * sizeof(Edge) * (streaming_counter - 1);
//					else
////						valid_io_size = IO_SIZE;
//						valid_io_size = IO_SIZE * sizeof(Edge);
//
//					assert(valid_io_size % sizeof(edge_unit) == 0);
//
//					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size, offset);
//					offset += valid_io_size;
//
//					for(long pos = 0; pos < valid_io_size; pos += edge_unit) {
//						// get an edge
//						Edge * e = (Edge*)(edge_local_buf + pos);
//						assert(vertex_map.find(e->src) != vertex_map.end());
//						VertexDataType * src_vertex = vertex_map.find(e->src)->second;
//						src_vertex->degree++;
//					}
//
//				}
//
//				//for debugging
////				for(size_t off = 0; off < vertex_file_size; off += vertex_unit){
////					VertexDataType* v = reinterpret_cast<VertexDataType*>(vertex_local_buf + off);
////					std::cout << *v << std::endl;
////				}
//
//				// write updated vertex value to disk
//				io_manager::write_to_file(fd_vertex, vertex_local_buf, vertex_file_size);
//
//				// delete
//				delete[] vertex_local_buf;
//				delete[] edge_local_buf;
//				close(fd_vertex);
//				close(fd_edge);
//			}
//		}


		void Engine::read_meta_file(const std::string & filename) {
			FILE * fd = fopen(filename.c_str(), "r");
			assert(fd != NULL );
			int counter = 0;
			char s[1024];
			VertexId start = 0, end = 0;

			while(fgets(s, 1024, fd) != NULL) {
				FIXLINE(s);

				char delims[] = "\t";
				char * t;
				t = strtok(s, delims);
				assert(t != NULL);

				// first line for edge_type and edge_unit
				if(counter == 0) {
					edge_type =  static_cast<EdgeType>(atoi(t));
					t = strtok(NULL, delims);
					assert(t != NULL);

					edge_unit = atoi(t);
				}
				// second line for num_vertices and num_vertices_per_part
				else if(counter == 1) {
					num_vertices = atoi(t);
					t = strtok(NULL, delims);
					assert(t != NULL);

					num_vertices_per_part = atoi(t);
				} else {
					assert(counter <= (num_partitions + 1));
					start = atoi(t);
					t = strtok(NULL, delims);
					assert(t != NULL);

					end = atoi(t);

					vertex_intervals.push_back(std::make_pair(start, end));
				}

				counter++;

			}

			fclose(fd);
		}


}


