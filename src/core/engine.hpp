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
#include "../utility/preprocessing.hpp"

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

		int num_vertices;
		int num_vertices_per_part;

//		int* vertex_intervals;

		static unsigned update_count;

		Engine(std::string _filename, int num_parts, int _num_vertices) : filename(_filename) {
//			num_threads = std::thread::hardware_concurrency();
			num_threads = 4;
			num_write_threads = num_threads > 2 ? 2 : 1;
			num_exec_threads = num_threads > 2 ? num_threads - 2 : 1;

			num_vertices = _num_vertices;
			Preprocessing proc(_filename, num_parts, num_vertices);

			num_partitions = num_parts;
			edge_type = static_cast<EdgeType>(proc.getEdgeType());
			edge_unit = proc.getEdgeUnit();
			vertex_unit = 8;
			num_vertices_per_part = proc.getNumVerPerPartition();

			std::cout << "Number of partitions: " << num_partitions << std::endl;
			std::cout << "Edge type: " << edge_type << std::endl;
			std::cout << "Number of bytes per edge: " << edge_unit << std::endl;
			std::cout << "Number of exec threads: " << num_exec_threads << std::endl;
			std::cout << "Number of write threads: " << num_write_threads << std::endl;
			std::cout << std::endl;
		}

		~Engine(){
//			delete[] vertex_intervals;
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
		template <typename VertexDataType>
		void init_vertex(std::function<void(char*)> init) {
			// a pair of <vertex_file, num_vertices>
			concurrent_queue<std::pair<int, int>> * task_queue = new concurrent_queue<std::pair<int, int>>(num_partitions);

			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
				int perms = O_WRONLY;
				std::string vertex_file = filename + "." + std::to_string(partition_id) + ".vertex";
				int fd = open(vertex_file.c_str(), perms, S_IRWXU);
				if(fd < 0) {
					fd = creat(vertex_file.c_str(), S_IRWXU);
				}
				task_queue->push(std::make_pair(fd, num_vertices_per_part));

			}

			// threads will load vertex and update, and apply update one by one
			std::vector<std::thread> threads;
			for(int i = 0; i < num_threads; i++)
				threads.push_back(std::thread(&Engine::init_produer<VertexDataType>, this, init, task_queue));

			// join all threads
			for(auto & t : threads)
				t.join();
		}



	protected:

		template <typename VertexDataType>
		void init_produer(std::function<void(char*)> init, concurrent_queue<std::pair<int, int>> * task_queue) {
			std::pair<int, int> pair(-1, -1);
			while(task_queue->test_pop_atomic(pair)) {
				int fd = pair.first;
				int num_vertex = pair.second;
				assert(fd > 0 && num_vertex > 0 );

				// size_t ok??
				size_t vertex_file_size = num_vertex * sizeof(VertexDataType);
				char * vertex_local_buf = new char[vertex_file_size];

				// for each vertex
				for(size_t pos = 0; pos < vertex_file_size; pos += sizeof(int)) {
					init(vertex_local_buf + pos);
				}

				io_manager::write_to_file(fd, vertex_local_buf, vertex_file_size);

				delete[] vertex_local_buf;
				close(fd);
			}
		}

	};

	unsigned Engine::update_count = 0;
}



#endif /* CORE_ENGINE_HPP_ */

