/*
 * engine.hpp
 *
 *  Created on: Mar 3, 2017
 *      Author: kai
 */

#ifndef CORE_ENGINE_HPP_
#define CORE_ENGINE_HPP_


#include "concurrent_queue.hpp"
#include "../struct/type.hpp"
#include "../utility/FileUtil.hpp"

//#include "../preprocessor/preproc.hpp"
//#include "../preprocessor/preprocessing.hpp"
#include "../preprocessor/preprocessing_new.hpp"


namespace RStream {

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
		std::vector<std::pair<VertexId, VertexId>> vertex_intervals;

		static unsigned update_count;
		static unsigned aggregation_count;

		Engine(std::string _filename, int num_parts, int input_format);

		~Engine();

		//clean files added by Zhiqiang
		void clean_files();


		/* init vertex data*/
		template <typename VertexDataType>
		void init_vertex(std::function<void(char*)> init);

		/*compute out degree for each vertex*/
		template <typename VertexDataType>
		void compute_degree();



	private:

		template <typename VertexDataType>
		void init_produer(std::function<void(char*)> init, concurrent_queue<std::pair<int, int>> * task_queue);

		template <typename VertexDataType>
		void load_vertices_hashMap(char* vertex_local_buf, const int vertex_file_size, std::unordered_map<VertexId, VertexDataType*> & vertex_map);

		template <typename VertexDataType>
		void compute_degree_producer(concurrent_queue<int> * task_queue);

		inline bool file_exists(const std::string  filename) {
			struct stat buffer;
			return (stat(filename.c_str(), &buffer) == 0);
		}

		void read_meta_file(const std::string & filename);

		// Removes \n from the end of line
		inline void FIXLINE(char * s) {
			int len = (int) strlen(s)-1;
			if(s[len] == '\n') s[len] = 0;
		}
	};


}



#endif /* CORE_ENGINE_HPP_ */

