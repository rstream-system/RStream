/*
 * aggregation.hpp
 *
 *  Created on: Aug 4, 2017
 *      Author: icuzzq
 */

#ifndef SRC_CORE_AGGREGATION_HPP_
#define SRC_CORE_AGGREGATION_HPP_

#include "mining_phase.hpp"

namespace RStream {

	class Aggregation {


	public:

		//constructor
		Aggregation(Engine & e, bool label_f);

		virtual ~Aggregation();


		/*
		 * do aggregation for update stream
		 * @param: in update stream
		 * @return: aggregation stream
		 * */
		Aggregation_Stream aggregate(Update_Stream in_update_stream, int sizeof_in_tuple);

		Update_Stream aggregate_filter(Update_Stream up_stream, Aggregation_Stream agg_stream, int sizeof_in_tuple, int threshold);

		void printout_aggstream(Aggregation_Stream agg_stream);

		void delete_aggstream(Aggregation_Stream agg_stream);


		/*Static functions*/
		static void write_buf_to_file(const char* file_name, char * local_buf, size_t fsize){
			int perms = O_WRONLY | O_APPEND;
			int fd = open(file_name, perms, S_IRWXU);
			if(fd < 0){
				fd = creat(file_name, S_IRWXU);
			}
			// flush buffer to update out stream
			io_manager::write_to_file(fd, local_buf, fsize);
			close(fd);
		}

		static char* convert_to_bytes(size_t sizeof_agg_pair, std::pair<const Canonical_Graph, int>& it_pair){
			Canonical_Graph canonical_graph = it_pair.first;
			int s = it_pair.second;

			char* out_cg = (char *)malloc(sizeof_agg_pair);
			size_t s_vector = sizeof_agg_pair- sizeof(unsigned int) * 2 - sizeof(int);
			std::memcpy(out_cg, reinterpret_cast<char*>(canonical_graph.get_tuple_const().data()), s_vector);
			unsigned int num_vertices = canonical_graph.get_number_vertices();
			std::memcpy(out_cg + s_vector, &num_vertices, sizeof(unsigned int));
			unsigned int hash_value = canonical_graph.get_hash();
			std::memcpy(out_cg + s_vector + sizeof(unsigned int), &hash_value, sizeof(unsigned int));

			std::memcpy(out_cg + s_vector + sizeof(unsigned int) * 2, &s, sizeof(int));

			return out_cg;
		}

		static int get_out_size(int sizeof_in_tuple){
			return sizeof_in_tuple + sizeof(unsigned int) * 2 + sizeof(int);
		}


	private:
		/* Private Fields */
		const Engine & context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

		bool label_flag;


		/* Private Functions */
		void atomic_init();

		Update_Stream shuffle_upstream_canonicalgraph(Update_Stream in_update_stream, int sizeof_in_tuple);

		void shuffle_on_canonical_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, int sizeof_in_tuple);

		void shuffle_on_canonical(std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining ** buffers_for_shuffle);

		void insert_tuple_to_buffer(int partition_id, std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle);


		Update_Stream aggregate_filter_local(Update_Stream up_stream_shuffled_on_canonical, Aggregation_Stream agg_stream, int sizeof_in_tuple, int threshold);

		void aggregate_filter_local_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, int sizeof_in_tuple, Aggregation_Stream agg_stream, int threshold);

		void build_aggmap(std::unordered_map<Canonical_Graph, int>& map, char* agg_local_buf, long agg_file_size, int sizeof_agg);

		bool filter_aggregate(std::vector<Element_In_Tuple> & update_tuple, std::unordered_map<Canonical_Graph, int>& map, int threshold);

		Aggregation_Stream aggregate_local(Update_Stream in_update_stream, int sizeof_in_tuple);

		Aggregation_Stream aggregate_global(Aggregation_Stream in_agg_stream, int sizeof_in_agg);

		void aggregate_global_per_thread(Aggregation_Stream in_agg_stream, concurrent_queue<int> * task_queue, int sizeof_in_agg, Aggregation_Stream out_agg_stream);

		void aggregate_on_canonical_graph(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, std::pair<Canonical_Graph, int>& in_agg_pair);

		void get_an_in_agg_pair(char * update_local_buf, std::pair<Canonical_Graph, int> & agg_pair, int sizeof_in_agg);

		void write_canonical_aggregation(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, std::string& file_name, unsigned int sizeof_in_agg);

		void aggregate_local_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, int sizeof_in_tuple);

		//for debugging only
		void printout_quickpattern_aggmap(std::unordered_map<Quick_Pattern, int>& quick_patterns_aggregation);

		//for debugging only
		void printout_cg_aggmap(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation);

		void aggregate_on_quick_pattern(std::unordered_map<Quick_Pattern, int>& quick_patterns_aggregation, Quick_Pattern& quick_pattern);

		void aggregate_on_canonical_graph(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, std::unordered_map<Quick_Pattern, int>& quick_patterns_aggregation);


		void shuffle_canonical_aggregation(std::unordered_map<Canonical_Graph, int>& canonical_graphs_aggregation, global_buffer_for_mining ** buffers_for_shuffle);

		// each writer thread generates a join_consumer
		void aggregate_consumer(Aggregation_Stream aggregation_stream, global_buffer_for_mining ** buffers_for_shuffle);

		void update_consumer(Update_Stream out_update_stream, global_buffer_for_mining ** buffers_for_shuffle);

		unsigned int get_global_bucket_index(unsigned int hash_val);


	};
}



#endif /* SRC_CORE_AGGREGATION_HPP_ */
