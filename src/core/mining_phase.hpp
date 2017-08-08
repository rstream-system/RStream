/*
 * mining_phase.hpp
 *
 *  Created on: Aug 3, 2017
 *      Author: icuzzq
 */

#ifndef CORE_MINING_PHASE_HPP_
#define CORE_MINING_PHASE_HPP_

#include "engine.hpp"
#include "meta_info.hpp"
#include "buffer_manager.hpp"
#include "pattern.hpp"
#include "../utility/Logger.hpp"


namespace RStream {


	class MPhase {

	public:
		MPhase(Engine & e, unsigned int maxsize);
		virtual ~MPhase();


		/* Public Functions */
		Update_Stream init();

		// gen shuffled init update stream based on edge partitions
		Update_Stream init_shuffle_all_keys();

		/** join update stream with edge stream to generate non-shuffled update stream
		 * @param in_update_stream: which is shuffled
		 * @param out_update_stream: which is non-shuffled
		 */
		Update_Stream join_all_keys_nonshuffle(Update_Stream in_update_stream);

		/** join update stream with edge stream to generate non-shuffled update stream
		 * @param in_update_stream: which is shuffled
		 * @param out_update_stream: which is non-shuffled
		 */
		Update_Stream join_mining(Update_Stream in_update_stream);

		/* join update stream with edge stream, shuffle on all keys
		 * @param in_update_stream -input file for update stream
		 * @param out_update_stream -output file for update stream
		 * */
		Update_Stream join_all_keys(Update_Stream in_update_stream);


		/** shuffle all keys on input update stream (in_update_stream)
		 * @param in_update_stream: which is non-shuffled
		 * @param out_update_stream: which is shuffled
		 */
		Update_Stream shuffle_all_keys(Update_Stream in_update_stream);

		Update_Stream collect(Update_Stream in_update_stream);

		void printout_upstream(Update_Stream in_update_stream);

		void delete_upstream(Update_Stream in_update_stream);


		/*getter*/
		inline unsigned int get_sizeof_in_tuple(){
			return sizeof_in_tuple;
		}

		inline unsigned int get_max_size(){
			return max_size;
		}

		/* Public Static Functions */
		static void printout_edgehashmap(std::vector<Element_In_Tuple>* edge_hashmap, VertexId n_vertices) {
			for (int i = 0; i < n_vertices; ++i) {
				std::cout << i << ": " << edge_hashmap[i] << std::endl;
			}
		}

		static bool task_comparator(std::tuple<int, long, long> t1, std::tuple<int, long, long> t2){
			return std::get<2>(t1) > std::get<2>(t2);
		}

		static long get_real_io_size(long io_size, int size_of_unit){
			long real_io_size = io_size - io_size % size_of_unit;
			assert(real_io_size % size_of_unit == 0);
			return real_io_size;
		}

		static concurrent_queue<std::tuple<int, long, long>>* divide_tasks(const int num_partitions, const std::string& filename, Update_Stream update_stream, int sizeof_in_tuple,
				 long chunk_unit){
			long real_chunk_unit = get_real_io_size(chunk_unit, sizeof_in_tuple);
			std::vector<std::tuple<int, long, long>> tasks;

			// divide in update stream into smaller chunks, to get better workload balance
			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
				int fd_update = open((filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(update_stream)).c_str(), O_RDONLY);
				long update_size = io_manager::get_filesize(fd_update);

				int chunk_counter = update_size / real_chunk_unit + 1;

				long valid_io_size = 0;
				long offset = 0;

				for(int counter = 0; counter < chunk_counter; counter++) {
					// last streaming
					if(counter == chunk_counter - 1)
						valid_io_size = update_size - real_chunk_unit * (chunk_counter - 1);
					else
						valid_io_size = real_chunk_unit;

					tasks.push_back(std::make_tuple(partition_id, offset, valid_io_size));
					offset += valid_io_size;
				}
			}

			std::sort(tasks.begin(), tasks.end(), task_comparator);

			concurrent_queue<std::tuple<int, long, long>> * task_queue = new concurrent_queue<std::tuple<int, long, long>>();
			for(auto it = tasks.begin(); it != tasks.end(); ++it){
				task_queue->push(*it);
			}
			return task_queue;
		}

		static void get_an_in_update(char * update_local_buf, std::vector<Element_In_Tuple> & tuple, int sizeof_in_tuple) {
			for(int index = 0; index < sizeof_in_tuple; index += sizeof(Element_In_Tuple)) {
				Element_In_Tuple element = *(Element_In_Tuple*)(update_local_buf + index);
				tuple.push_back(element);
			}
		}

		static void get_an_in_update(char * update_local_buf, MTuple & tuple) {
			tuple.init(update_local_buf);
		}

		static void get_an_in_update(char * update_local_buf, MTuple_join & tuple, std::unordered_set<VertexId>& vertices_set) {
			tuple.init(update_local_buf, vertices_set);
		}

		static void get_an_in_update(char * update_local_buf, std::vector<Element_In_Tuple> & tuple, int sizeof_in_tuple, std::unordered_set<VertexId>& vertices_set) {
			for(int index = 0; index < sizeof_in_tuple; index += sizeof(Element_In_Tuple)) {
				Element_In_Tuple element = *(Element_In_Tuple*)(update_local_buf + index);
				tuple.push_back(element);
				vertices_set.insert(element.vertex_id);
			}
		}

		static std::string get_string_task_tuple(std::tuple<int, long, long>& task_id){
			return "<" + std::to_string(std::get<0>(task_id)) + ", " + std::to_string(std::get<1>(task_id)) + ", " + std::to_string(std::get<2>(task_id)) + ">";
		}

		static void delete_upstream_static(Update_Stream in_update_stream, const int num_partitions, const std::string& file_name){
			for(int partition_id = 0; partition_id < num_partitions; partition_id++) {
				std::string filename = file_name + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream);
				FileUtil::delete_file(filename);
			}
		}


	protected:
		unsigned int max_size;

		unsigned int get_num_vertices(std::vector<Element_In_Tuple> & update_tuple);

		unsigned int get_num_vertices(MTuple & update_tuple);


	private:
		void atomic_init();

		unsigned int get_count(Update_Stream in_update_stream);

		void edges_loader(std::vector<Element_In_Tuple>* edge_hashmap, concurrent_queue<int> * read_task_queue);

		// each exec thread generates a join producer
		void join_all_keys_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue);

//		// each exec thread generates a join producer
//		void join_allkeys_nonshuffle_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, std::vector<Element_In_Tuple> * edge_hashmap);

		void join_allkeys_nonshuffle_tuple_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue, std::vector<Element_In_Tuple> * edge_hashmap);

		// each exec thread generates a join producer
		void join_mining_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue);

		void insert_tuple_to_buffer(int partition_id, std::vector<Element_In_Tuple>& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle);
		void insert_tuple_to_buffer(int partition_id, MTuple_join& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle);
		void insert_tuple_to_buffer(int partition_id, MTuple& in_update_tuple, global_buffer_for_mining** buffers_for_shuffle);

		void shuffle_all_keys_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue);

		void collect_producer(Update_Stream in_update_stream, global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue);

		void init_producer(global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue);

		void shuffle_all_keys_producer_init(global_buffer_for_mining ** buffers_for_shuffle, concurrent_queue<int> * task_queue);

		// each writer thread generates a join_consumer
		void consumer(Update_Stream out_update_stream, global_buffer_for_mining ** buffers_for_shuffle);

		void shuffle_on_all_keys(std::vector<Element_In_Tuple> & out_update_tuple, global_buffer_for_mining ** buffers_for_shuffle);
		void shuffle_on_all_keys(MTuple & out_update_tuple, global_buffer_for_mining ** buffers_for_shuffle);

		bool gen_an_out_update(std::vector<Element_In_Tuple> & in_update_tuple, Element_In_Tuple & element, BYTE history, std::unordered_set<VertexId>& vertices_set);

		bool gen_an_out_update(MTuple_join & in_update_tuple, Element_In_Tuple & element, BYTE history, std::unordered_set<VertexId>& vertices_set);

		// key index is always stored in the first element of the vector
		BYTE get_key_index(std::vector<Element_In_Tuple> & in_update_tuple);
		BYTE get_key_index(MTuple_join & in_update_tuple);

		void set_key_index(std::vector<Element_In_Tuple> & out_update_tuple, int new_key_index);
		void set_key_index(MTuple & out_update_tuple, int new_key_index);

		// TODO: do we need to store src.label?
		void build_edge_hashmap(char * edge_buf, std::vector<Element_In_Tuple> * edge_hashmap, size_t edge_file_size, int start_vertex);

		int get_global_buffer_index(VertexId key);


//		virtual bool filter_join(std::vector<Element_In_Tuple> & update_tuple) = 0;
//		virtual bool filter_collect(std::vector<Element_In_Tuple> & update_tuple) = 0;

		virtual bool filter_join(MTuple_join & update_tuple) = 0;
		virtual bool filter_collect(MTuple & update_tuple) = 0;


		/* Private fields */
		const Engine context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

		// num of bytes for in_update_tuple
		unsigned int sizeof_in_tuple;

	};
}



#endif /* CORE_MINING_PHASE_HPP_ */
