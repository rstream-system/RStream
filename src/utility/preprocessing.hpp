/*
 * preprocessing.hpp
 *
 *  Created on: Mar 27, 2017
 *      Author: kai
 */

#ifndef UTILITY_PREPROCESSING_HPP_
#define UTILITY_PREPROCESSING_HPP_

#include "../core/engine.hpp"

namespace RStream {
	class Preprocessing {
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_chunk_id;
		std::atomic<int> atomic_partition_number;
		std::string input;
		std::string output;
		int num_partitions;
		int num_vertices;
		int edge_type;
		int vertices_per_partition;
		int edge_unit;

	public:
		Preprocessing(std::string _input, std::string _output, int _num_partitions, int _num_vertices, int _edge_type) :
			input(_input), output(_output), num_partitions(_num_partitions), num_vertices(_num_vertices), edge_type(_edge_type)
		{
			atomic_num_producers = 0;
			atomic_chunk_id = 0;
			atomic_partition_number = num_partitions;
			vertices_per_partition = num_vertices / num_partitions;

			EdgeType e_type = static_cast<EdgeType>(edge_type);

			// size of each edge
			if(e_type == EdgeType::NO_WEIGHT) {
				edge_unit = sizeof(VertexId) * 2;
			}
			else if(e_type == EdgeType::WITH_WEIGHT) {
				edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
			}
		}

		void generate_partitions() {

			int num_threads = std::thread::hardware_concurrency();
			int num_write_threads = num_threads > 2 ? 2 : 1;
			int num_exec_threads = num_threads > 2 ? num_threads - 2 : 1;

			int fd = open(input.c_str(), O_RDONLY);
			assert(fd > 0 );

			// get file size
			long file_size = io_manager::get_filesize(fd);
			int streaming_counter = file_size / IO_SIZE + 1;
			long valid_io_size = 0;
			long offset = 0;

			concurrent_queue<std::tuple<int, long, long>> * task_queue = new concurrent_queue<std::tuple<int, long, long>>(65536);
			// <fd, offset, length>
			for(int counter = 0; counter < streaming_counter; counter++) {
				if(counter == streaming_counter - 1)
					// TODO: potential overflow?
					valid_io_size = file_size - IO_SIZE * (streaming_counter - 1);
				else
					valid_io_size = IO_SIZE;

				task_queue->push(std::make_tuple(fd, offset, valid_io_size));
				offset += valid_io_size;
			}

			// allocate global buffers for shuffling
			// TODO: Edge?
			global_buffer<Edge> ** buffers_for_shuffle = buffer_manager<Edge>::get_global_buffers(num_partitions);

			std::vector<std::thread> exec_threads;
			for(int i = 0; i < num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->producer(buffers_for_shuffle, task_queue); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < num_write_threads; i++)
				write_threads.push_back(std::thread(&Preprocessing::consumer, this, buffers_for_shuffle, task_queue->size()));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

		}

		void producer(global_buffer<Edge> ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long> > * task_queue) {
			atomic_num_producers++;

			int fd = -1;
			long offset = 0, length = 0;
			auto one_task = std::make_tuple(fd, offset, length);

			// streaming edges
			char * local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
			VertexId src = 0, dst = 0;
			Weight weight = 0.0f;

			// pop from queue
			while(task_queue->test_pop_atomic(one_task)){
				fd = std::get<0>(one_task);
				offset = std::get<1>(one_task);
				length = std::get<2>(one_task);

				io_manager::read_from_file(fd, local_buf, length, offset);
				for(long pos = 0; pos < length; pos += edge_unit) {

					src = *(VertexId*)(local_buf + pos);
					dst = *(VertexId*)(local_buf + pos + sizeof(VertexId));
					assert(src >= 0 && src < num_vertices && dst >= 0 && dst < num_vertices);

					if(edge_type == 1)
						weight = *(Weight*)(local_buf + pos + sizeof(VertexId) * 2);
					assert(weight >= 0.0f);

					// TODO: with weight and w/o weight?
					Edge * e = new Edge(src, dst, weight);
					// insert into shuffle buffer accordingly
					int index = get_global_buffer_index(src);
					global_buffer<Edge>* global_buf = buffer_manager<Edge>::get_global_buffer(buffers_for_shuffle, num_partitions, index);
					global_buf->insert(e, index);
				}
			}

			delete[] local_buf;
			atomic_num_producers--;
		}

		void consumer(global_buffer<Edge> ** buffers_for_shuffle, int num_chunks) {
			while(atomic_num_producers != 0) {
				int i = (atomic_chunk_id++) % num_chunks ;

//				//debugging info
//				print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");

				const char * file_name = (output + "." + std::to_string(i)).c_str();
				global_buffer<Edge>* g_buf = buffer_manager<Edge>::get_global_buffer(buffers_for_shuffle, num_partitions, i);
				g_buf->flush(file_name, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;
//				std::cout << i << std::endl;
				if(i >= 0){
//					//debugging info
//					print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");

					const char * file_name = (output + "." + std::to_string(i)).c_str();
					global_buffer<Edge>* g_buf = buffer_manager<Edge>::get_global_buffer(buffers_for_shuffle, num_partitions, i);
					g_buf->flush_end(file_name, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}

		int get_global_buffer_index(int src) {
			int partition_id = src/ vertices_per_partition;
			return partition_id < (num_partitions - 1) ? partition_id : (num_partitions - 1);
		}
	};

}



#endif /* UTILITY_PREPROCESSING_HPP_ */
