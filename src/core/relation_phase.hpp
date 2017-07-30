/*
 * relation_phase.hpp
 *
 *  Created on: Mar 16, 2017
 *      Author: kai
 */

#ifndef CORE_RELATION_PHASE_HPP_
#define CORE_RELATION_PHASE_HPP_

#include "scatter.hpp"

namespace RStream {
	template<typename InUpdateType, typename OutUpdateType>
	class RPhase {
		static_assert(
			std::is_base_of<BaseUpdate, InUpdateType>::value,
			"OldUpdateType must be a subclass of BaseUpdate."
		);

		static_assert(
			std::is_base_of<BaseUpdate, OutUpdateType>::value,
			"NewUpdateType must be a subclass of BaseUpdate."
		);

		const Engine & context;
		std::atomic<int> atomic_num_producers;
//		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

	public:
//		struct JoinResultType {
//			InUpdateType old_update;
//			VertexId target;
//
//			JoinResultType() : target(0) {};
//			JoinResultType(InUpdateType u, VertexId t) : old_update(u), target(t) {};
//		};

//		virtual bool filter(InUpdateType * update, Edge * edge) = 0;
//		virtual OutUpdateType * project_columns(InUpdateType * in_update, Edge * edge) = 0;
		virtual bool filter(InUpdateType * update, VertexId edge_src, VertexId edge_dst) = 0;
		virtual OutUpdateType * project_columns(InUpdateType * in_update, VertexId edge_src, VertexId edge_dst) = 0;

//		virtual int new_key();

		RPhase(Engine & e) : context(e) {}

		void atomic_init() {
			atomic_num_producers = context.num_exec_threads;
			atomic_partition_number = context.num_partitions;
		}

		virtual ~RPhase() {}

		/* join update stream with edge stream
		 * @param in_update_stream -input file for update stream
		 * @param out_update_stream -output file for update stream
		 * */
		Update_Stream join(Update_Stream in_update_stream) {
			atomic_init();

			print_thread_info_locked("--------------------Start Join Phase--------------------\n\n");

			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
//				std::cout << partition_id << std::endl;
			}

			// allocate global buffers for shuffling
			global_buffer<OutUpdateType> ** buffers_for_shuffle = buffer_manager<OutUpdateType>::get_global_buffers(context.num_partitions);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->join_producer(in_update_stream, buffers_for_shuffle, task_queue); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&RPhase::join_consumer, this, update_c, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			print_thread_info_locked("--------------------Finish Join Phase--------------------\n\n");

			return update_c;
		}

		/* compute set difference for the two update_stream
		 * result = update_stream1 - update_stream2
		 * */
		Update_Stream set_difference(Update_Stream update_stream1, Update_Stream update_stream2) {
			atomic_init();

			print_thread_info_locked("--------------------Start Set Difference Phase--------------------\n\n");

			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
//				std::cout << partition_id << std::endl;
			}

			global_buffer<OutUpdateType> ** buffers = buffer_manager<OutUpdateType>::get_global_buffers(context.num_partitions);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->set_difference_producer(update_stream1, update_stream2, buffers, task_queue); } ));

			// write threads will flush buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&RPhase::set_difference_consumer, this, update_c, buffers));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers;
			delete task_queue;

			print_thread_info_locked("--------------------Finish Set Difference Phase--------------------\n\n");

			return update_c;
		}

		// append update_stream2 to the end of update_stream1
		void union_relation (Update_Stream update_stream1, Update_Stream update_stream2) {
			print_thread_info_locked("--------------------Start Union Phase--------------------\n\n");
//			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->union_relation_worker(update_stream1, update_stream2, task_queue); } ));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			delete task_queue;

			print_thread_info_locked("--------------------Finish Union Phase--------------------\n\n");

		}

		Update_Stream remove_dup(Update_Stream update_stream) {
			print_thread_info_locked("--------------------Start Remove Duplicates Phase--------------------\n\n");

			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->remove_dup_worker(update_stream, update_c, task_queue); } ));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			delete task_queue;

			print_thread_info_locked("--------------------Finish Remove Duplicates Phase--------------------\n\n");

			return update_c;
		}

	private:
		// each exec thread generates a join producer
		void join_producer(Update_Stream in_update_stream, global_buffer<OutUpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
//			atomic_num_producers++;
			int partition_id = -1;

			for(unsigned int i = 0; i < context.num_partitions; i++) {
				assert(buffers_for_shuffle[i]->get_capacity() == BUFFER_CAPACITY);
			}

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){

				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_update > 0 && fd_edge > 0 );

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);
				long edge_file_size = io_manager::get_filesize(fd_edge);

				print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id)
						+ " of update size " + std::to_string(update_file_size) + ", edge file size " + std::to_string(edge_file_size) + "\n");

				// read from files to thread local buffer
//				char * update_local_buf = new char[update_file_size];
//				io_manager::read_from_file(fd_update, update_local_buf, update_file_size);

				// streaming updates
//				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
//				int streaming_counter = update_file_size / IO_SIZE + 1;

				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE * sizeof(InUpdateType));
				int streaming_counter = update_file_size / (IO_SIZE * sizeof(InUpdateType)) + 1;
				assert((update_file_size % sizeof(InUpdateType)) == 0);

				// edges are fully loaded into memory
				char * edge_local_buf = new char[edge_file_size];
				io_manager::read_from_file(fd_edge, edge_local_buf, edge_file_size, 0);

				// build edge hashmap
				const int n_vertices = context.vertex_intervals[partition_id].second - context.vertex_intervals[partition_id].first + 1;
//				int start_vertex = partition_id * num_vertices;
				int vertex_start = context.vertex_intervals[partition_id].first;
				assert(n_vertices > 0 && vertex_start >= 0);

//				std::array<std::vector<VertexId>, num_vertices> edge_hashmap;
//				std::vector<VertexId> edge_hashmap[n_vertices];

				std::vector<std::vector<VertexId>> edge_hashmap(n_vertices);
//				for(unsigned int i = 0; i < edge_hashmap.size(); i++)
//					edge_hashmap[i] = std::vector<VertexId>();

				build_edge_hashmap(edge_local_buf, edge_hashmap, edge_file_size, vertex_start);

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
//						valid_io_size = update_file_size - IO_SIZE * (streaming_counter - 1);
						valid_io_size = update_file_size - IO_SIZE * sizeof(InUpdateType) * (streaming_counter - 1);
					else
//						valid_io_size = IO_SIZE;
						valid_io_size = IO_SIZE * sizeof(InUpdateType);

					assert(valid_io_size % sizeof(InUpdateType) == 0);
					print_thread_info_locked(std::to_string(counter) + "th streaming, start join of size "
							+ std::to_string(valid_io_size) + " with partition " + std::to_string(partition_id) + "\n");

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// streaming updates in, do hash join
					for(long pos = 0; pos < valid_io_size; pos += sizeof(InUpdateType)) {
						// get an update
						InUpdateType * update = (InUpdateType*)(update_local_buf + pos);

						// update.target is edge.src, the key to index edge_hashmap
						for(VertexId target : edge_hashmap[update->target - vertex_start]) {
//							Edge * e = new Edge(update->target, target);
//							if(!filter(update, e)) {
							if(!filter(update, update->target, target)) {
	//							NewUpdateType * new_update = new NewUpdateType(update, target);

								//TODO: generate join result
//								char* join_result = reinterpret_cast<char*>(&update);
//								OutUpdateType * out_update = project_columns(update, e);
								OutUpdateType * out_update = project_columns(update, update->target, target);
//								std::cout << *e << std::endl;
//								std::cout << *update << std::endl;
//								std::cout << *out_update << std::endl;

								// insert into shuffle buffer accordingly
//								int index = get_global_buffer_index(out_update);
								int index = meta_info::get_index(out_update->target, context);
								assert(index >= 0);

								global_buffer<OutUpdateType>* global_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
								global_buf->insert(out_update, index);

								delete out_update;

							}
//							delete e;
						}

					}

					print_thread_info_locked(std::to_string(counter) + "th streaming, finish join of size "
												+ std::to_string(valid_io_size) + " with partition " + std::to_string(partition_id) + "\n");

				}

				// streaming updates in, do hash join
//				for(size_t pos = 0; pos < update_file_size; pos += sizeof(InUpdateType)) {
//					// get an update
//					InUpdateType & update = *(InUpdateType*)(update_local_buf + pos);
//
//					// update.target is edge.src, the key to index edge_hashmap
//					for(VertexId target : edge_hashmap[update.target - start_vertex]) {
//						if(!filter(update, update.target, target)) {
////							NewUpdateType * new_update = new NewUpdateType(update, target);
//
//							//TODO: generate join result
//							char* join_result = reinterpret_cast<char*>(&update);
//							OutUpdateType * out_update = nullptr;
//							project_columns(join_result, out_update);
//
//							// insert into shuffle buffer accordingly
//							int index = get_global_buffer_index(out_update);
//							global_buffer<OutUpdateType>* global_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
//							global_buf->insert(out_update, index);
//
//						}
//					}
//
//				}

				free(update_local_buf);
				delete[] edge_local_buf;

				close(fd_update);
				close(fd_edge);
			}

			atomic_num_producers--;
		}

		void join_consumer(Update_Stream out_update_stream, global_buffer<OutUpdateType> ** buffers_for_shuffle) {
			consumer(out_update_stream, buffers_for_shuffle);
		}

		// each writer thread generates a join_consumer
		void consumer(Update_Stream out_update_stream, global_buffer<OutUpdateType> ** buffers_for_shuffle) {
			int counter = 0;

			while(atomic_num_producers != 0) {
//				int i = (atomic_partition_id++) % context.num_partitions ;
				if(counter == context.num_partitions)
					counter = 0;

				int i = counter++;

				const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(out_update_stream)).c_str();
				std::string file_name_str = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(out_update_stream));

				global_buffer<OutUpdateType>* g_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
//				g_buf->flush(file_name, i);
				g_buf->flush(file_name_str, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;
//				std::cout << i << std::endl;
				if(i >= 0){
//					//debugging info
//					print_thread_info("as a consumer dealing with buffer[" + std::to_string(i) + "]\n");

//					const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(out_update_stream)).c_str();

					std::string file_name_str = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(out_update_stream));
					global_buffer<OutUpdateType>* g_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
//					g_buf->flush_end(file_name, i);
					g_buf->flush_end(file_name_str, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}

		void set_difference_producer(Update_Stream update_stream1, Update_Stream update_stream2, global_buffer<OutUpdateType> ** buffers, concurrent_queue<int> * task_queue) {
//			atomic_num_producers++;
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){

				int fd_update1 = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(update_stream1)).c_str(), O_RDONLY);
				int fd_update2 = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(update_stream2)).c_str(), O_RDONLY);
				assert(fd_update1 > 0 && fd_update2 > 0 );

				// get file size
				long update1_file_size = io_manager::get_filesize(fd_update1);
				long update2_file_size = io_manager::get_filesize(fd_update2);

				// streaming update1
//				char * update1_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
//				int streaming_counter = update1_file_size / IO_SIZE + 1;
				char * update1_buf = (char *)memalign(PAGE_SIZE, IO_SIZE * sizeof(OutUpdateType));
				int streaming_counter = update1_file_size / (IO_SIZE * sizeof(OutUpdateType)) + 1;

				// Assumption: update2 can be fully loaded into memory
				char * update2_buf = new char[update2_file_size];
				io_manager::read_from_file(fd_update2, update2_buf, update2_file_size, 0);

				std::unordered_set<OutUpdateType> set_of_updates2;
				build_update_hashset(update2_buf, set_of_updates2, update2_file_size);

				long valid_io_size = 0;
				long offset = 0;

				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
//						valid_io_size = update1_file_size - IO_SIZE * (streaming_counter - 1);
						valid_io_size = update1_file_size - IO_SIZE * sizeof(OutUpdateType) * (streaming_counter - 1);
					else
//						valid_io_size = IO_SIZE;
						valid_io_size = IO_SIZE * sizeof(OutUpdateType);

					assert(valid_io_size % sizeof(OutUpdateType) == 0);

					io_manager::read_from_file(fd_update1, update1_buf, valid_io_size, offset);
					offset += valid_io_size;

					// streaming update1 in, do set difference
					for(long pos = 0; pos < valid_io_size; pos += sizeof(OutUpdateType)) {
						// get an update1
						OutUpdateType * one_update1 = (OutUpdateType*)(update1_buf + pos);
						auto existed = set_of_updates2.find(*one_update1);

						if(existed != set_of_updates2.end())
							continue;

						int index = partition_id;
						global_buffer<OutUpdateType>* global_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers, context.num_partitions, index);
						global_buf->insert(one_update1, index);
					}
				}

				free(update1_buf);
				delete[] update2_buf;

				close(fd_update1);
				close(fd_update2);
			}

			atomic_num_producers--;
		}

		void set_difference_consumer(Update_Stream out_update_stream, global_buffer<OutUpdateType> ** buffers) {
			consumer(out_update_stream, buffers);
		}

		void union_relation_worker(Update_Stream update_stream1, Update_Stream update_stream2, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				int fd_update1 = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(update_stream1)).c_str(), O_WRONLY | O_APPEND);
				int fd_update2 = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(update_stream2)).c_str(), O_RDONLY);
				assert(fd_update1 > 0 && fd_update2 > 0 );

				// get file size
				long update1_file_size = io_manager::get_filesize(fd_update1);
				long update2_file_size = io_manager::get_filesize(fd_update2);

				// Assumption: update2 can be fully loaded into memory
				char * update2_buf = new char[update2_file_size];
				io_manager::read_from_file(fd_update2, update2_buf, update2_file_size, 0);

				// append update2 to update1
				io_manager::append_to_file(fd_update1, update2_buf, update2_file_size);

				delete[] update2_buf;

				close(fd_update1);
				close(fd_update2);
			}

		}

		void remove_dup_worker(Update_Stream in_update_stream, Update_Stream out_update_stream, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			while(task_queue->test_pop_atomic(partition_id)) {
				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);
				char * update_buf = (char *)memalign(PAGE_SIZE, IO_SIZE * sizeof(OutUpdateType));
				int streaming_counter = update_file_size / (IO_SIZE * sizeof(OutUpdateType)) + 1;

				std::unordered_set<OutUpdateType> set_of_updates;

				long valid_io_size = 0;
				long offset = 0;
				// for all streaming updates
				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = update_file_size - IO_SIZE * sizeof(OutUpdateType) * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE * sizeof(OutUpdateType);

					assert(valid_io_size % sizeof(OutUpdateType) == 0);
					io_manager::read_from_file(fd_update, update_buf, valid_io_size, offset);
					offset += valid_io_size;

					build_update_hashset(update_buf, set_of_updates, valid_io_size);
				}

				std::vector<OutUpdateType> out_updates;
				std::copy(set_of_updates.begin(), set_of_updates.end(), std::back_inserter(out_updates));
//				const char * file_name = (context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(out_update_stream)).c_str();

				std::string file_name_str = (context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(out_update_stream));
				char* buf = reinterpret_cast<char*>(out_updates.data());
				write_updates_to_file(buf, file_name_str, out_updates.size() * sizeof(OutUpdateType));

				free(update_buf);
				close(fd_update);
			}
		}

		void write_updates_to_file(char * buf, std::string file, size_t length) {
			const char * file_name = file.c_str();
			int perms = O_WRONLY | O_APPEND;
			int fd = open(file_name, perms, S_IRWXU);
			if(fd < 0){
				fd = creat(file_name, S_IRWXU);
			}

			// flush buffer to update out stream
			io_manager::write_to_file(fd, buf, length);
			close(fd);
		}

		void build_edge_hashmap(char * edge_buf, std::vector<std::vector<VertexId>> & edge_hashmap, size_t edge_file_size, int start_vertex) {
			int edge_unit = context.edge_unit;
			assert(edge_unit > 0);
			// for each edge
			for(size_t pos = 0; pos < edge_file_size; pos += edge_unit) {
				// get an edge
				Edge e = *(Edge*)(edge_buf + pos);
				assert(e.src >= start_vertex);
				// e.src is the key
				edge_hashmap[e.src - start_vertex].push_back(e.target);
			}
		}

		void build_update_hashset(char * update_buf, std::unordered_set<OutUpdateType> & set_of_updates, size_t update_file_size) {
			// for each update
			for(size_t pos = 0;  pos < update_file_size; pos += sizeof(OutUpdateType)) {
				// get an update
				OutUpdateType  one_update = *(OutUpdateType*)(update_buf + pos);
				set_of_updates.insert(one_update);

			}
		}
//
//		int get_global_buffer_index(OutUpdateType* new_update) {
////			return new_update->target / context.num_vertices_per_part;
//			int partition_id = new_update->target / context.num_vertices_per_part;
//			return partition_id < (context.num_partitions - 1) ? partition_id : (context.num_partitions - 1);
//
////			int target = new_update->target;
////
////			int lb = 0, ub = context.num_partitions;
////			int i = (lb + ub) / 2;
////
////			while(true){
//////				int c = context.vertex_intervals[i];
////				if(i == 0){
////					return 0;
////				}
//////				int p = context.vertex_intervals[i - 1];
////				if(c >= target && p < target){
////					return i;
////				}
////				else if(c > target){
////					ub = i;
////				}
////				else if(c < target){
////					lb = i;
////				}
////				i = (lb + ub) / 2;
////			}
//		}

	};
}



#endif /* CORE_RELATION_PHASE_HPP_ */
