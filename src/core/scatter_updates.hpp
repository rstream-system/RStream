/*
 * scatter_updates.hpp
 *
 *  Created on: Jul 11, 2017
 *      Author: kai
 */

#ifndef CORE_SCATTER_UPDATES_HPP_
#define CORE_SCATTER_UPDATES_HPP_

#include "io_manager.hpp"
#include "buffer_manager.hpp"
#include "concurrent_queue.hpp"
#include "type.hpp"
#include "constants.hpp"

namespace RStream {

	template <typename InUpdateType, typename OutUpdateType>
	class Scatter_Updates {
		const Engine& context;

		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_id;
		std::atomic<int> atomic_partition_number;

	public:

		Scatter_Updates(Engine & e) : context(e) {};

		void atomic_init() {
			atomic_num_producers = context.num_exec_threads;
			atomic_partition_number = context.num_partitions;
		}

		/*
		 * given an in_update, generate an out_update and scatter
		 * */
		Update_Stream scatter_updates(Update_Stream in_update_stream, std::function<OutUpdateType*(InUpdateType*)> generate_one_update) {
			atomic_init();

			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// allocate global buffers for shuffling
			global_buffer<OutUpdateType> ** buffers_for_shuffle = buffer_manager<OutUpdateType>::get_global_buffers(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back(std::thread([=] { this->scatter_updates_producer(in_update_stream, generate_one_update, buffers_for_shuffle, task_queue); }));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&Scatter_Updates::scatter_updates_consumer, this, buffers_for_shuffle, update_c));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			return update_c;
		}

	private:
		void scatter_updates_producer(Update_Stream in_update_stream, std::function<OutUpdateType*(InUpdateType*)> generate_one_update,
						global_buffer<OutUpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {

//			atomic_num_producers++;
			int partition_id = -1;

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				int fd_update = open((context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream)).c_str(), O_RDONLY);
				assert(fd_update > 0);

				// get file size
				long update_file_size = io_manager::get_filesize(fd_update);

				// streaming updates
//				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
//				int streaming_counter = update_file_size / IO_SIZE + 1;
				char * update_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE * sizeof(InUpdateType));
				int streaming_counter = update_file_size / (IO_SIZE * sizeof(InUpdateType)) + 1;

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

					io_manager::read_from_file(fd_update, update_local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// streaming updates in
					for(long pos = 0; pos < valid_io_size; pos += sizeof(InUpdateType)) {
						// get an in_update
						InUpdateType * in_update = (InUpdateType*)(update_local_buf + pos);

						// gen an out_update
						OutUpdateType * out_update = generate_one_update(in_update);

						// insert into shuffle buffer accordingly
						int index = get_global_buffer_index(out_update);
						global_buffer<OutUpdateType>* global_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
						global_buf->insert(out_update, index);

					}
				}

				free(update_local_buf);
				close(fd_update);
			}

			atomic_num_producers--;
		}

		// each writer thread generates a scatter_consumer
		void scatter_updates_consumer(global_buffer<OutUpdateType> ** buffers_for_shuffle, Update_Stream update_count) {
			unsigned int counter = 0;
			while(atomic_num_producers != 0) {
//				int i = (atomic_partition_id++) % context.num_partitions ;
				if(counter == context.num_partitions)
					counter = 0;
				unsigned int i = counter++;

				const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count)).c_str();
				global_buffer<OutUpdateType>* g_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;

				if(i >= 0){

					const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count)).c_str();
					global_buffer<OutUpdateType>* g_buf = buffer_manager<OutUpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
					g_buf->flush_end(file_name, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}


		int get_global_buffer_index(OutUpdateType* out_update) {
		//			return update_info->target / context.num_vertices_per_part;

			int partition_id = out_update->target / context.num_vertices_per_part;
			return partition_id < (context.num_partitions - 1) ? partition_id : (context.num_partitions - 1);
		}

	};
}



#endif /* CORE_SCATTER_UPDATES_HPP_ */
