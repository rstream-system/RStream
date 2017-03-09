/*
 * buffer_manager.hpp
 *
 *  Created on: Mar 3, 2017
 *      Author: kai
 */

#ifndef CORE_BUFFER_MANAGER_HPP_
#define CORE_BUFFER_MANAGER_HPP_

#include <queue>
#include <mutex>
#include <condition_variable>

#include "constants.hpp"

namespace RStream {

	// global buffer for shuffling, accessing by multithreads
	template <typename T>
	class buffer {
	    size_t capacity;
		T * buf;
		size_t count;
		std::mutex mutex;
		std::condition_variable cond_full;
		std::condition_variable cond_empty;

	public:
		buffer(size_t _capacity) : capacity{_capacity}, count(0) {
			buf = new T [capacity];
		}

		~buffer() {
			delete[] buf;
		 }

		void insert(T* item) {
			std::unique_lock<std::mutex> lock(mutex);
			cond_full.wait(lock, [&] {return !is_full();});

			// insert item to buffer
			buf[count++] = *item;

			lock.unlock();
			cond_empty.notify_one();
		}

		void flush(io_manager * io_mgr, int fd) {
			std::unique_lock<std::mutex> lock(mutex);
			cond_empty.wait(lock, [&]{return is_full(); });

			// flush buffer to update out stream
			char * output_buf = (char * ) buf;
			io_mgr->write_to_file(fd, output_buf, BUFFER_CAPACITY * sizeof(T));
			count = 0;
			lock.unlock();
			cond_full.notify_one();
		}

		bool is_full() {
			return count == capacity;
		}

		bool is_empty() {
			return count == 0;
		}
	};

	template <typename T>
	class buffer_manager {
		int num_partitions;
		buffer<T> ** global_buffers;

	public:
		buffer_manager(int _num_partitions) : num_partitions(_num_partitions), global_buffers(nullptr) {}

		// global buffers for shuffling
		buffer<T> ** get_global_buffers() {
			if(global_buffers == nullptr) {
				for(int i = 0; i < num_partitions; i++) {
					global_buffers[i] = new buffer<T>(BUFFER_CAPACITY);
				}
			}

			return global_buffers;
		}

		buffer<T>* get_global_buffer(int index) {
			if(index >= 0 && index <num_partitions)
				return global_buffers[index];
			else
				return nullptr;
		}

		// thread local buffer
		void get_local_buffer(char * buf, size_t file_size) {
			buf = (char *)malloc(file_size);
		}
	};
}



#endif /* CORE_BUFFER_MANAGER_HPP_ */
