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
	class global_buffer {
	    size_t capacity;
		T * buf;
		size_t count;
		std::mutex mutex;
		std::condition_variable cond_full;
		std::condition_variable cond_empty;

	public:
		global_buffer(size_t _capacity) : capacity{_capacity}, count(0) {
			buf = new T [capacity];
		}

		~global_buffer() {
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

		void flush(int fd) {
			std::unique_lock<std::mutex> lock(mutex);
			cond_empty.wait(lock, [&]{return is_full(); });

			// flush buffer to update out stream
			char * output_buf = (char * ) buf;
			io_manager::write_to_file(fd, output_buf, BUFFER_CAPACITY * sizeof(T));
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

	public:

		// global buffers for shuffling
		static global_buffer<T> **  get_global_buffers(int num_partitions) {
			global_buffer<T> ** buffers = new global_buffer<T> * [num_partitions];

			for(int i = 0; i < num_partitions; i++) {
				buffers[i] = new global_buffer<T>(BUFFER_CAPACITY);
			}

			return buffers;
		}

		// get one global buffer according to the index
		static global_buffer<T>* get_global_buffer(global_buffer<T> ** buffers, int num_partitions, int index) {
			if(index >= 0 && index <num_partitions)
				return buffers[index];
			else
				return nullptr;
		}

	};
}



#endif /* CORE_BUFFER_MANAGER_HPP_ */
