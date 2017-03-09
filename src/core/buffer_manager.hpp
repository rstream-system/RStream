/*
 * buffer_manager.hpp
 *
 *  Created on: Mar 3, 2017
 *      Author: kai
 */

#ifndef CORE_BUFFER_MANAGER_HPP_
#define CORE_BUFFER_MANAGER_HPP_

#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>

#include "constants.hpp"

namespace RStream {

	// global buffer for shuffling, accessing by multithreads
	template <typename T>
	class buffer {
		const size_t capacity;
		std::vector<T> buf;
		std::mutex mutex;
		std::condition_variable cond_full;
		std::condition_variable cond_empty;

	public:
		buffer(const size_t _capacity) : capacity(_capacity) {}

		void insert(T* item) {
			std::unique_lock<std::mutex> lock(mutex);
			cond_full.wait(lock, [&] {return !is_full();});

			// insert item to buffer
			buf.push_back(item);
			lock.unlock();
			cond_empty.notify_one();
		}

		void flush(io_manager * io_mgr, int fd) {
			std::unique_lock<std::mutex> lock(mutex);
			cond_empty.wait(lock, [&]{return is_full(); });

			// flush buffer to update out stream
			reinterpret_cast<char*>(buf.data());
			io_mgr->write_to_file(fd, buf, BUFFER_CAPACITY * sizeof(T));
			buf.clear();
			lock.unlock();
			cond_full.notify_one();
		}

		bool is_full() {
			return buf.size() == capacity;
		}

		bool is_empty() {
			return buf.size() == 0;
		}
	};

	template <typename T>
	class buffer_manager {
		int num_partitions;
		T** global_buffers;

	public:
		buffer_manager(int _num_partitions) : num_partitions(_num_partitions), global_buffers(nullptr) {}

		// global buffers for shuffling
		T** get_global_buffers() {
			if(global_buffers == nullptr) {
				global_buffers = new T* [num_partitions];
				for(int i = 0; i < num_partitions; i++) {
					global_buffers[i] = new buffer<T>(BUFFER_CAPACITY);
				}
			}

			return global_buffers;
		}

		T* get_global_buffer(int index) {
			if(index >= 0 && index <num_partitions)
				return global_buffers[index];
			else
				return nullptr;
		}

		// thread local buffer
		void get_local_buffer(char * buf, size_t file_size) {
			buf = malloc(file_size);
		}
	};
}



#endif /* CORE_BUFFER_MANAGER_HPP_ */
