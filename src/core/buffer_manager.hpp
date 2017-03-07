/*
 * buffer_manager.hpp
 *
 *  Created on: Mar 3, 2017
 *      Author: kai
 */

#ifndef CORE_BUFFER_MANAGER_HPP_
#define CORE_BUFFER_MANAGER_HPP_

namespace RStream {

	// global buffer for shuffling, accessing by multithreads
	template <typename T>
	class buffer {
		const size_t capacity;
		size_t count;
		T * buf;
		T * pos;
		std::mutex mutex;
		std::condition_variable cond_full;
		std::condition_variable cond_empty;

	public:
		buffer(const size_t _capacity) : capacity(_capacity), count(0), pos(nullptr) {
			buf = new T[capacity];
		}

		~buffer() {
			delete[] buf;
		}

		void insert(T* item) {
			std::unique_lock<std::mutex> lock(mutex);
			cond_full.wait(lock, [&] {return !is_full();});

			// insert item to buffer

			count++;
			lock.unlock();
			cond_empty.notify_one();
		}

		void flush(io_manager * io_mgr, int fd) {
			std::unique_lock<std::mutex> lock(mutex);
			cond_empty.wait(lock, [&]{return is_full(); });

			// flush buffer to update out stream
			char * output_buf = (char * ) buf;
			io_mgr->write_to_file(fd, output_buf, BUFFER_SIZE * sizeof(T));

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
		T** global_buffers;

	public:
		buffer_manager(int _num_partitions) : num_partitions(_num_partitions), global_buffers(nullptr) {}

		// global buffers for shuffling
		T** get_global_buffers() {
			if(global_buffers == nullptr) {
				global_buffers = new T* [num_partitions];
				for(int i = 0; i < num_partitions; i++) {
					global_buffers[i] = new buffer<T>(BUFFER_SIZE);
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
	};
}



#endif /* CORE_BUFFER_MANAGER_HPP_ */
