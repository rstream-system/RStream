/*
 * buffer_manager.hpp
 *
 *  Created on: Mar 3, 2017
 *      Author: kai
 */

#ifndef CORE_BUFFER_MANAGER_HPP_
#define CORE_BUFFER_MANAGER_HPP_

#include "../common/RStreamCommon.hpp"

#include "constants.hpp"

namespace RStream {

	// global buffer for shuffling in graph mining.
	//The data structure of tuple is fixed(variable sized array) for every join all keys.
	// that's why we don't use template
	// TODO: buffer_manager needs to be rewritten later.

	class global_buffer_for_mining {
		size_t capacity;
		size_t count;
		size_t sizeof_tuple;
		size_t index;
		char * buf;
		std::mutex mutex;
		std::condition_variable not_full;

	public:
		global_buffer_for_mining(size_t _capacity, size_t _sizeof_tuple) :
			capacity{_capacity}, count(0), sizeof_tuple(_sizeof_tuple), index(0) {
			buf = new char[sizeof_tuple * capacity];
		}

		~global_buffer_for_mining() {
			delete[] buf;
		}

		void insert(char * tuple) {
			std::unique_lock<std::mutex> lock(mutex);
			not_full.wait(lock, [&] {return !is_full();});

			// insert tuple to buffer
			std::memcpy(buf + index, tuple, sizeof_tuple);
			index += sizeof_tuple;
			count++;
		}

		void flush(const char * file_name, const int i) {
			std::unique_lock<std::mutex> lock(mutex);

			if(is_full()){
				int perms = O_WRONLY | O_APPEND;
				int fd = open(file_name, perms, S_IRWXU);
				if(fd < 0){
					fd = creat(file_name, S_IRWXU);
				}
				// flush buffer to update out stream
				io_manager::write_to_file(fd, buf, capacity * sizeof_tuple);
				close(fd);

				count = 0;
				not_full.notify_all();
			}
		}

		void flush_end(const char * file_name, const int i) {
			std::unique_lock<std::mutex> lock(mutex);
//			if(!is_empty()){
				int perms = O_WRONLY | O_APPEND;
				int fd = open(file_name, perms, S_IRWXU);
				if(fd < 0){
					fd = creat(file_name, S_IRWXU);
				}

				// flush buffer to update out stream
				io_manager::write_to_file(fd, buf, count * sizeof_tuple);
				close(fd);
//			}

		}

		bool is_full() {
			return count == capacity;
		}

		bool is_empty() {
			return count == 0;
		}

	};

	// global buffer for shuffling, accessing by multithreads
	template <typename T>
	class global_buffer {
	    size_t capacity;
		T * buf;
		size_t count;
		std::mutex mutex;
		std::condition_variable not_full;

	public:
		global_buffer(size_t _capacity) : capacity{_capacity}, count(0) {
			buf = new T [capacity];
		}

		~global_buffer() {
			delete[] buf;
		 }

		void insert(T* item, const int index) {
			std::unique_lock<std::mutex> lock(mutex);
			not_full.wait(lock, [&] {return !is_full();});

			// insert item to buffer
			buf[count++] = *item;

//			debugging info
//			print_thread_info_locked("inserting an item: " + item->toString() + " to buffer[" + std::to_string(index) + "]\n");
		}

		void flush(const char * file_name, const int i) {
			std::unique_lock<std::mutex> lock(mutex);

			if(is_full()){
				int perms = O_WRONLY | O_APPEND;
				int fd = open(file_name, perms, S_IRWXU);
				if(fd < 0){
					fd = creat(file_name, S_IRWXU);
				}
				// flush buffer to update out stream
				char * b = (char *) buf;
				io_manager::write_to_file(fd, b, capacity * sizeof(T));
				close(fd);

				count = 0;
				not_full.notify_all();

//				print_thread_info_locked("flushed buffer[" + std::to_string(i) + "] to file " + std::string(file_name) + "\n");
			}

			//debugging info
//			if(is_full()){
//				print_thread_info_locked("flushed buffer[" + std::to_string(i) + "] to file " + std::string(file_name) + "\n");
//			}
//			else{
//				print_thread_info_locked("trying to flush buffer[" + std::to_string(i) + "] to file " + std::string(file_name) + "\n");
//			}
		}

		void flush_end(const char * file_name, const int i) {
			std::unique_lock<std::mutex> lock(mutex);
//			if(!is_empty()){
				int perms = O_WRONLY | O_APPEND;
				int fd = open(file_name, perms, S_IRWXU);
				if(fd < 0){
					fd = creat(file_name, S_IRWXU);
				}

				// flush buffer to update out stream
				char * b = (char *) buf;
				io_manager::write_to_file(fd, b, count * sizeof(T));
				close(fd);
//			}

			//debugging info
			if(!is_empty()){
				print_thread_info_locked("flushed end buffer[" + std::to_string(i) + "] to file " + std::string(file_name) + "\n");
			}
//			else{
//				print_thread_info_locked("trying to flush buffer[" + std::to_string(i) + "] to file " + std::string(file_name) + "\n");
//			}
		}

		bool is_full() {
			return count == capacity;
		}

		bool is_empty() {
			return count == 0;
		}

		size_t get_capacity() {
			return capacity;
		}
	};

	class buffer_manager_for_mining {
	public:
		static global_buffer_for_mining ** get_global_buffers_for_mining(int num_partitions, int sizeof_tuple) {
			global_buffer_for_mining ** buffers = new global_buffer_for_mining * [num_partitions];

			for(int i = 0; i < num_partitions; i++) {
				buffers[i] = new global_buffer_for_mining(BUFFER_CAPACITY, sizeof_tuple);
			}

			return buffers;
		}

		static global_buffer_for_mining * get_global_buffer_for_mining(global_buffer_for_mining ** buffers, int num_partitions, int index) {
			assert(index >= 0 && index < num_partitions);
//			if(index >= 0 && index < num_partitions)
			return buffers[index];
//			else
//				return nullptr;
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
			assert(index >= 0 && index < num_partitions);
//			if(index >= 0 && index < num_partitions)
			return buffers[index];
//			else
//				return nullptr;
		}

	};
}



#endif /* CORE_BUFFER_MANAGER_HPP_ */
