/*
 * io_manager.hpp
 *
 *  Created on: Mar 3, 2017
 *      Author: kai
 */

#ifndef CORE_IO_MANAGER_HPP_
#define CORE_IO_MANAGER_HPP_

#include <iostream>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>

namespace RStream {
	class io_manager {
	public:
		static size_t get_filesize(int fd) {
			if(fd < 0) {
				std::cout << "Could not open file " << fd << std::endl;
				assert(false);
			}

			off_t size = lseek(fd, 0, SEEK_END);
			close(fd);
			return size;
		}

		// how to read efficiently?
		void read_from_file(int fd, char * buf, size_t fsize) {
			size_t n_read = 0;
			while(n_read < fsize) {
				ssize_t n_bytes = pread(fd, buf, fsize - n_read, n_read);
				if(n_bytes == size_t(-1)) {
					std::cout << "Read error ! " << std::endl;
					assert(false);
				}
				assert(n_bytes > 0);
				buf += n_bytes;
				n_read += n_bytes;
			}

			assert(n_read <= fsize);
		}

		// how to write efficiently
		void write_to_file(int fd, char * buf, size_t fsize) {
			size_t n_write = 0;
			assert(fd > 0);

			while(n_write < fsize) {
				size_t n_bytes = pwrite(fd, buf, fsize - n_write, n_write);
				if(n_bytes == size_t(-1)) {
					std::cout << "Write error! " << std::endl;
					assert(false);
				}
				assert(n_bytes > 0);
				buf += n_bytes;
				n_write += n_bytes;
			}
		}
	};
}



#endif /* CORE_IO_MANAGER_HPP_ */
