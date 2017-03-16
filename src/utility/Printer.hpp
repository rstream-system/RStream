/*
 * Print.hpp
 *
 *  Created on: Mar 14, 2017
 *      Author: icuzzq
 */

#include "../common/RStreamCommon.hpp"

#ifndef SRC_PRINT_HPP_
#define SRC_PRINT_HPP_

std::mutex lock_log;

void print_thread_info_locked(const std::string & info){
	std::unique_lock<std::mutex> lock(lock_log);
	pid_t x = syscall(__NR_gettid);
	std::cout << "thread " << x << ": " << info;
}

//void print_thread_info(const std::string & info){
//	pid_t x = syscall(__NR_gettid);
//	std::cout << "thread " << x << ": " << info;
//}



#endif /* SRC_PRINT_HPP_ */
