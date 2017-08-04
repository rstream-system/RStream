/*
 * constants.hpp
 *
 *  Created on: Mar 7, 2017
 *      Author: kai
 */

#ifndef CORE_CONSTANTS_HPP_
#define CORE_CONSTANTS_HPP_

size_t BUFFER_CAPACITY = 8000000;
long IO_SIZE = 24 * 1024 * 1024; // 24M
long CHUNK_SIZE = IO_SIZE * 3;
long PAGE_SIZE = 4 * 1024; // 4K
int MAX_QUEUE_SIZE = 65536;

#endif /* CORE_CONSTANTS_HPP_ */
