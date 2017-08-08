/*
 * constants.hpp
 *
 *  Created on: Mar 7, 2017
 *      Author: kai
 */

#ifndef CORE_CONSTANTS_HPP_
#define CORE_CONSTANTS_HPP_

namespace RStream{

const size_t BUFFER_CAPACITY = 4000000;
const long IO_SIZE = 24 * 1024 * 1024; // 24M
const long CHUNK_SIZE = IO_SIZE * 2;
const long PAGE_SIZE = 4 * 1024; // 4K
const int MAX_QUEUE_SIZE = 65536;

}
#endif /* CORE_CONSTANTS_HPP_ */
