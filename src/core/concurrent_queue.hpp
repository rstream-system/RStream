/*
 * concurrent_queue.hpp
 *
 *  Created on: Mar 6, 2017
 *      Author: kai
 */

#ifndef CORE_CONCURRENT_QUEUE_HPP_
#define CORE_CONCURRENT_QUEUE_HPP_

#include "../common/RStreamCommon.hpp"

namespace RStream {
	template <typename T>
	class concurrent_queue {
		std::queue<T> queue;
		std::mutex mutex;

	public:
		concurrent_queue(const size_t _capacity) {}

//		inline bool isEmpty(){
//			return queue.empty();
//		}

		void push(const T & item) {
			queue.push(item);
		}

//		T pop() {
//			std::unique_lock<std::mutex> lock(mutex);
//			auto item = queue.front();
//			queue.pop();
//			return item;
//		}

		bool test_pop_atomic(T& item){
			std::unique_lock<std::mutex> lock(mutex);
			if(queue.empty()){
				return false;
			}
			else{
				item = queue.front();
				queue.pop();
				return true;
			}
		}

		int size() { return queue.size();}
	};
}



#endif /* CORE_CONCURRENT_QUEUE_HPP_ */
