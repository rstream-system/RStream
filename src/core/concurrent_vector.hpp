/*
 * concurrent_vector.hpp
 *
 *  Created on: Aug 10, 2017
 *      Author: Zhiqiang
 */


#include "../common/RStreamCommon.hpp"

namespace RStream {
	template <typename T>
	class concurrent_vector {
		std::mutex mutex;

	public:
		std::vector<T> vector;


		concurrent_vector() {}

		concurrent_vector(const size_t _capacity) {
			vector.reserve(_capacity);
		}

		~concurrent_vector() {}


		void insert_atomic(const T & item) {
			std::unique_lock<std::mutex> lock(mutex);
			vector.push_back(item);
		}


//		bool contains(T& item){
//			return set.find(item) != set.end();
//		}

	};
}

