/*
 * concurrent_set.hpp
 *
 *  Created on: Aug 10, 2017
 *      Author: Zhiqiang
 */


#include "../common/RStreamCommon.hpp"

namespace RStream {
	template <typename T>
	class concurrent_set {
		std::mutex mutex;

	public:
		std::unordered_set<T> set;

		concurrent_set() {}

		concurrent_set(const size_t _capacity) {}

		~concurrent_set() {}


		void insert_atomic(const T & item) {
			std::unique_lock<std::mutex> lock(mutex);
			set.insert(item);
		}

		bool contains(T& item){
			return set.find(item) != set.end();
		}

	};
}


