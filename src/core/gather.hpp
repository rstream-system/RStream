/*
 * gather.hpp
 *
 *  Created on: Aug 4, 2017
 *      Author: kai
 */

#ifndef CORE_GATHER_HPP_
#define CORE_GATHER_HPP_

#include "engine.hpp"

namespace RStream {
	template <typename VertexDataType, typename UpdateType>
	class Gather {

	static_assert(
				std::is_base_of<BaseVertex, VertexDataType>::value,
				"VertexDataType must be a subclass of BaseVertex."
			);

			static_assert(
				std::is_base_of<BaseUpdate, UpdateType>::value,
				"UpdateType must be a subclass of BaseUpdate."
			);
	public:
		Gather(Engine & e);
		virtual ~Gather();

		void gather(Update_Stream in_update_stream, std::function<void(UpdateType*, VertexDataType*)> apply_one_update);

	private:
		void load_vertices_hashMap(char* vertex_local_buf, const int vertex_file_size, std::unordered_map<VertexId, VertexDataType*> & vertex_map);

		void gather_producer(Update_Stream in_update_stream, std::function<void(UpdateType*, VertexDataType*)> apply_one_update,
								concurrent_queue<int> * task_queue);

		const Engine& context;
	};
}



#endif /* CORE_GATHER_HPP_ */
