/*
 * scatter.hpp
 *
 *  Created on: Aug 4, 2017
 *      Author: kai
 */

#ifndef CORE_SCATTER_HPP_
#define CORE_SCATTER_HPP_

#include "engine.hpp"
#include "meta_info.hpp"

namespace RStream {
	template <typename VertexDataType, typename UpdateType>
	class Scatter {

	static_assert(
				std::is_base_of<BaseVertex, VertexDataType>::value,
				"VertexDataType must be a subclass of BaseVertex."
			);

			static_assert(
				std::is_base_of<BaseUpdate, UpdateType>::value,
				"UpdateType must be a subclass of BaseUpdate."
			);

	public:
		Scatter(Engine & e);
		virtual ~Scatter();

		/* scatter with vertex data (for graph computation use)*/
		Update_Stream scatter_with_vertex(std::function<UpdateType*(Edge*, VertexDataType*)> generate_one_update);

		/* scatter without vertex data (for relational algebra use)*/
		Update_Stream scatter_no_vertex(std::function<UpdateType*(Edge*)> generate_one_update);

	private:
		void atomic_init();

		void load_vertices_hashMap(char* vertex_local_buf, const int vertex_file_size, std::unordered_map<VertexId, VertexDataType*> & vertex_map);

		void scatter_producer_with_vertex(std::function<UpdateType*(Edge*, VertexDataType*)> generate_one_update,
						global_buffer<UpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue);

		void scatter_producer_no_vertex(std::function<UpdateType*(Edge*)> generate_one_update,
						global_buffer<UpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue);

		void scatter_consumer(global_buffer<UpdateType> ** buffers_for_shuffle, Update_Stream update_count);

		const Engine& context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_number;
	};
}



#endif /* CORE_SCATTER_HPP_ */
