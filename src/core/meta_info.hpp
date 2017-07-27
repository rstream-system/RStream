/*
 * meta_info.hpp
 *
 *  Created on: Jul 26, 2017
 *      Author: kai
 */

#ifndef CORE_META_INFO_HPP_
#define CORE_META_INFO_HPP_

namespace RStream {
	class meta_info{

		// each partition has almost same number of vertices
		static int get_index(VertexId id, const Engine & context) {
			int partition_id = id / context.num_vertices_per_part;
			return partition_id < (context.num_partitions - 1) ? partition_id : (context.num_partitions - 1);
		}
	};
}



#endif /* CORE_META_INFO_HPP_ */
