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
	public:
//		// get index when partition on vertices
		static int get_index(VertexId id, const Engine & context) {
			int partition_id = id / context.num_vertices_per_part;
			return partition_id < (context.num_partitions - 1) ? partition_id : (context.num_partitions - 1);
		}

		// get index when partition on edges
//		static int get_index(VertexId id, const Engine & context) {
////			for(unsigned int i = 0; i < context.vertex_intervals.size(); i++) {
////				std::pair<VertexId, VertexId> interval = context.vertex_intervals.at(i);
////				if(id >= interval.first && id <= interval.second)
////					return i;
////			}
//
//
//			int left = 0, right = context.vertex_intervals.size() - 1;
//			while(left <= right) {
//				int mid = (left + right) / 2;
//				if(id >= context.vertex_intervals.at(mid).first && id <= context.vertex_intervals.at(mid).second)
//					return mid;
//				else if(id > context.vertex_intervals.at(mid).second)
//					left = mid + 1;
//				else
//					right = mid - 1;
//			}
//
//			return -1;
//		}
	};
}



#endif /* CORE_META_INFO_HPP_ */
