/*
 * type.hpp
 *
 *  Created on: Mar 6, 2017
 *      Author: kai
 */

#ifndef CORE_TYPE_HPP_
#define CORE_TYPE_HPP_

#include "../common/RStreamCommon.hpp"

typedef int VertexId;
typedef float Weight;

struct Edge {
	VertexId src;
	VertexId target;
	Weight weight;

};

inline std::ostream & operator<<(std::ostream & strm, const Edge& edge){
	strm << "(" << edge.src << ", " << edge.target << ", " << edge.weight << ")";
	return strm;
}


struct T {
	VertexId target;

};

#endif /* CORE_TYPE_HPP_ */
