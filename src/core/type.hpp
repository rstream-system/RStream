/*
 * type.hpp
 *
 *  Created on: Mar 6, 2017
 *      Author: kai
 */

#ifndef CORE_TYPE_HPP_
#define CORE_TYPE_HPP_

typedef int VertexId;
typedef float Weight;

struct Edge {
	VertexId src;
	VertexId target;
	Weight weight;


};

struct T {
	VertexId target;

};

#endif /* CORE_TYPE_HPP_ */
