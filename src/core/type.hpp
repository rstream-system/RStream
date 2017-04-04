/*
 * type.hpp
 *
 *  Created on: Mar 6, 2017
 *      Author: kai
 */

#ifndef CORE_TYPE_HPP_
#define CORE_TYPE_HPP_

#include "../common/RStreamCommon.hpp"

typedef unsigned Update_Stream;
typedef int VertexId;
typedef float Weight;

struct Edge {
	VertexId src;
	VertexId target;

	Edge(VertexId _src, VertexId _target) : src(_src), target(_target) {}
	Edge() : src(0), target(0) {}

	std::string toString(){
		return "(" + std::to_string(src) + ", " + std::to_string(target) + ")";
	}
};

struct WeightedEdge {
	VertexId src;
	VertexId target;
	Weight weight;

	WeightedEdge(VertexId _src, VertexId _target, Weight _weight) : src(_src), target(_target), weight(_weight) {}
	WeightedEdge() : src(0), target(0), weight(0.0f) {}

	std::string toString(){
		return "(" + std::to_string(src) + ", " + std::to_string(target) + std::to_string(weight) + ")";
	}
};

//struct Vertex_Interval {
//	VertexId start;
//	VertexId end;
//};

inline std::ostream & operator<<(std::ostream & strm, const WeightedEdge& edge){
	strm << "(" << edge.src << ", " << edge.target << ", " << edge.weight << ")";
	return strm;
}

inline std::ostream & operator<<(std::ostream & strm, const Edge& edge){
	strm << "(" << edge.src << ", " << edge.target  << ")";
	return strm;
}


struct BaseUpdate {
	VertexId target;

	BaseUpdate(){};
	BaseUpdate(VertexId _target) : target(_target) {};

	std::string toString(){
		return std::to_string(target);
	}

};

struct BaseVertex {
//	VertexId id;
};

//struct Update_Stream {
//	unsigned update_filename;
//};

inline std::ostream & operator<<(std::ostream & strm, const BaseUpdate& up){
	strm << "(" << up.target << ")";
	return strm;
}


#endif /* CORE_TYPE_HPP_ */
