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
typedef unsigned char BYTE;

struct Edge {
	VertexId src;
	VertexId target;

	Edge(VertexId _src, VertexId _target) : src(_src), target(_target) {}
	Edge() : src(0), target(0) {}

	std::string toString(){
		return "(" + std::to_string(src) + ", " + std::to_string(target) + ")";
	}
};

struct LabeledEdge {
	VertexId src;
	VertexId target;
	BYTE src_label;
	BYTE target_label;
	BYTE edge_label;

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


/*
 *  Graph mining support. Join on all keys for each vertex tuple.
 *  Each element in the tuple contains 8 bytes, first 4 bytes is vertex id,
 *  second 4 bytes contains edge label(1byte) + vertex label(1byte) + history info(1byte).
 *  History info is used to record subgraph structure.
 *
 *
 *  [ ] [ ] [ ] [ ] || [ ] [ ] [ ] [ ]
 *    vertex id        idx  el  vl info
 *     4 bytes          1   1   1    1
 *
 * */
struct Element_In_Tuple {
	VertexId vertex_id;
	BYTE key_index;
	BYTE edge_label;
	BYTE vertex_label;
	BYTE history_info;

	Element_In_Tuple(VertexId _vertex_id, BYTE _edge_label, BYTE _vertex_label) :
		vertex_id(_vertex_id), key_index(0), edge_label(_edge_label), vertex_label(_vertex_label), history_info(0) {

	}

	Element_In_Tuple(VertexId _vertex_id, BYTE _edge_label, BYTE _vertex_label, BYTE _history) :
				vertex_id(_vertex_id), key_index(0), edge_label(_edge_label), vertex_label(_vertex_label), history_info(_history) {

	}

	Element_In_Tuple(VertexId _vertex_id, BYTE _key_index, BYTE _edge_label, BYTE _vertex_label, BYTE _history) :
		vertex_id(_vertex_id), key_index(_key_index), edge_label(_edge_label), vertex_label(_vertex_label), history_info(_history) {

	}

	void set_vertex_id(VertexId new_id){
		vertex_id = new_id;
	}
};

// One tuple contains multiple elements. "size" is the num of elements in one tuple
struct Update_Tuple {
	std::vector<Element_In_Tuple> elements;
};


#endif /* CORE_TYPE_HPP_ */
