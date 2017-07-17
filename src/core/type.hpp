/*
 * type.hpp
 *
 *  Created on: Mar 6, 2017
 *      Author: kai
 */

#ifndef CORE_TYPE_HPP_
#define CORE_TYPE_HPP_

#include "../common/RStreamCommon.hpp"

#include "defs.hh"
#include "graph.hh"
#include "timer.hh"
#include "utils.hh"

typedef unsigned Update_Stream;
typedef unsigned Aggregation_Stream;
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

class EdgeComparator{
public:
	int operator()(const Edge& oneEdge, const Edge& otherEdge){
		if(oneEdge.src == otherEdge.src){
			return oneEdge.target > otherEdge.target;
		}
		else{
			return oneEdge.src > otherEdge.src;
		}
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


struct Canonical_Graph {

public:
	Canonical_Graph(bliss::AbstractGraph* ag, bool is_directed){
		construct_cg(ag, is_directed);
	}

	~Canonical_Graph(){

	}


	int cmp(Canonical_Graph& other_cg){

	}

	unsigned int get_hash(){

	}



private:
	std::vector<Element_In_Tuple> tuple;

	void construct_cg(bliss::AbstractGraph* ag, bool is_directed){
		assert(!is_directed);
		if(!is_directed){
			bliss::Graph* graph = (bliss::Graph*) ag;
			graph->sort_edges_rstream();

			std::unordered_map<VertexId, BYTE> map;
			std::priority_queue<Edge, std::vector<Edge>, EdgeComparator> min_heap;

			std::vector<bliss::Graph::Vertex> vertices = graph->get_vertices_rstream();

			Edge* first_edge = getFirstEdge(vertices);
			min_heap.push(*first_edge);
			Element_In_Tuple* element = generate_first_element(*first_edge, map, vertices);
			tuple.push_back(*element);
			delete first_edge;
			delete element;

			while(!min_heap.empty()){
				Edge edge = min_heap.top();
				Element_In_Tuple* element = generate_element(edge, map, vertices);
				tuple.push_back(*element);
				delete element;

				min_heap.pop();
				add_neighbours(edge, min_heap, vertices);
			}

		}
	}

	Edge* getFirstEdge(std::vector<bliss::Graph::Vertex>& vertices){
		for(unsigned int i = 0; i < vertices.size(); ++i){
			if(!vertices[i].edges.empty()){
				Edge* edge = new Edge(i+1, vertices[i].edges[0] + 1);
				assert(edge->src < edge->target);
				return edge;
			}
		}

		return nullptr;
	}

	Element_In_Tuple* generate_first_element(Edge& edge, std::unordered_map<VertexId, BYTE>& map, std::vector<bliss::Graph::Vertex>& vertices){
		map[edge.src] = 0;
		Element_In_Tuple* element = new Element_In_Tuple(edge.src, (BYTE)0, (BYTE)vertices[edge.src - 1].color);
		return element;
	}

	Element_In_Tuple* generate_element(Edge& edge, std::unordered_map<VertexId, BYTE>& map, std::vector<bliss::Graph::Vertex>& vertices){
		assert(edge.src < edge.target);
		Element_In_Tuple* element;
		if(map.find(edge.src) != map.end()){
			element = new Element_In_Tuple(edge.target, 0, vertices[edge.target - 1].color, map[edge.src]);
			if(map.find(edge.target) == map.end()){
				unsigned int s = tuple.size() + 1;
				map[edge.target] = s;
			}
		}
		else if(map.find(edge.target) != map.end()){
			element = new Element_In_Tuple(edge.src, 0, vertices[edge.src - 1].color, map[edge.target]);
			if(map.find(edge.src) == map.end()){
				unsigned int s = tuple.size() + 1;
				map[edge.src] = s;
			}
		}
		else{
			//wrong case

		}
		return element;
	}

	void add_neighbours(Edge& edge, std::priority_queue<Edge, std::vector<Edge>, EdgeComparator>& min_heap, std::vector<bliss::Graph::Vertex>& vertices){

	}


};




#endif /* CORE_TYPE_HPP_ */
