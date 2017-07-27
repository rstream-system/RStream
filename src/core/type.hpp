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
//typedef unsigned int BYTE;

enum class FORMAT {
	EdgeList,
	AdjList
};

enum class EdgeType {
		NO_WEIGHT = 0,
		WITH_WEIGHT = 1,
};

struct Edge {
	VertexId src;
	VertexId target;

	Edge(VertexId _src, VertexId _target) : src(_src), target(_target) {}
	Edge() : src(0), target(0) {}

	std::string toString(){
		return "(" + std::to_string(src) + ", " + std::to_string(target) + ")";
	}

	void swap(){
		if (src > target) {
			VertexId tmp = src;
			src = target;
			target = tmp;
		}
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

inline std::ostream & operator<<(std::ostream & strm, const LabeledEdge& edge){
	strm << "(" << edge.src << ", " << (int)edge.src_label << " - " << edge.target << ", " << (int)edge.target_label << ")";
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

	Element_In_Tuple(){

	}

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

	int cmp(const Element_In_Tuple& other) const {
		//compare vertex id
		if(vertex_id < other.vertex_id){
			return -1;
		}
		if(vertex_id > other.vertex_id){
			return 1;
		}

		//compare history info
		if(history_info < other.history_info){
			return -1;
		}
		if(history_info > other.history_info){
			return 1;
		}

		//compare vertex label
		if(vertex_label < other.vertex_label){
			return -1;
		}
		if(vertex_label > other.vertex_label){
			return 1;
		}

		//compare edge label
		if(edge_label < other.edge_label){
			return -1;
		}
		if(edge_label > other.edge_label){
			return 1;
		}

		//compare index
		if(key_index < other.key_index){
			return -1;
		}
		if(key_index > other.key_index){
			return 1;
		}

		return 0;
	}
};

//// One tuple contains multiple elements. "size" is the num of elements in one tuple
//struct Update_Tuple {
//	std::vector<Element_In_Tuple> elements;
//};


class Canonical_Graph {

public:
	Canonical_Graph(){

	}

	Canonical_Graph(std::vector<Element_In_Tuple>& tuple, unsigned int num_vertices, unsigned int hash_v)
		: tuple(tuple), number_of_vertices(num_vertices), hash_value(hash_v) {

	}

	Canonical_Graph(bliss::AbstractGraph* ag, bool is_directed){
		construct_cg(ag, is_directed);
	}

	~Canonical_Graph(){

	}


	int cmp(const Canonical_Graph& other_cg) const {
		//compare the numbers of vertices
		if(get_number_vertices() < other_cg.get_number_vertices()){
			return -1;
		}
		if(get_number_vertices() > other_cg.get_number_vertices()){
			return 1;
		}

		//compare hash value
		if(get_hash() < other_cg.get_hash()){
			return -1;
		}
		if(get_hash() > other_cg.get_hash()){
			return 1;
		}

		//compare edges
		assert(tuple.size() == other_cg.tuple.size());
		for(unsigned int i = 0; i < tuple.size(); ++i){
			const Element_In_Tuple & t1 = tuple[i];
			const Element_In_Tuple & t2 = other_cg.tuple[i];

			int cmp_element = t1.cmp(t2);
			if(cmp_element != 0){
				return cmp_element;
			}
		}

		return 0;
	}

	inline unsigned int get_hash() const {
		return hash_value;
	}

	inline unsigned int get_number_vertices() const {
		return number_of_vertices;
	}

	//operator for map
	bool operator==(const Canonical_Graph& other) const {
		return cmp(other) == 0;
	}

	inline std::vector<Element_In_Tuple> get_tuple() const {
		return tuple;
	}


private:
	std::vector<Element_In_Tuple> tuple;
	unsigned int number_of_vertices;
	unsigned int hash_value;


	void construct_cg(bliss::AbstractGraph* ag, bool is_directed){
		assert(!is_directed);
		if(!is_directed){
			number_of_vertices = ag->get_nof_vertices();
			hash_value = ag->get_hash();

			transform_to_tuple(ag);
		}
	}

	void transform_to_tuple(bliss::AbstractGraph* ag){
		bliss::Graph* graph = (bliss::Graph*) ag;
		std::unordered_set<VertexId> set;
		std::unordered_map<VertexId, BYTE> map;
		std::priority_queue<Edge, std::vector<Edge>, EdgeComparator> min_heap;

		std::vector<bliss::Graph::Vertex> vertices = graph->get_vertices_rstream();

		VertexId first_src = init_heapAndset(vertices, min_heap, set);
		assert(first_src != -1);
		push_first_element(first_src, map, vertices);

		while(!min_heap.empty()){
			Edge edge = min_heap.top();
			push_element(edge, map, vertices);

			min_heap.pop();
			add_neighbours(edge, min_heap, vertices, set);
		}
	}

	VertexId init_heapAndset(std::vector<bliss::Graph::Vertex>& vertices, std::priority_queue<Edge, std::vector<Edge>, EdgeComparator>& min_heap, std::unordered_set<VertexId>& set){
		for(unsigned int i = 0; i < vertices.size(); ++i){
			if(!vertices[i].edges.empty()){
				for(auto v: vertices[i].edges){
					min_heap.push(Edge(i + 1, v + 1));
				}
				set.insert(i + 1);
				return i + 1;
			}
		}

		return -1;
	}

	void push_first_element(VertexId first, std::unordered_map<VertexId, BYTE>& map, std::vector<bliss::Graph::Vertex>& vertices){
		map[first] = 0;
		tuple.push_back(Element_In_Tuple(first, (BYTE)0, (BYTE)vertices[first - 1].color));
	}

	void push_element(Edge& edge, std::unordered_map<VertexId, BYTE>& map, std::vector<bliss::Graph::Vertex>& vertices){
		assert(edge.src < edge.target);
		if(map.find(edge.src) != map.end()){
			tuple.push_back(Element_In_Tuple(edge.target, 0, vertices[edge.target - 1].color, map[edge.src]));
			if(map.find(edge.target) == map.end()){
				unsigned int s = tuple.size() + 1;
				map[edge.target] = s;
			}
		}
		else if(map.find(edge.target) != map.end()){
			tuple.push_back(Element_In_Tuple(edge.src, 0, vertices[edge.src - 1].color, map[edge.target]));
			if(map.find(edge.src) == map.end()){
				unsigned int s = tuple.size() + 1;
				map[edge.src] = s;
			}
		}
		else{
			//wrong case
	    	std::cout << "wrong case!!!" << std::endl;
	    	throw std::exception();
		}
	}

	void add_neighbours(Edge& edge, std::priority_queue<Edge, std::vector<Edge>, EdgeComparator>& min_heap, std::vector<bliss::Graph::Vertex>& vertices, std::unordered_set<VertexId>& set){
		add_neighbours(edge.src, min_heap, vertices, set);
		add_neighbours(edge.target, min_heap, vertices, set);
	}

	void add_neighbours(VertexId srcId, std::priority_queue<Edge, std::vector<Edge>, EdgeComparator>& min_heap, std::vector<bliss::Graph::Vertex>& vertices, std::unordered_set<VertexId>& set){
		if(set.find(srcId) == set.end()){
			for(auto v: vertices[srcId - 1].edges){
				VertexId target = v + 1;
				if(set.find(target) == set.end()){
					Edge edge(srcId, target);
					edge.swap();
					min_heap.push(edge);
				}
			}

		}
	}

};

namespace std {
	template<>
	struct hash<Canonical_Graph> {
		std::size_t operator()(const Canonical_Graph& cg) const {
			//simple hash
			return std::hash<int>()(cg.get_hash());
		}
	};
}


class Quick_Pattern {

public:
	Quick_Pattern(){

	}

//	Quick_Pattern(std::vector<Element_In_Tuple>& t){
//		tuple = t;
//	}

	~Quick_Pattern(){

	}

	//operator for map
	bool operator==(const Quick_Pattern& other) const {
		//compare edges
		assert(tuple.size() == other.tuple.size());
		for(unsigned int i = 0; i < tuple.size(); ++i){
			const Element_In_Tuple & t1 = tuple[i];
			const Element_In_Tuple & t2 = other.tuple[i];

			int cmp_element = t1.cmp(t2);
			if(cmp_element != 0){
				return false;
			}
		}

		return true;
	}

	unsigned int get_hash() const {
		//TODO
		return 0;
	}

	void push(Element_In_Tuple& element){
		tuple.push_back(element);
	}

	inline std::vector<Element_In_Tuple> get_tuple() const {
		return tuple;
	}

private:
	std::vector<Element_In_Tuple> tuple;

};

namespace std {
	template<>
	struct hash<Quick_Pattern> {
		std::size_t operator()(const Quick_Pattern& qp) const {
			//simple hash
			return std::hash<int>()(qp.get_hash());
		}
	};
}




#endif /* CORE_TYPE_HPP_ */
