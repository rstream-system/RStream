/*
 * canonical_graph.hpp
 *
 *  Created on: Aug 4, 2017
 *      Author: icuzzq
 */

#ifndef SRC_CORE_CANONICAL_GRAPH_HPP_
#define SRC_CORE_CANONICAL_GRAPH_HPP_

#include "type.hpp"



namespace RStream {
class Canonical_Graph {

	friend std::ostream & operator<<(std::ostream & strm, const Canonical_Graph& cg);

public:
	Canonical_Graph();

//	Canonical_Graph(std::vector<Element_In_Tuple>& tuple, unsigned int num_vertices, unsigned int hash_v)
//		: tuple(tuple), number_of_vertices(num_vertices), hash_value(hash_v) {
//
//	}

	Canonical_Graph(bliss::AbstractGraph* ag, bool is_directed);

	~Canonical_Graph();


	int cmp(const Canonical_Graph& other_cg) const;

	inline unsigned int get_hash() const {
		return hash_value;
	}

	inline unsigned int get_number_vertices() const {
		return number_of_vertices;
	}

	//operator for map
	inline bool operator==(const Canonical_Graph& other) const {
		return cmp(other) == 0;
	}

	inline std::vector<Element_In_Tuple>& get_tuple() {
		return tuple;
	}

	inline std::vector<Element_In_Tuple> get_tuple_const() const {
		return tuple;
	}

	inline void set_number_vertices(unsigned int num_vertices) {
		number_of_vertices = num_vertices;
	}

	inline void set_hash_value(unsigned int hash) {
		hash_value = hash;
	}


private:
	std::vector<Element_In_Tuple> tuple;
	unsigned int number_of_vertices;
	unsigned int hash_value;


	void construct_cg(bliss::AbstractGraph* ag, bool is_directed);

	void transform_to_tuple(bliss::AbstractGraph* ag);


	VertexId init_heapAndset(std::vector<bliss::Graph::Vertex>& vertices, std::priority_queue<Edge, std::vector<Edge>, EdgeComparator>& min_heap, std::unordered_set<VertexId>& set);

	void push_first_element(VertexId first, std::unordered_map<VertexId, BYTE>& map, std::vector<bliss::Graph::Vertex>& vertices);

	void push_element(Edge& edge, std::unordered_map<VertexId, BYTE>& map, std::vector<bliss::Graph::Vertex>& vertices);

	void add_neighbours(Edge& edge, std::priority_queue<Edge, std::vector<Edge>, EdgeComparator>& min_heap, std::vector<bliss::Graph::Vertex>& vertices, std::unordered_set<VertexId>& set);

	void add_neighbours(VertexId srcId, std::priority_queue<Edge, std::vector<Edge>, EdgeComparator>& min_heap, std::vector<bliss::Graph::Vertex>& vertices, std::unordered_set<VertexId>& set);

};
}

namespace std {
	template<>
	struct hash<RStream::Canonical_Graph> {
		std::size_t operator()(const RStream::Canonical_Graph& cg) const {
			//simple hash
			return std::hash<int>()(cg.get_hash());
		}
	};
}

#endif /* SRC_CORE_CANONICAL_GRAPH_HPP_ */
