/*
 * bliss_lib.hpp
 *
 *  Created on: Jul 3, 2017
 *      Author: icuzzq
 */

#ifndef SRC_CORE_PATTERN_HPP_
#define SRC_CORE_PATTERN_HPP_

#include "../common/RStreamCommon.hpp"
#include "type.hpp"

#include "defs.hh"
#include "graph.hh"
#include "timer.hh"
#include "utils.hh"

//static bool opt_directed = false;

class pattern {

	/**
	 * The hook function that prints the found automorphisms.
	 * \a param must be a file descriptor (FILE *).
	 */
	static void report_aut(void* param, const unsigned int n,
			const unsigned int* aut) {
		assert(param);
		fprintf((FILE*) param, "Generator: ");
		bliss::print_permutation((FILE*) param, n, aut, 1);
		fprintf((FILE*) param, "\n");
	}

public:

	static bliss::AbstractGraph* turn_canonical_graph(std::vector<Element_In_Tuple> & sub_graph, const bool is_directed) {
		bliss::AbstractGraph* ag = 0;

		//read graph from tuple
		ag = readGraph(sub_graph, is_directed);

		//turn to canonical form
		bliss::AbstractGraph* cf = turnCanonical(ag);

		delete ag;
		return cf;
	}


	static bool is_automorphism(std::vector<Element_In_Tuple> & sub_graph) {
		return false;
	}


	static void turn_quick_pattern_sideffect(std::vector<Element_In_Tuple> & sub_graph) {
		std::unordered_map<VertexId, VertexId> map;
		VertexId new_id = 1;

		for(unsigned int i = 0; i < sub_graph.size(); i++) {
			VertexId old_id = sub_graph.at(i).vertex_id;

			auto iterator = map.find(old_id);
			if(iterator == map.end()) {
				sub_graph.at(i).set_vertex_id(new_id);
				map[old_id] = new_id++;

			} else {
				sub_graph.at(i).set_vertex_id(iterator->second);
			}
		}

	}


	static void turn_quick_pattern_pure(std::vector<Element_In_Tuple> & sub_graph, std::vector<Element_In_Tuple> & graph_quick_pattern) {
			std::unordered_map<VertexId, VertexId> map;
			VertexId new_id = 1;

			for(unsigned int i = 0; i < sub_graph.size(); i++) {
				Element_In_Tuple old_tuple = sub_graph[i];
				Element_In_Tuple * new_tuple = new Element_In_Tuple(old_tuple.vertex_id, old_tuple.key_index, old_tuple.edge_label, old_tuple.vertex_label, old_tuple.history_info);

				VertexId old_id = new_tuple->vertex_id;
				auto iterator = map.find(old_id);
				if(iterator == map.end()) {
					new_tuple->set_vertex_id(new_id);
					map[old_id] = new_id++;

				} else {
					new_tuple->set_vertex_id(iterator->second);
				}

				graph_quick_pattern.push_back(*new_tuple);
			}

		}


private:

	static bliss::AbstractGraph* readGraph(std::vector<Element_In_Tuple> & sub_graph, bool opt_directed){
		bliss::AbstractGraph* g = 0;

		//get the number of vertices
		std::unordered_map<VertexId, BYTE> vertices;
		for(unsigned int index = 0; index < sub_graph.size(); ++index){
			Element_In_Tuple tuple = sub_graph[index];
			vertices[tuple.vertex_id] = tuple.vertex_label;
		}

		//construct graph
		const unsigned int number_vertices = vertices.size();
		if(opt_directed){
			g = new bliss::Digraph(vertices.size());
		}
		else{
			g = new bliss::Graph(vertices.size());
		}

		//set vertices
		for(unsigned int i = 0; i < number_vertices; ++i){
			g->change_color(i, vertices[i + 1]);
		}

		//read edges
		assert(sub_graph.size() > 1);
		for(unsigned int index = 1; index < sub_graph.size(); ++index){
			Element_In_Tuple tuple = sub_graph[index];
			VertexId from = sub_graph[tuple.history_info].vertex_id;
			VertexId to = tuple.vertex_id;
			g->add_edge(from - 1, to - 1);
		}

		return g;
	}

	static bliss::AbstractGraph* turnCanonical(bliss::AbstractGraph* ag){
		//canonical labeling
		bliss::Stats stats;
		const unsigned int* cl = ag->canonical_form(stats, &report_aut, stdout);

		//permute to canonical form
		bliss::AbstractGraph* cf = ag->permute(cl);

		delete[] cl;
		return cf;
	}

//	static std::vector<Element_In_Tuple> writeTuple(bliss::AbstractGraph* cf){
//		std::vector<Element_In_Tuple> out;
//		//TODO
//
//		return out;
//	}

	static void writeOutGraph(bliss::AbstractGraph* graph, std::string& outfile){
		FILE* const fp = fopen(outfile.c_str(), "w");
		if (!fp)
			printf("Cannot open '%s' for outputting the canonical form, aborting", outfile.c_str());
		graph->write_dimacs(fp);
		fclose(fp);
	}

};

#endif /* SRC_CORE_PATTERN_HPP_ */
