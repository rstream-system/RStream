/*
 * bliss_lib.hpp
 *
 *  Created on: Jul 3, 2017
 *      Author: icuzzq
 */

#ifndef SRC_CORE_BLISS_LIB_HPP_
#define SRC_CORE_BLISS_LIB_HPP_

#include "../common/RStreamCommon.hpp"
#include "type.hpp"

#include "defs.hh"
#include "graph.hh"
#include "timer.hh"
#include "utils.hh"

static bool opt_directed = false;

class Bliss_Lib {

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

	static std::vector<Element_In_Tuple> turn_canonical_graph(std::vector<Element_In_Tuple> & sub_graph, std::string & outfile) {
		bliss::AbstractGraph* aGraph = 0;

		//read graph from tuple
		aGraph = readGraph(sub_graph, opt_directed);
		writeOut(aGraph, outfile);

		//canonical labeling
		bliss::Stats stats;
		const unsigned int* cl = aGraph->canonical_form(stats, &report_aut, stdout);

		//permute to canonical form
		bliss::AbstractGraph* cf = aGraph->permute(cl);
		writeOut(cf, outfile.append(".cano"));

		//write back to tuple
		std::vector<Element_In_Tuple> out = writeGraph(cf);

		delete cf;
		delete aGraph;

		return out;
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

	static std::vector<Element_In_Tuple> writeGraph(bliss::AbstractGraph* cf){
		std::vector<Element_In_Tuple> out;
		return out;
	}


	static void writeOut(bliss::AbstractGraph* graph, std::string& outfile){
		FILE* const fp = fopen(outfile.c_str(), "w");
		if (!fp)
			printf("Cannot open '%s' for outputting the canonical form, aborting", outfile.c_str());
		graph->write_dimacs(fp);
		fclose(fp);
	}

};

#endif /* SRC_CORE_BLISS_LIB_HPP_ */
