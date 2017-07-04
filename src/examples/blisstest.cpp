/*
 * blisstest.cpp
 *
 *  Created on: Jul 3, 2017
 *      Author: icuzzq
 */

#include "../core/pattern.hpp"

int main(int argc, char **argv) {
//	const char* in = "/home/icuzzq/Desktop/bliss-0.73/inputs/input2.txt";
	std::string out = "/home/icuzzq/Desktop/bliss-0.73/inputs/out.txt";
//	Bliss_Lib::turn_canonical_graph(in, out);


	std::vector<Element_In_Tuple> sub_graph;
	Element_In_Tuple* tuple0 = new Element_In_Tuple(1, 0, 1);
	Element_In_Tuple* tuple1 = new Element_In_Tuple(2, 0, 2, 0);
	Element_In_Tuple* tuple2 = new Element_In_Tuple(4, 0, 4, 1);
	Element_In_Tuple* tuple3 = new Element_In_Tuple(3, 0, 3, 1);
	sub_graph.push_back(*tuple0);
	sub_graph.push_back(*tuple1);
	sub_graph.push_back(*tuple2);
	sub_graph.push_back(*tuple3);

	pattern::turn_canonical_graph(sub_graph, false);

}



