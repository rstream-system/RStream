///*
// * blisstest.cpp
// *
// *  Created on: Jul 3, 2017
// *      Author: icuzzq
// */
//
//#include "../core/pattern.hpp"
//
//int main(int argc, char **argv) {
////	const char* in = "/home/icuzzq/Desktop/bliss-0.73/inputs/input2.txt";
////	std::string out = "/home/icuzzq/Desktop/bliss-0.73/inputs/out.txt";
////	Bliss_Lib::turn_canonical_graph(in, out);
//
//
////	std::vector<Element_In_Tuple> sub_graph;
////	Element_In_Tuple tuple1 (1, 0, 2, 0);
////	Element_In_Tuple tuple2 (2, 0, 3, 0);
////	Element_In_Tuple tuple3 (3, 0, 4, 0);
////	sub_graph.push_back(tuple1);
////	sub_graph.push_back(tuple2);
////	sub_graph.push_back(tuple3);
////
////	Canonical_Graph* cg = Pattern::turn_canonical_graph(sub_graph, false);
////	std::cout << *cg << std::endl;
////	delete cg;
////
////	sub_graph.clear();
////	Element_In_Tuple tuple21 (1, 0, 1, 0);
////	Element_In_Tuple tuple22 (2, 0, 3, 0);
////	Element_In_Tuple tuple23 (3, 0, 4, 0);
////	sub_graph.push_back(tuple21);
////	sub_graph.push_back(tuple22);
////	sub_graph.push_back(tuple23);
////
////	Canonical_Graph* cg2 = Pattern::turn_canonical_graph(sub_graph, false);
////	std::cout << *cg2 << std::endl;
////	delete cg2;
//
//	std::vector<Element_In_Tuple> sub_graph1;
//	Element_In_Tuple tuple1 (1, 0, 'a', 0);
//	Element_In_Tuple tuple2 (2, 0, 'b', 0);
//	Element_In_Tuple tuple3 (3, 0, 'c', 0);
//	sub_graph1.push_back(tuple1);
//	sub_graph1.push_back(tuple2);
//	sub_graph1.push_back(tuple3);
//
//	Pattern::turn_quick_pattern_sideffect(sub_graph1);
//	std::cout << "quick pattern1: " << std::endl;
//    std::cout << sub_graph1 << std::endl;
//
//	Canonical_Graph* cg1 = Pattern::turn_canonical_graph(sub_graph1, false);
////	std::cout << *cg1 << std::endl;
//	std::cout << "cg1: " << std::endl;
//	std::cout << cg1->get_tuple() << std::endl << std::endl;;
//
//	std::vector<Element_In_Tuple> sub_graph2;
//	Element_In_Tuple tuple4 (1, 0, 'a', 0);
//	Element_In_Tuple tuple5 (3, 0, 'c', 0);
//	Element_In_Tuple tuple6 (4, 0, 'b', 0);
//	sub_graph2.push_back(tuple4);
//	sub_graph2.push_back(tuple5);
//	sub_graph2.push_back(tuple6);
//
//	Pattern::turn_quick_pattern_sideffect(sub_graph2);
//	std::cout << "quick pattern2: " << std::endl;
//	std::cout << sub_graph2 << std::endl;
//
//	Canonical_Graph* cg2 = Pattern::turn_canonical_graph(sub_graph2, false);
////	std::cout << *cg2 << std::endl;
//
//	std::cout << "cg2: " << std::endl;
//	std::cout << cg2->get_tuple() << std::endl << std::endl;
//
//	std::vector<Element_In_Tuple> sub_graph3;
//	Element_In_Tuple tuple7 (2, 0, 'b', 0);
//	Element_In_Tuple tuple8 (5, 0, 'a', 0);
//	Element_In_Tuple tuple9 (3, 0, 'c', 1);
//	sub_graph3.push_back(tuple7);
//	sub_graph3.push_back(tuple8);
//	sub_graph3.push_back(tuple9);
//
//	Pattern::turn_quick_pattern_sideffect(sub_graph3);
//	std::cout << "quick pattern3: " << std::endl;
//	std::cout << sub_graph3 << std::endl;
//
//	Canonical_Graph* cg3 = Pattern::turn_canonical_graph(sub_graph3, false);
//
//	std::cout << "cg3: " << std::endl;
//	std::cout << cg3->get_tuple() << std::endl;
//
//}



