/*
 * pagerank.cpp
 *
 *  Created on: Mar 7, 2017
 *      Author: kai
 */

#include <core/engine.hpp>

using namespace RStream;

int generate_one_update()
{
	return 0;
}

int main(int argc, const char ** argv) {
	engine<float, float, int> graph_engine("test.txt");
	std::function<int(Edge&)> gen_update = generate_one_update;
	graph_engine.scatter(generate_one_update);
}


