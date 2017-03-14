/*
 * pagerank.cpp
 *
 *  Created on: Mar 7, 2017
 *      Author: kai
 */

#include "../core/engine.hpp"

using namespace RStream;

struct Update : T {
	float sum;

	Update(int t, float s) {
		target = t;
		sum = s;
	}
};

Update* generate_one_update(Edge & e)
{
	Update* update = new Update(e.target, 0);
	return update;
}

int main(int argc, const char ** argv) {
	engine graph_engine("/home/icuzzq/Workspace/git/RStream/input/input");
	std::function<T*(Edge&)> gen_update = generate_one_update;
	graph_engine.scatter(generate_one_update);
}


