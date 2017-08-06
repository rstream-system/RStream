/*
 * mining_tuple.cpp
 *
 *  Created on: Aug 5, 2017
 *      Author: icuzzq
 */

#include "mining_tuple.hpp"

namespace RStream{





//MTuple_vector::MTuple_vector(): size_of_tuple(0), num_vertices(0) {}

MTuple_vector::MTuple_vector(unsigned int size_of_t): num_vertices(0) {
	size = size_of_t / sizeof(Element_In_Tuple);
	capacity = size + 1;
	elements = new Element_In_Tuple[capacity];
}

MTuple_vector::~MTuple_vector(){
	delete[] elements;
}


void MTuple_vector::init(char * update_local_buf, std::unordered_set<VertexId>& vertices_set){
	vertices_set.reserve(size);

	for(unsigned int index = 0; index < size; index++) {
		Element_In_Tuple element = *(Element_In_Tuple*)(update_local_buf + index * sizeof(Element_In_Tuple));
		elements[index] = element;
		vertices_set.insert(element.vertex_id);
	}
}

void MTuple_vector::push(Element_In_Tuple& element){
	elements[size++] = element;
}

void MTuple_vector::pop(){
	size--;
}


}




