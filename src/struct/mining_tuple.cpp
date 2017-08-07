/*
 * mining_tuple.cpp
 *
 *  Created on: Aug 5, 2017
 *      Author: icuzzq
 */

#include "mining_tuple.hpp"

namespace RStream{


MTuple::MTuple(unsigned int size_of_t) {
	size = size_of_t / sizeof(Element_In_Tuple);
	elements = nullptr;
}

MTuple::~MTuple(){

}

void MTuple::init(char* update_local_buf){
	elements = (Element_In_Tuple*)update_local_buf;
}

Element_In_Tuple& MTuple::at(unsigned int index){
	return elements[index];
}

std::ostream & operator<<(std::ostream & strm, const MTuple& tuple){
	if(tuple.get_size() == 0){
		strm << "(empty)";
		return strm;
	}

	strm << "(";
	for(unsigned int index = 0; index < tuple.get_size() - 1; ++index){
		strm << tuple.elements[index] << ", ";
	}
	strm << tuple.elements[tuple.get_size() - 1];
	strm << ")";
	return strm;
}


MTuple_join::MTuple_join(unsigned int size_of_t): MTuple(size_of_t) {
	capacity = size + 1;
	num_vertices = 0;
	added_element = nullptr;
}

MTuple_join::~MTuple_join(){

}

void MTuple_join::init(char * update_local_buf, std::unordered_set<VertexId>& vertices_set){
	vertices_set.reserve(size);

	for(unsigned int index = 0; index < size; index++) {
		Element_In_Tuple element = *(Element_In_Tuple*)(update_local_buf + index * sizeof(Element_In_Tuple));
		vertices_set.insert(element.vertex_id);
	}

	elements = (Element_In_Tuple*)update_local_buf;
}

Element_In_Tuple& MTuple_join::at(unsigned int index){
	if(index == capacity - 1){
		return *added_element;
	}
	return elements[index];
}

void MTuple_join::push(Element_In_Tuple& element){
	added_element = &element;
	size++;
}

void MTuple_join::pop(){
	added_element = nullptr;
	size--;
}


}




