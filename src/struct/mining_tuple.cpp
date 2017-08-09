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

//-----------------------------------------------------------------------------------


MTuple_join::MTuple_join(unsigned int size_of_t): MTuple(size_of_t) {
	capacity = size + 1;
	added_element = nullptr;
}

MTuple_join::~MTuple_join(){

}

void MTuple_join::init(char * update_local_buf, std::unordered_set<VertexId>& vertices_set){
	vertices_set.reserve(size);

	for(unsigned int index = 0; index < size; index++) {
		Element_In_Tuple* element = (Element_In_Tuple*)(update_local_buf + index * sizeof(Element_In_Tuple));
		vertices_set.insert(element->vertex_id);
	}

	elements = (Element_In_Tuple*)update_local_buf;
}

Element_In_Tuple& MTuple_join::at(unsigned int index){
	if(index == capacity - 1){
		return *added_element;
	}
	return elements[index];
}

void MTuple_join::push(Element_In_Tuple* element){
	added_element = element;
	size++;
}

void MTuple_join::pop(){
	added_element = nullptr;
	size--;
}

//======================================================================================


MTuple_simple::MTuple_simple(unsigned int size_of_t){
	size = size_of_t / sizeof(Base_Element);
	elements = nullptr;
}

MTuple_simple::~MTuple_simple(){

}

void MTuple_simple::init(char* update_local_buf){
	elements = (Base_Element*)update_local_buf;
}

Base_Element& MTuple_simple::at(unsigned int index){
	return elements[index];
}

bool MTuple_simple::operator==(const MTuple_simple& other) const{
	assert(size == other.size);
	for(unsigned int i = 0; i < size; ++i){
		if(elements[i].id != other.elements[i].id){
			return false;
		}
	}

	return true;
}

std::ostream & operator<<(std::ostream & strm, const MTuple_simple& tuple){
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

//-----------------------------------------------------------------------------------

MTuple_join_simple::MTuple_join_simple(unsigned int size_of_t) : MTuple_simple(size_of_t){
	capacity = size + 1;
	added_element = nullptr;
}

MTuple_join_simple::~MTuple_join_simple(){

}

Base_Element& MTuple_join_simple::at(unsigned int index){
	if(index == capacity - 1){
		return *added_element;
	}
	return elements[index];
}


void MTuple_join_simple::push(Base_Element* element){
	added_element = element;
	size++;
}

void MTuple_join_simple::pop(){
	added_element = nullptr;
	size--;
}

std::ostream & operator<<(std::ostream & strm, const MTuple_join_simple& tuple){
	if(tuple.get_size() == 0){
		strm << "(empty)";
		return strm;
	}

	strm << "(";

	if(tuple.size == tuple.capacity){
		for(unsigned int index = 0; index < tuple.get_size() - 1; ++index){
			strm << tuple.elements[index] << ", ";
		}
		strm << *(tuple.added_element);
	}
	else{
		for(unsigned int index = 0; index < tuple.get_size() - 1; ++index){
			strm << tuple.elements[index] << ", ";
		}
		strm << tuple.elements[tuple.get_size() - 1];
	}

	strm << ")";
	return strm;
}


}




