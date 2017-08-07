/*
 * mining_tuple.hpp
 *
 *  Created on: Aug 5, 2017
 *      Author: icuzzq
 */

#ifndef SRC_STRUCT_MINING_TUPLE_HPP_
#define SRC_STRUCT_MINING_TUPLE_HPP_

#include "type.hpp"

namespace RStream{

class MTuple{
public:
	MTuple(unsigned int size_of_tuple);
	virtual ~MTuple();

	void init(char * update_local_buf);

	virtual Element_In_Tuple& at(unsigned int index);

	inline unsigned int get_size(){
		return size;
	}

	inline Element_In_Tuple* get_elements(){
		return elements;
	}

protected:
	unsigned int size;
	Element_In_Tuple* elements;

};



class MTuple_join : public MTuple {

public:
	MTuple_join(unsigned int size_of_tuple);
	virtual ~MTuple_join();

	void init(char * update_local_buf, std::unordered_set<VertexId>& vertices_set);

	Element_In_Tuple& at(unsigned int index);

	void push(Element_In_Tuple& element);

	void pop();

	inline void set_num_vertices(unsigned int num){
		num_vertices = num;
	}

	inline unsigned int get_num_vertices(){
		return num_vertices;
	}

	inline Element_In_Tuple* get_added_element(){
		return added_element;
	}

private:
	unsigned int capacity;
	unsigned int num_vertices;
	Element_In_Tuple* added_element;


};















}


#endif /* SRC_STRUCT_MINING_TUPLE_HPP_ */
