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

	friend std::ostream & operator<<(std::ostream & strm, const MTuple& cg);

public:
	MTuple(unsigned int size_of_tuple);
	virtual ~MTuple();

	void init(char * update_local_buf);

	virtual Element_In_Tuple& at(unsigned int index);

	inline unsigned int get_size() const{
		return size;
	}

	inline Element_In_Tuple* get_elements(){
		return elements;
	}

	inline virtual unsigned int get_num_vertices(){
		return elements[size - 1].key_index;
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

	void push(Element_In_Tuple* element);

	void pop();

	inline Element_In_Tuple* get_added_element(){
		return added_element;
	}

	inline void set_num_vertices(unsigned int num){
		added_element->key_index = (BYTE)num;
	}

	inline unsigned int get_num_vertices(){
		return added_element->key_index;
	}


protected:
	unsigned int capacity;
	Element_In_Tuple* added_element;


};


class MTuple_simple {
	friend std::ostream & operator<<(std::ostream & strm, const MTuple_simple& cg);
public:
	MTuple_simple(unsigned int size_of_tuple);
	virtual ~MTuple_simple();


	void init(char * update_local_buf);

	virtual Base_Element& at(unsigned int index);

	bool operator==(const MTuple_simple& other) const;

	virtual inline unsigned int get_hash() const {
		bliss::UintSeqHash h;

		for(unsigned int i = 0; i < size; ++i){
			h.update(elements[i].id);
		}

		return h.get_value();
	}

	inline unsigned int get_size() const{
		return size;
	}

	inline Base_Element* get_elements(){
		return elements;
	}


protected:
	unsigned int size;
	Base_Element* elements;

};


class MTuple_join_simple : public MTuple_simple {
	friend std::ostream & operator<<(std::ostream & strm, const MTuple_join_simple& cg);
public:
	MTuple_join_simple(unsigned int size_of_tuple);
	virtual ~MTuple_join_simple();


	Base_Element& at(unsigned int index);

	void push(Base_Element* element);

	void pop();

	inline unsigned int get_hash(){
		bliss::UintSeqHash h;

		for(unsigned int i = 0; i < size; ++i){
			h.update(elements[i].id);
		}
		h.update(added_element->id);

		return h.get_value();
	}

	inline Base_Element* get_added_element(){
		return added_element;
	}


private:
	unsigned int capacity;
	Base_Element* added_element;

};




}

namespace std {
	template<>
	struct hash<RStream::MTuple_simple> {
		std::size_t operator()(const RStream::MTuple_simple& qp) const {
			//simple hash
			return std::hash<int>()(qp.get_hash());
		}
	};
}


#endif /* SRC_STRUCT_MINING_TUPLE_HPP_ */
