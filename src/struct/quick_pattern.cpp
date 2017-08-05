/*
 * quick_pattern.cpp
 *
 *  Created on: Aug 4, 2017
 *      Author: icuzzq
 */

#include "quick_pattern.hpp"

namespace RStream {


	Quick_Pattern::Quick_Pattern(){

	}

//	Quick_Pattern(std::vector<Element_In_Tuple>& t){
//		tuple = t;
//	}

	Quick_Pattern::~Quick_Pattern(){

	}

	//operator for map
	bool Quick_Pattern::operator==(const Quick_Pattern& other) const {
		//compare edges
		assert(tuple.size() == other.tuple.size());
		for(unsigned int i = 0; i < tuple.size(); ++i){
			const Element_In_Tuple & t1 = tuple[i];
			const Element_In_Tuple & t2 = other.tuple[i];

			int cmp_element = t1.cmp(t2);
			if(cmp_element != 0){
				return false;
			}
		}

		return true;
	}

	unsigned int Quick_Pattern::get_hash() const {
		//TODO
		bliss::UintSeqHash h;

		h.update(tuple.size());

		//hash vertex labels and edges
		for(unsigned int i = 0; i < tuple.size(); ++i){
			auto element = tuple[i];
			h.update(element.vertex_id);
			h.update(element.vertex_label);
			h.update(element.history_info);
		}

		return h.get_value();

//		return 0;
	}




std::ostream & operator<<(std::ostream & strm, const Quick_Pattern& quick_pattern){
	strm << quick_pattern.get_tuple();
	return strm;
}

}
