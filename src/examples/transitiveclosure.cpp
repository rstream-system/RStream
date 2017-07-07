///*
// * transitiveclosure.cpp
// *
// *  Created on: Jul 6, 2017
// *      Author: kai
// */
//
//#include "../core/engine.hpp"
//#include "../core/scatter.hpp"
//#include "../core/relation_phase.hpp"
//#include "BaseApplication.hpp"
//
//using namespace RStream;
//
//struct Update_Stream_TC {
//	VertexId src;
//	VertexId target;
//
//	bool operator == (const Update_Stream_TC & obj) const {
//		if(src == obj.src && target == obj.target)
//			return true;
//		else
//			return false;
//	}
//};
//
//namespace std {
//	template<>
//	struct hash<Update_Stream_TC> {
//		size_t operator() (const Update_Stream_TC & obj) const {
//			size_t hash = 17;
//			hash = 31 * hash + obj.src;
//			hash = 31 * hash + obj.target;
//
//			return hash;
//		}
//	};
//}
//
//
//
