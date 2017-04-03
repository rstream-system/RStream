/*
 * BaseApplication.hpp
 *
 *  Created on: Apr 3, 2017
 *      Author: icuzzq
 */

#ifndef SRC_EXAMPLES_BASEAPPLICATION_HPP_
#define SRC_EXAMPLES_BASEAPPLICATION_HPP_


#include "../core/engine.hpp"
#include "../core/scatter.hpp"
#include "../core/gather.hpp"
#include "../core/relation_phase.hpp"

namespace RStream{
class BaseApplication {
public:
	virtual void run() = 0;

};

}


#endif /* SRC_EXAMPLES_BASEAPPLICATION_HPP_ */
