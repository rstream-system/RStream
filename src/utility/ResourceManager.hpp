/*
 * ResourceManager.hpp
 *
 *  Created on: Dec 16, 2016
 *      Author: icuzzq
 */

#ifndef UTILITY_RESOURCEMANAGER_HPP_
#define UTILITY_RESOURCEMANAGER_HPP_

#include "Timer.hpp"

class ResourceManager {
public:
	/* Class Constructors & Destructor */
	ResourceManager();
	virtual ~ResourceManager();

	/*Public Methods*/
	std::string result();


    inline std::string getWallTimeString(){
    	return this->wall_ms_string;
    }

    inline std::string getCPUTimeString(){
    	return this->cpu_ms_string;
    }

    inline std::string getMemoryString(){
    	return this->memory_kb_string;
    }

private:
	/* Declaring Variables */
	boost::posix_time::ptime startTime;

	/*Private Methods*/
	static void getUsage(double& totalTime, double& peakMem);


    std::string wall_ms_string;
    std::string cpu_ms_string;
    std::string memory_kb_string;

};



#endif /* UTILITY_RESOURCEMANAGER_HPP_ */
