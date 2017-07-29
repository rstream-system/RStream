/*
 * ResourceManager.cpp
 *
 *  Created on: Dec 16, 2016
 *      Author: icuzzq
 */


#include "ResourceManager.hpp"


ResourceManager::ResourceManager(): startTime{boost::posix_time::second_clock::local_time()}, wall_ms_string("0"), cpu_ms_string("0"), memory_kb_string("0") {

}

ResourceManager::~ResourceManager(){

}

std::string ResourceManager::result(){
	//wall time usage
	boost::posix_time::time_duration diff = boost::posix_time::second_clock::local_time() - startTime;
	auto millisecs_wall = diff.total_milliseconds();
	double secs_wall = millisecs_wall / 1000.0;
	double mins_wall = secs_wall / 60.0;
	double hours_wall = mins_wall / 60.0;

    double time_cpu, memory;
	getUsage(time_cpu, memory);

	//cpu time usage
	double millisecs_cpu = time_cpu;
	double secs_cpu = millisecs_cpu / 1000.0;
	double mins_cpu = secs_cpu / 60.0;
	double hours_cpu = mins_cpu / 60.0;

	//peak memory usage
	double kbm = memory;
	double mbm = kbm / 1024.0;
	double gbm = mbm / 1024.0;

	this->wall_ms_string = Timer::to_string_with_precision(millisecs_wall, 3);
	this->cpu_ms_string = Timer::to_string_with_precision(millisecs_cpu, 3);
	this->memory_kb_string = Timer::to_string_with_precision(kbm, 3);

	return
		"Wall time: " +
		Timer::to_string_with_precision(millisecs_wall, 3) + " ms; " +
		Timer::to_string_with_precision(secs_wall, 3) + " s; " +
		Timer::to_string_with_precision(mins_wall, 3) + " m; " +
		Timer::to_string_with_precision(hours_wall, 3) + " h\n" +
		"CPU time: " +
		Timer::to_string_with_precision(millisecs_cpu, 3) + " ms; " +
		Timer::to_string_with_precision(secs_cpu, 3) + " s; " +
		Timer::to_string_with_precision(mins_cpu, 3) + " m; " +
		Timer::to_string_with_precision(hours_cpu, 3) + " h\n" +
		"Peak memory: " +
		Timer::to_string_with_precision(kbm, 3) + " KB; " +
		Timer::to_string_with_precision(mbm, 3) + " MB; " +
		Timer::to_string_with_precision(gbm, 3) + " GB";
}

void ResourceManager::getUsage(double& totalTime, double& peakMem){
    struct rusage CurUsage;
    getrusage(RUSAGE_SELF, &CurUsage);
    double UCPUTime = (double)((CurUsage.ru_utime.tv_sec * (uint64)1000000) +
                               CurUsage.ru_utime.tv_usec) / 1000.0;
    double SCPUTime = (double)((CurUsage.ru_stime.tv_sec * (uint64)1000000) +
                               CurUsage.ru_stime.tv_usec) / 1000.0;
    totalTime = (UCPUTime + SCPUTime);
    peakMem = (double)CurUsage.ru_maxrss;
}


