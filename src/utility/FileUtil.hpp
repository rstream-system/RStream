/*
 * FileUtil.hpp
 *
 *  Created on: Aug 1, 2017
 *      Author: icuzzq
 */

#ifndef SRC_UTILITY_FILEUTIL_HPP_
#define SRC_UTILITY_FILEUTIL_HPP_

#include "../common/RStreamCommon.hpp"

class FileUtil{

public:
	static void delete_file(const std::string& file_name){
		if (std::remove(file_name.c_str()) != 0)
			perror("Error deleting file");
		else
			std::cout << (file_name + " successfully deleted.\n");
	}



};




#endif /* SRC_UTILITY_FILEUTIL_HPP_ */
