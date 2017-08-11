/*
 * FileUtil.hpp
 *
 *  Created on: Aug 1, 2017
 *      Author: icuzzq
 */

#ifndef SRC_UTILITY_FILEUTIL_HPP_
#define SRC_UTILITY_FILEUTIL_HPP_

#include "../common/RStreamCommon.hpp"

namespace RStream{

class FileUtil{

public:
	static void delete_file(const std::string& file_name){
		if (std::remove(file_name.c_str()) != 0)
			perror("Error deleting file");
		else{
//			std::cout << (file_name + " successfully deleted.\n");
		}
	}

	static bool file_exists(const std::string  filename) {
		struct stat buffer;
		return (stat(filename.c_str(), &buffer) == 0);
	}

};

}


#endif /* SRC_UTILITY_FILEUTIL_HPP_ */
