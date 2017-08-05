/*
 * global_info.hpp
 *
 *  Created on: Jul 20, 2017
 *      Author: kai
 */

#ifndef CORE_GLOBAL_INFO_HPP_
#define CORE_GLOBAL_INFO_HPP_

#include "../utility/FileUtil.hpp"
#include "scatter.hpp"

namespace RStream {
	class Global_Info {

	public:
		static long count(Update_Stream result, int sizeof_an_item, Engine & context) {
			long res = 0;

			for(int i = 0; i < context.num_partitions; i++) {
				int fd_update = open((context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(result)).c_str(), O_RDONLY);
				assert(fd_update > 0);
				res += io_manager::get_filesize(fd_update);
				close(fd_update);
			}

			assert(res % sizeof_an_item == 0);

			return res / sizeof_an_item;
		}

		static void delete_upstream(Update_Stream in_update_stream, Engine & context){
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				std::string filename = context.filename + "." + std::to_string(partition_id) + ".update_stream_" + std::to_string(in_update_stream);
				FileUtil::delete_file(filename);
			}
		}
	};
}



#endif /* CORE_GLOBAL_INFO_HPP_ */
