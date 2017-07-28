/*
 * global_info.hpp
 *
 *  Created on: Jul 20, 2017
 *      Author: kai
 */

#ifndef CORE_GLOBAL_INFO_HPP_
#define CORE_GLOBAL_INFO_HPP_

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
	};
}



#endif /* CORE_GLOBAL_INFO_HPP_ */
