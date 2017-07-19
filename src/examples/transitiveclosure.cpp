///*
// * transitiveclosure.cpp
// *
// *  Created on: Jul 6, 2017
// *      Author: kai
// */

#include "../core/engine.hpp"
#include "../core/scatter.hpp"
#include "../core/scatter_updates.hpp"
#include "../core/relation_phase.hpp"

using namespace RStream;

struct Update_Stream_TC : BaseUpdate{
	VertexId src;
//	VertexId target;

	Update_Stream_TC() : BaseUpdate() {
		src = 0;
		target = 0;
	}

	Update_Stream_TC(VertexId _src, VertexId _target) : BaseUpdate(_target)
	{
		src = _src;
	}

	bool operator == (const Update_Stream_TC & obj) const {
		if(src == obj.src && target == obj.target)
			return true;
		else
			return false;
	}
};

typedef Update_Stream_TC In_Update_TC;
typedef Update_Stream_TC Out_Update_TC;

namespace std {
	template<>
	struct hash<Update_Stream_TC> {
		size_t operator() (const Update_Stream_TC & obj) const {
			size_t hash = 17;
			hash = 31 * hash + obj.src;
			hash = 31 * hash + obj.target;

			return hash;
		}
	};
}

Out_Update_TC * generate_one_update(Edge * e) {
	Out_Update_TC * out_update = new Out_Update_TC(e->src, e->target);
	return out_update;
}

Out_Update_TC * generate_out_update(In_Update_TC * in_update) {
	Out_Update_TC * out_update = new Out_Update_TC(in_update->src, in_update->target);
	return out_update;
}

class TC : public RPhase<In_Update_TC, Out_Update_TC> {
	public:
		TC(Engine & e) : RPhase(e) {};
		~TC(){};

		bool filter(In_Update_TC * update, Edge * edge) {
			// Do NOT add self edge
			if(update->src == edge->target)
				return true;

			return false;
		}

		Out_Update_TC * project_columns(In_Update_TC * in_update, Edge * edge) {
			Out_Update_TC * new_update = new Out_Update_TC(in_update->src, edge->target);
			return new_update;
		}
};

bool should_terminate(Update_Stream delta_tc, Engine & e) {
	for(int i = 0; i < e.num_partitions; i++) {
		int fd_update = open((e.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(delta_tc)).c_str(), O_RDONLY);

		if(io_manager::get_filesize(fd_update) > 0) {
			close(fd_update);
			return false;
		}

		close(fd_update);
	}

	return true;
}

inline std::ostream & operator<<(std::ostream & strm, const In_Update_TC& update){
	strm << "(" << update.target << ", " << update.src << ")";
	return strm;
}

template<typename T>
void printUpdateStream(int num_partitions, std::string fileName, Update_Stream in_stream){
	for(int i = 0; i < num_partitions; i++) {
		std::cout << "--------------------" + (fileName + "." + std::to_string(i) + ".update_stream_" + std::to_string(in_stream)) + "---------------------\n";
		int fd_update = open((fileName + "." + std::to_string(i) + ".update_stream_" + std::to_string(in_stream)).c_str(), O_RDONLY);
//		assert(fd_update > 0 );

		if(fd_update <= 0)
			continue;

		// get file size
		long update_file_size = io_manager::get_filesize(fd_update);

		char * update_local_buf = new char[update_file_size];
		io_manager::read_from_file(fd_update, update_local_buf, update_file_size, 0);

		// for each update
		for(size_t pos = 0; pos < update_file_size; pos += sizeof(T)) {
			// get an update
			T & update = *(T*)(update_local_buf + pos);
			std::cout << update << std::endl;
		}
	}
}

int main(int argc, char ** argv) {
//	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_new.txt", 3, 6);
	Engine e("/home/kai/workspace/rstream_data/random/test.txt", 3, 10);

	//scatter phase first to generate updates
//	Scatter<BaseVertex, In_Update_TC> scatter_edges(e);
//	Update_Stream delta_tc = scatter_edges.scatter_no_vertex(generate_one_update);
//	printUpdateStream<In_Update_TC>(e.num_partitions, e.filename, delta_tc);

//	Update_Stream tc = scatter_edges.scatter_no_vertex(generate_one_update);
//	printUpdateStream<In_Update_TC>(e.num_partitions, e.filename, tc);

//	Scatter_Updates<In_Update_TC, Out_Update_TC> sc_up(e);
//	TC triangle_counting(e);
//
//	while(!should_terminate(delta_tc, e)) {
//
//		Update_Stream out = triangle_counting.join(delta_tc);
//		printUpdateStream<In_Update_TC>(e.num_partitions, e.filename, out);
//		Update_Stream delta = triangle_counting.set_difference(out, tc);
//		triangle_counting.union_relation(tc, delta);
//		Update_Stream new_delta = sc_up.scatter_updates(delta, generate_out_update);
//		delta_tc = new_delta;
//	}
}



