//
//#include "../core/engine.hpp"
//#include "../core/scatter.hpp"
//#include "../core/relation_phase.hpp"
//#include "../core/global_info.hpp"
////#include "BaseApplication.hpp"
//
//using namespace RStream;
//
//struct RInUpdate_TriC : BaseUpdate {
//	VertexId src;
//
//	RInUpdate_TriC() : BaseUpdate () {
////		target = 0;
////		src = 0;
//	}
//
//	RInUpdate_TriC(VertexId t, VertexId s) : BaseUpdate(t){
//		src = s;
//	}
//};
//
//inline std::ostream & operator<<(std::ostream & strm, const RInUpdate_TriC& update){
//	strm << "(" << update.target << ", " << update.src << ")";
//	return strm;
//}
//
//struct ROutUpdate_TriC : BaseUpdate {
//	VertexId src1;
//	VertexId src2;
//
//	ROutUpdate_TriC() : BaseUpdate () {
////		target = 0;
////		src1 = 0;
////		src2 = 0;
//	}
//
//	ROutUpdate_TriC(VertexId t, VertexId s1, VertexId s2) : BaseUpdate(t){
//		src1 = s1;
//		src2 = s2;
//	}
//
//};
//
//inline std::ostream & operator<<(std::ostream & strm, const ROutUpdate_TriC& update){
//	strm << "(" << update.target << ", " << update.src1 << ", " << update.src2 << ")";
//	return strm;
//}
//
//
//RInUpdate_TriC* generate_one_update(Edge * e)
//{
//	RInUpdate_TriC* update = new RInUpdate_TriC(e->target, e->src);
//	return update;
//}
//
//
//class R1 : public RPhase<RInUpdate_TriC, ROutUpdate_TriC> {
//public:
//	R1(Engine & e) : RPhase(e) {};
//	~R1(){};
//
//	bool filter(RInUpdate_TriC * update, Edge * edge) {
////		if(update->src > update->target || edge->src > edge->target) return true;
////		return false;
//
//		if(update->src < update->target && edge->src < edge->target)
//			return false;
//
//		return true;
//	}
//
//	ROutUpdate_TriC * project_columns(RInUpdate_TriC * in_update, Edge * edge) {
//		ROutUpdate_TriC * new_update = new ROutUpdate_TriC(edge->target, in_update->src, in_update->target);
//		return new_update;
//	}
//};
//
//class R2 : public RPhase<ROutUpdate_TriC, ROutUpdate_TriC> {
//public:
//	R2(Engine & e) : RPhase(e) {};
//	~R2(){};
//
//	bool filter(ROutUpdate_TriC * update, Edge * edge) {
//		if(update->src1 != edge->target) return true;
//		return false;
//	}
//
//	ROutUpdate_TriC * project_columns(ROutUpdate_TriC * in_update, Edge * edge) {
//		ROutUpdate_TriC * new_update = new ROutUpdate_TriC(in_update->target, in_update->src1, in_update->src2);
//		return new_update;
//	}
//};
//
//template<typename T>
//void printUpdateStream(int num_partitions, std::string fileName, Update_Stream in_stream){
//	for(int i = 0; i < num_partitions; i++) {
//		std::cout << "--------------------" + (fileName + "." + std::to_string(i) + ".update_stream_" + std::to_string(in_stream)) + "---------------------\n";
//		int fd_update = open((fileName + "." + std::to_string(i) + ".update_stream_" + std::to_string(in_stream)).c_str(), O_RDONLY);
////		assert(fd_update > 0 );
//
//		if(fd_update <= 0)
//			continue;
//
//		// get file size
//		long update_file_size = io_manager::get_filesize(fd_update);
//
//		char * update_local_buf = new char[update_file_size];
//		io_manager::read_from_file(fd_update, update_local_buf, update_file_size, 0);
//
//		// for each update
//		for(size_t pos = 0; pos < update_file_size; pos += sizeof(T)) {
//			// get an update
//			T & update = *(T*)(update_local_buf + pos);
//			std::cout << update << std::endl;
//		}
//	}
//}
//
//
//int main(int argc, char ** argv) {
//
////	Engine e("/home/icuzzq/Workspace/git/RStream/input/input_new.txt", 3, 6);
//
//	Engine e("/home/kai/workspace/rstream_data/soc-LiveJournal1.txt", 10, 4847571);
////	Engine e("/home/kai/workspace/rstream_data/com-amazon.ungraph.txt", 4, 600000);
////	Engine e("/home/kai/workspace/rstream_data/test.txt", 3, 7);
//
//	// get running time (wall time)
//	auto start = std::chrono::high_resolution_clock::now();
//
//	//scatter phase first to generate updates
//	Scatter<BaseVertex, RInUpdate_TriC> scatter_phase(e);
//	Update_Stream in_stream = scatter_phase.scatter_no_vertex(generate_one_update);
////	printUpdateStream<RInUpdate_TriC>(e.num_partitions, e.filename, in_stream);
////
////	//relational phase 1
//	R1 r1(e);
//	Update_Stream out_stream_1 = r1.join(in_stream);
////	printUpdateStream<ROutUpdate_TriC>(e.num_partitions, e.filename, out_stream_1);
////
////	//relational phase 2
//	R2 r2(e);
//	Update_Stream out_stream_2 = r2.join(out_stream_1);
////	printUpdateStream<ROutUpdate_TriC>(e.num_partitions, e.filename, out_stream_2);
//
//	auto end = std::chrono::high_resolution_clock::now();
//	std::chrono::duration<double> diff = end - start;
//	std::cout << "Finish triangle counting. Running time : " << diff.count() << " s\n";
//
//	std::cout << "Triangle Counting : " << Global_Info::count(out_stream_2, sizeof(ROutUpdate_TriC), e) << std::endl;
//}




