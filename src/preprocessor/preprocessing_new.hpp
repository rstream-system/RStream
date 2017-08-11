/*
 * preprocessing_new.hpp
 *
 *  Created on: Jul 26, 2017
 *      Author: kai
 */

#ifndef UTILITY_PREPROCESSING_NEW_HPP_
#define UTILITY_PREPROCESSING_NEW_HPP_

#include "../core/buffer_manager.hpp"

namespace RStream {
	class Preprocessing_new {
		std::string input;
		int format;

		VertexId minVertexId;
		VertexId maxVertexId;

		int numPartitions;
		int numVertices;
		int vertices_per_partition;

		int edgeType;
		int edge_unit;

		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_number;
		int num_exec_threads;
		int num_write_threads;
		std::vector<int> degree;
		std::vector<std::pair<VertexId, VertexId>> intervals;

	public:
		Preprocessing_new(std::string & _input, int _num_partitioins, int _format) : input(_input), format(_format),minVertexId(INT_MAX), maxVertexId(INT_MIN),
			numPartitions(_num_partitioins), numVertices(0), vertices_per_partition(0), edgeType(0), edge_unit(0){
			num_exec_threads = 3;
			num_write_threads = 1;

			atomic_init();
			run();
		}

		void atomic_init() {
			atomic_num_producers = num_exec_threads;
			atomic_partition_number = numPartitions;
		}

		void run() {
			std::cout << "start preprocessing..." << std::endl;

			// convert txt to binary
			if(format == (int)FORMAT::EdgeList) {

				// check if .binary exists already
//				if(!FileUtil::file_exists(input + ".binary")) {
					std::cout << "start to convert edge list file..." << std::endl;
					convert_edgelist();
					std::cout << "convert edge list file done." << std::endl;
//				}

				if(edgeType == (int)EdgeType::NO_WEIGHT) {
					std::cout << "start to partition on vertices..." << std::endl;
					partition_on_vertices<Edge>();
					std::cout << "partition on vertices done." << std::endl;

//					std::cout << "start to partition on edges..." << std::endl;
//					partition_on_edges<Edge>();
//					std::cout << "partition on edges done." << std::endl;
				}

				std::cout << "gen partition done!" << std::endl;
				write_meta_file();

			} else if(format == (int)FORMAT::AdjList) {

				// check if .binary exists already
//				if(!FileUtil::file_exists(input + ".binary")) {
					std::cout << "start to convert adj list file..." << std::endl;
					convert_adjlist();
					std::cout << "convert adj list file done." << std::endl;
//				}

				std::cout << "start to partition on vertices..." << std::endl;
				partition_on_vertices<LabeledEdge>();

//				std::cout << "start to partition on edges..." << std::endl;
//				partition_on_edges<LabeledEdge>();

				std::cout << "gen partition done!" << std::endl;
				write_meta_file();
			}

		}

		// note: vertex id always starts with 0
		void convert_edgelist() {
			FILE * fd = fopen(input.c_str(), "r");
			assert(fd != NULL );
			char * buf = (char *)memalign(PAGE_SIZE, IO_SIZE);
			long pos = 0;

			long counter = 0;
			char s[1024];
			while(fgets(s, 1024, fd) != NULL) {
				if (s[0] == '#') continue; // Comment
				if (s[0] == '%') continue; // Comment

				char delims[] = "\t, ";
				char * t;
				t = strtok(s, delims);
				assert(t != NULL);

				VertexId from = atoi(t);
				t = strtok(NULL, delims);
				assert(t != NULL);
				VertexId to = atoi(t);

				minVertexId = std::min(minVertexId, from);
				minVertexId = std::min(minVertexId, to);
				maxVertexId = std::max(maxVertexId, from);
				maxVertexId = std::max(maxVertexId, to);
			}
			numVertices = maxVertexId - minVertexId + 1;
			degree = std::vector<int>(numVertices);

			fclose(fd);
			fd = fopen(input.c_str(), "r");
			while(fgets(s, 1024, fd) != NULL) {
				if (s[0] == '#') continue; // Comment
				if (s[0] == '%') continue; // Comment

				char delims[] = "\t, ";
				char * t;
				t = strtok(s, delims);
				assert(t != NULL);

				VertexId from = atoi(t);
				from -= minVertexId;
				t = strtok(NULL, delims);
				assert(t != NULL);
				VertexId to = atoi(t);
				to -= minVertexId;

				if(from == to) continue;

				degree.at(from)++;

//				void * data = nullptr;
				Weight val;
				/* Check if has value */
				t = strtok(NULL, delims);
				if(t != NULL) {
					val = atof(t);
//					data = new WeightedEdge(from, to, val);
					WeightedEdge * wedge = new WeightedEdge(from, to, val);
					edge_unit = sizeof(VertexId) * 2 + sizeof(Weight);
//					std::memcpy(buf + pos, data, edge_unit);
					std::memcpy(buf + pos, (void*)wedge, edge_unit);
					pos += edge_unit;
					edgeType = (int)EdgeType::WITH_WEIGHT;
					delete wedge;

				} else {
//					data = new Edge(from, to);
					Edge * edge = new Edge(from, to);
					edge_unit =  sizeof(VertexId) * 2;
//					std::memcpy(buf + pos, data, edge_unit);
					std::memcpy(buf + pos, (void*)edge, edge_unit);
					pos += edge_unit;
					edgeType = (int)EdgeType::NO_WEIGHT;
					delete edge;

				}
				counter++;
				if(counter % 1000000 == 0)
					std::cout << "Processing " << counter << " lines " << std::endl;

				assert(IO_SIZE % edge_unit == 0);
				if(pos >= IO_SIZE) {
					int perms = O_WRONLY | O_APPEND;
					int fout = open((input + ".binary").c_str(), perms, S_IRWXU);
					if(fout < 0){
						fout = creat((input + ".binary").c_str(), S_IRWXU);
					}
					io_manager::write_to_file(fout, buf, IO_SIZE);
					counter -= IO_SIZE / edge_unit;
					close(fout);
					pos = 0;
				}

			}

			int perms = O_WRONLY | O_APPEND;
			int fout = open((input + ".binary").c_str(), perms, S_IRWXU);
			if(fout < 0){
				fout = creat((input + ".binary").c_str(), S_IRWXU);
			}

			io_manager::write_to_file(fout, buf, counter * edge_unit);
			close(fout);
			free(buf);

			fclose(fd);
		};

		// note: vertex id always starts with 0
		// (int)src, (int)target, (BYTE)src_label, (BYTE)target_label
		void convert_adjlist() {
			edgeType = (int)EdgeType::Labeled;
			edge_unit = sizeof(VertexId) * 2 + sizeof(BYTE) * 2;

			FILE* fd = fopen(input.c_str(), "r");
			assert(fd != NULL);
			
			char buf[2048], delims[] = "\t ";
			VertexId vert;
			BYTE val;
			int count = 0, size = 0, maxsize = 0;
			std::vector<BYTE> vertLabels;
			while (fgets(buf, 2048, fd) != NULL) {
				int len = strlen(buf);
				if (size == 0) {
					vert = std::stoi(strtok(buf, delims));
					val = (BYTE)(std::stoi(strtok(NULL, delims)));
					
					if (count == 0) minVertexId = std::min(minVertexId, vert);
					vertLabels.push_back(val);
					count++;
				}

				size += len;
				if (buf[len-1] == '\n') {
					maxsize = std::max(size, maxsize);
					size = 0;
				}
			}
			fclose(fd);
			numVertices = count;
			degree = std::vector<int>(numVertices);

			fd = fopen(input.c_str(), "r");
			assert(fd != NULL);
			FILE* output = fopen((input + ".binary").c_str(), "wb");
			assert(output != NULL);
			char* adj_list = new char[maxsize+1];
			while (fgets(adj_list, maxsize+1, fd) != NULL) {
				int len = strlen(adj_list);
				adj_list[len-1] = 0;
				VertexId src = std::stoi(strtok(adj_list, delims));
				BYTE srcLab = (BYTE)(std::stoi(strtok(NULL, delims)));

				std::set<VertexId> neighbors;
				char* strp;
				while ((strp = strtok(NULL, delims)) != NULL) {
					VertexId tgt = std::stoi(strp);
					if (src == tgt) continue;
					neighbors.insert(tgt-minVertexId);
				}

				maxVertexId = std::max(maxVertexId, src);
				src -= minVertexId;
				degree.at(src) = neighbors.size();

				for (std::set<VertexId>::iterator iter = neighbors.begin(); iter != neighbors.end(); iter++) {
					BYTE tgtLab = vertLabels[*iter];
					VertexId tgt = *iter;
					fwrite((const void*) &src, sizeof(VertexId), 1, output);
					fwrite((const void*) &tgt, sizeof(VertexId), 1, output);
					fwrite((const void*) &srcLab, sizeof(BYTE), 1, output);
					fwrite((const void*) &tgtLab, sizeof(BYTE), 1, output);
				}
			}

			fclose(fd);
			fclose(output);
		}

		template<typename T>
		void partition_on_vertices() {
			vertices_per_partition = numVertices / numPartitions;

			VertexId intvalStart = 0, intvalEnd = 0;

			// gen vertex interval info
			for(int i = 0; i < numPartitions; i++) {
				// last partition
				if(i == numPartitions - 1) {
//					end = start + numVertices - vertices_per_partition * (numPartitions - 1) - 1;
//					meta_file << start << "\t" << end << "\n";

					intvalEnd = intvalStart + numVertices - vertices_per_partition * (numPartitions - 1) - 1;
					intervals.push_back(std::make_pair(intvalStart, intvalEnd));
				} else {
//					end = start + vertices_per_partition - 1;
//					meta_file << start << "\t" << end << "\n";
//					start = end + 1;

					intvalEnd = intvalStart + vertices_per_partition - 1;
					intervals.push_back(std::make_pair(intvalStart, intvalEnd));
					intvalStart = intvalEnd + 1;
				}
			}

			int fd = open((input + ".binary").c_str(), O_RDONLY);
			assert(fd > 0 );

			// get file size
			long file_size = io_manager::get_filesize(fd);
			int streaming_counter = file_size / (IO_SIZE * sizeof(T)) + 1;
			long valid_io_size = 0;
			long offset = 0;

			concurrent_queue<std::tuple<int, long, long>> * task_queue = new concurrent_queue<std::tuple<int, long, long>>(65536);
			// <fd, offset, length>
			for(int counter = 0; counter < streaming_counter; counter++) {
				if(counter == streaming_counter - 1)
					// TODO: potential overflow?
//					valid_io_size = file_size - IO_SIZE * (streaming_counter - 1);
					valid_io_size = file_size - IO_SIZE * sizeof(T) * (streaming_counter - 1);
				else
//					valid_io_size = IO_SIZE;
					valid_io_size = IO_SIZE * sizeof(T);

				task_queue->push(std::make_tuple(fd, offset, valid_io_size));
				offset += valid_io_size;
			}

			global_buffer<T> ** buffers_for_shuffle = buffer_manager<T>::get_global_buffers(numPartitions);

			std::vector<std::thread> exec_threads;
			for(int i = 0; i < num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->producer_partition_on_vertices<T>(buffers_for_shuffle, task_queue); } ));

			std::vector<std::thread> write_threads;
			for(int i = 0; i < num_write_threads; i++)
				write_threads.push_back(std::thread(&Preprocessing_new::consumer<T>, this, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;
			close(fd);

		};

		template <typename T>
		void partition_on_edges() {
			int fd = open((input + ".binary").c_str(), O_RDONLY);
			assert(fd > 0 );

			// get file size
			long file_size = io_manager::get_filesize(fd);
			// get num of edges
			long numEdges = file_size / sizeof(T);
			long edges_per_part = numEdges / numPartitions;

			long counter = 0;
			VertexId intvalStart = 0;

			// gen vertex interval info
			for(unsigned int i = 0; i < degree.size(); i++) {
				counter += degree.at(i);
				if(counter >= edges_per_part) {
					intervals.push_back(std::make_pair(intvalStart, i - 1));
					intvalStart = i;
					counter = 0;

				}
				if(intervals.size() == numPartitions - 1) {
					intervals.push_back(std::make_pair(i, degree.size() - 1));
				}
			}
			assert(intervals.size() == numPartitions);

			for(unsigned int i = 0; i < intervals.size(); i++) {
				std::cout << "interval " << i << " [ " << intervals.at(i).first << " , " << intervals.at(i).second << " ]" << std::endl;
			}

			int streaming_counter = file_size / (IO_SIZE * sizeof(T)) + 1;
			long valid_io_size = 0;
			long offset = 0;

			concurrent_queue<std::tuple<int, long, long>> * task_queue = new concurrent_queue<std::tuple<int, long, long>>(65536);
			// <fd, offset, length>
			for(int counter = 0; counter < streaming_counter; counter++) {
				if(counter == streaming_counter - 1)
					// TODO: potential overflow?
//					valid_io_size = file_size - IO_SIZE * (streaming_counter - 1);
					valid_io_size = file_size - IO_SIZE * sizeof(T) * (streaming_counter - 1);
				else
//					valid_io_size = IO_SIZE;
					valid_io_size = IO_SIZE * sizeof(T);

				task_queue->push(std::make_tuple(fd, offset, valid_io_size));
				offset += valid_io_size;
			}

			global_buffer<T> ** buffers_for_shuffle = buffer_manager<T>::get_global_buffers(numPartitions);

			std::vector<std::thread> exec_threads;
			for(int i = 0; i < num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->producer_partition_on_edges<T>(buffers_for_shuffle, task_queue); } ));

			std::vector<std::thread> write_threads;
			for(int i = 0; i < num_write_threads; i++)
				write_threads.push_back(std::thread(&Preprocessing_new::consumer<T>, this, buffers_for_shuffle));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;
			close(fd);


		};

		void write_meta_file() {
			std::ofstream meta_file(input + ".meta");
			if(meta_file.is_open()) {
				meta_file << edgeType << "\t" << edge_unit << "\n";
				meta_file << numVertices << "\t" << vertices_per_partition << "\n";

				VertexId start = 0, end = 0;
//				for(int i = 0; i < numPartitions; i++) {
//					// last partition
//					if(i == numPartitions - 1) {
//						end = start + numVertices - vertices_per_partition * (numPartitions - 1) - 1;
//						meta_file << start << "\t" << end << "\n";
//					} else {
//						end = start + vertices_per_partition - 1;
//						meta_file << start << "\t" << end << "\n";
//						start = end + 1;
//					}
//				}
				for(unsigned int i = 0; i < intervals.size(); i++) {
					meta_file << intervals.at(i).first << "\t" << intervals.at(i).second << "\n";
				}

			} else {
				std::cout << "Could not open meta file!";
			}

			meta_file.close();
		}


	private:
		template<typename T>
		void producer_partition_on_vertices(global_buffer<T> ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long> > * task_queue) {
			int fd = -1;
			long offset = 0, length = 0;
			auto one_task = std::make_tuple(fd, offset, length);

			VertexId src = 0, dst = 0;
			Weight weight = 0.0f;
			BYTE src_label, dst_label;
			char * local_buf = (char*)memalign(PAGE_SIZE, IO_SIZE * sizeof(T));

			// pop from queue
			while(task_queue->test_pop_atomic(one_task)){
				fd = std::get<0>(one_task);
				offset = std::get<1>(one_task);
				length = std::get<2>(one_task);

//				char * local_buf = (char*)memalign(PAGE_SIZE, IO_SIZE * sizeof(T));
//				int streaming_counter = length / (IO_SIZE * sizeof(T)) + 1;

				assert((length % sizeof(T)) == 0);
				io_manager::read_from_file(fd, local_buf, length, offset);

				for(long pos = 0; pos < length; pos += sizeof(T)) {
					src = *(VertexId*)(local_buf + pos);
					dst = *(VertexId*)(local_buf + pos + sizeof(VertexId));
					assert(src >= 0 && src < numVertices && dst >= 0 && dst < numVertices);

//					void * data = nullptr;
					if(typeid(T) == typeid(Edge)) {
//						data = new Edge(src, dst);
						Edge * data = new Edge(src, dst);

						int index = get_index_partition_vertices(src);

						global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, index);
						global_buf->insert((T*)data, index);

						delete data;

					} else if(typeid(T) == typeid(WeightedEdge)) {
						weight = *(Weight*)(local_buf + pos + sizeof(VertexId) * 2);
//						data = new WeightedEdge(src, dst, weight);
						WeightedEdge * data = new WeightedEdge(src, dst, weight);

						int index = get_index_partition_vertices(src);

						global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, index);
						global_buf->insert((T*)data, index);

						delete data;


					} else if(typeid(T) == typeid(LabeledEdge)) {
						src_label = *(BYTE*)(local_buf + pos + sizeof(VertexId) * 2);
						dst_label = *(BYTE*)(local_buf + pos + sizeof(VertexId) * 2 + sizeof(BYTE));
//						data = new LabeledEdge(src, dst, src_label, dst_label);
						LabeledEdge * data = new LabeledEdge(src, dst, src_label, dst_label);

						int index = get_index_partition_vertices(src);

						global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, index);
						global_buf->insert((T*)data, index);

						delete data;
					}

//					int index = get_index_partition_vertices(src);
//
//					global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, index);
//					global_buf->insert((T*)data, index);

				}


//				long valid_io_size = 0;
//				long off = offset;
//				// for all streaming
//				for(int counter = 0; counter < streaming_counter; counter++) {
//					if(counter == streaming_counter - 1)
//						// TODO: potential overflow?
//						valid_io_size = length - IO_SIZE * sizeof(T) * (streaming_counter - 1);
//					else
//						valid_io_size = IO_SIZE * sizeof(T);
//
//					assert(valid_io_size % sizeof(T) == 0);
//
//					io_manager::read_from_file(fd, local_buf, length, off);
//					off += valid_io_size;
//
//					for(long pos = 0; pos < valid_io_size; pos += sizeof(T)) {
//						src = *(VertexId*)(local_buf + pos);
//						dst = *(VertexId*)(local_buf + pos + sizeof(VertexId));
//						assert(src >= 0 && src < numVertices && dst >= 0 && dst < numVertices);
//
//						void * data = nullptr;
//						if(typeid(T) == typeid(Edge)) {
//							data = new Edge(src, dst);
//						} else if(typeid(T) == typeid(WeightedEdge)) {
//							weight = *(Weight*)(local_buf + pos + sizeof(VertexId) * 2);
//							data = new WeightedEdge(src, dst, weight);
//						} else if(typeid(T) == typeid(LabeledEdge)) {
//							src_label = *(BYTE*)(local_buf + pos + sizeof(VertexId) * 2);
//							dst_label = *(BYTE*)(local_buf + pos + sizeof(VertexId) * 2 + sizeof(BYTE));
//							data = new LabeledEdge(src, dst, src_label, dst_label);
//						}
//
//						int index = get_index_partition_vertices(src);
//
//						global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, index);
//						global_buf->insert((T*)data, index);
//
//					}
//
//				}

			}

			free(local_buf);
			atomic_num_producers--;
		}

		template<typename T>
		void producer_partition_on_edges(global_buffer<T> ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long> > * task_queue) {
			int fd = -1;
			long offset = 0, length = 0;
			auto one_task = std::make_tuple(fd, offset, length);

			VertexId src = 0, dst = 0;
			Weight weight = 0.0f;
			BYTE src_label, dst_label;
			char * local_buf = (char*)memalign(PAGE_SIZE, IO_SIZE * sizeof(T));

			// pop from queue
			while(task_queue->test_pop_atomic(one_task)){
				fd = std::get<0>(one_task);
				offset = std::get<1>(one_task);
				length = std::get<2>(one_task);

//				char * local_buf = (char*)memalign(PAGE_SIZE, IO_SIZE * sizeof(T));
//				int streaming_counter = length / (IO_SIZE * sizeof(T)) + 1;

				assert((length % sizeof(T)) == 0);
				io_manager::read_from_file(fd, local_buf, length, offset);

				for(long pos = 0; pos < length; pos += sizeof(T)) {
					src = *(VertexId*)(local_buf + pos);
					dst = *(VertexId*)(local_buf + pos + sizeof(VertexId));
					assert(src >= 0 && src < numVertices && dst >= 0 && dst < numVertices);

//					void * data = nullptr;
					if(typeid(T) == typeid(Edge)) {
//						data = new Edge(src, dst);
						Edge * data = new Edge(src, dst);

						int index = get_index_partition_edges(src);
						assert(index >= 0);

						global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, index);
						global_buf->insert((T*)data, index);

						delete data;

					} else if(typeid(T) == typeid(WeightedEdge)) {
						weight = *(Weight*)(local_buf + pos + sizeof(VertexId) * 2);
//						data = new WeightedEdge(src, dst, weight);
						WeightedEdge * data = new WeightedEdge(src, dst, weight);

						int index = get_index_partition_edges(src);
						assert(index >= 0);

						global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, index);
						global_buf->insert((T*)data, index);

						delete data;

					} else if(typeid(T) == typeid(LabeledEdge)) {
						src_label = *(BYTE*)(local_buf + pos + sizeof(VertexId) * 2);
						dst_label = *(BYTE*)(local_buf + pos + sizeof(VertexId) * 2 + sizeof(BYTE));
//						data = new LabeledEdge(src, dst, src_label, dst_label);
						LabeledEdge * data = new LabeledEdge(src, dst, src_label, dst_label);

						int index = get_index_partition_edges(src);
						assert(index >= 0);

						global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, index);
						global_buf->insert((T*)data, index);

						delete data;
					}

//					int index = get_index_partition_edges(src);
//					assert(index >= 0);
//
//					global_buffer<T>* global_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, index);
//					global_buf->insert((T*)data, index);

				}

			}

			free(local_buf);
			atomic_num_producers--;
		}

		template<typename T>
		void consumer(global_buffer<T> ** buffers_for_shuffle) {
			int counter = 0;

			while(atomic_num_producers != 0) {
				if(counter == numPartitions)
					counter = 0;

				int i = counter++;
				std::string file_name = (input + "." + std::to_string(i));
				global_buffer<T>* g_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, i);
				g_buf->flush(file_name, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;
				if(i >= 0){
					std::string file_name_str = (input + "." + std::to_string(i));
					global_buffer<T>* g_buf = buffer_manager<T>::get_global_buffer(buffers_for_shuffle, numPartitions, i);
					g_buf->flush_end(file_name_str, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}

		int get_index_partition_vertices(int src) {

			int partition_id = src/ vertices_per_partition;
			return partition_id < (numPartitions - 1) ? partition_id : (numPartitions - 1);
		}

		int get_index_partition_edges(int src) {
			for(unsigned int i = 0; i < intervals.size(); i++) {
				std::pair<VertexId, VertexId> interval = intervals.at(i);
				if(src >= interval.first && src <= interval.second)
					return i;
			}

			return -1;
		}

	};
}



#endif /* UTILITY_PREPROCESSING_NEW_HPP_ */
