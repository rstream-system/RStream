/*
 * scatter.hpp
 *
 *  Created on: Aug 4, 2017
 *      Author: kai
 */

#ifndef CORE_SCATTER_HPP_
#define CORE_SCATTER_HPP_

#include "engine.hpp"
#include "meta_info.hpp"
#include "concurrent_set.hpp"
#include "concurrent_vector.hpp"

namespace RStream {
	template <typename VertexDataType, typename UpdateType>
	class Scatter {

	static_assert(
				std::is_base_of<BaseVertex, VertexDataType>::value,
				"VertexDataType must be a subclass of BaseVertex."
			);

			static_assert(
				std::is_base_of<BaseUpdate, UpdateType>::value,
				"UpdateType must be a subclass of BaseUpdate."
			);

	public:
		Scatter(Engine & e): context(e) {}
		virtual ~Scatter() {}

		/* scatter with vertex data (for graph computation use)*/
		Update_Stream scatter_with_vertex(std::function<UpdateType*(Edge*, VertexDataType*)> generate_one_update) {
			atomic_init();

			Logger::print_thread_info_locked("--------------------Start Scatter Phase--------------------\n\n");

			Update_Stream update_c = Engine::update_count++;

			// a pair of <vertex, edge_stream> for each partition
//			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);
//
//			// push task into concurrent queue
//			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
//				task_queue->push(partition_id);
//			}

			std::vector<std::pair<long, std::tuple<int, long, long>>> tasks;
			concurrent_queue<std::tuple<int, long, long>> * task_queue = new concurrent_queue<std::tuple<int, long, long>>(MAX_QUEUE_SIZE);

			// divide in update stream into smaller chuncks, to get better workload balance
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				long edge_file_size = io_manager::get_filesize(fd_edge);
				int streaming_counter = edge_file_size / (IO_SIZE * sizeof(Edge)) + 1;
				assert((edge_file_size % sizeof(Edge)) == 0);

				long valid_io_size = 0;
				long offset = 0;

				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						valid_io_size = edge_file_size - IO_SIZE * sizeof(Edge) * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE * sizeof(Edge);

					tasks.push_back(std::make_pair(valid_io_size, std::make_tuple(partition_id, offset, valid_io_size)));
					offset += valid_io_size;
				}

			}

			// sort tasks on size, larger size has a higher priority to run
			std::sort(tasks.begin(), tasks.end());
			for(int i = tasks.size() - 1; i >= 0; i--) {
				task_queue->push(tasks.at(i).second);
			}

			// allocate global buffers for shuffling
			global_buffer<UpdateType> ** buffers_for_shuffle = buffer_manager<UpdateType>::get_global_buffers(context.num_partitions);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->scatter_producer_with_vertex(generate_one_update, buffers_for_shuffle, task_queue); } ));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&Scatter::scatter_consumer, this, buffers_for_shuffle, update_c));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			Logger::print_thread_info_locked("--------------------Finish Scatter Phase-------------------\n\n");

			return update_c;
		}

		/* scatter without vertex data (for relational algebra use)*/
		Update_Stream scatter_no_vertex(std::function<UpdateType*(Edge*)> generate_one_update) {
			atomic_init();

			Logger::print_thread_info_locked("--------------------Start Scatter Phase--------------------\n\n");

			Update_Stream update_c = Engine::update_count++;

			concurrent_queue<int> * task_queue = new concurrent_queue<int>(context.num_partitions);

			// allocate global buffers for shuffling
			global_buffer<UpdateType> ** buffers_for_shuffle = buffer_manager<UpdateType>::get_global_buffers(context.num_partitions);

			// push task into concurrent queue
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				task_queue->push(partition_id);
			}

	//			//for debugging only
	//			scatter_producer(generate_one_update, buffers_for_shuffle, task_queue);
	//			std::cout << "scatter done!" << std::endl;
	//			scatter_consumer(buffers_for_shuffle);

			// exec threads will produce updates and push into shuffle buffers
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back(std::thread([=] { this->scatter_producer_no_vertex(generate_one_update, buffers_for_shuffle, task_queue); }));

			// write threads will flush shuffle buffer to update out stream file as long as it's full
			std::vector<std::thread> write_threads;
			for(int i = 0; i < context.num_write_threads; i++)
				write_threads.push_back(std::thread(&Scatter::scatter_consumer, this, buffers_for_shuffle, update_c));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

			for(auto &t : write_threads)
				t.join();

			delete[] buffers_for_shuffle;
			delete task_queue;

			Logger::print_thread_info_locked("--------------------Finish Scatter Phase-------------------\n\n");

			return update_c;
		}

	private:
		void atomic_init() {
			atomic_num_producers = context.num_exec_threads;
			atomic_partition_number = context.num_partitions;
		}

		void load_vertices_hashMap(char* vertex_local_buf, const int vertex_file_size, std::unordered_map<VertexId, VertexDataType*> & vertex_map) {
			for(size_t off = 0; off < vertex_file_size; off += context.vertex_unit){
				VertexDataType* v = reinterpret_cast<VertexDataType*>(vertex_local_buf + off);
				vertex_map[v->id] = v;
			}
		}

//		void scatter_producer_with_vertex(std::function<UpdateType*(Edge*, VertexDataType*)> generate_one_update,
//						global_buffer<UpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {

		void scatter_producer_with_vertex(std::function<UpdateType*(Edge*, VertexDataType*)> generate_one_update,
								global_buffer<UpdateType> ** buffers_for_shuffle, concurrent_queue<std::tuple<int, long, long>> * task_queue) {

//			int partition_id = -1;
			VertexId vertex_start = -1;
			assert(context.vertex_unit == sizeof(VertexDataType));

			int partition_id = -1;
			long chunk_offset = 0, chunk_size = 0;
			auto one_task = std::make_tuple(partition_id, chunk_offset, chunk_size);

			// pop from queue
//			while(task_queue->test_pop_atomic(partition_id)){
			while(task_queue->test_pop_atomic(one_task)){
				partition_id = std::get<0>(one_task);
				chunk_offset = std::get<1>(one_task);
				chunk_size = std::get<2>(one_task);


				int fd_vertex = open((context.filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDONLY);
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_vertex > 0 && fd_edge > 0 );

				// get start vertex id
				vertex_start = context.vertex_intervals[partition_id].first;
				assert(vertex_start >= 0 && vertex_start < context.num_vertices);

				// get file size
				long vertex_file_size = io_manager::get_filesize(fd_vertex);
//				long edge_file_size = io_manager::get_filesize(fd_edge);

//				Logger::print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + " of size " + std::to_string(edge_file_size) + "\n");

				// vertex data fully loaded into memory
				char * vertex_local_buf = new char[vertex_file_size];
				io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size, 0);
				std::unordered_map<VertexId, VertexDataType*> vertex_map;
				load_vertices_hashMap(vertex_local_buf, vertex_file_size, vertex_map);

				// streaming edges
				char * edge_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE * sizeof(Edge));
//				int streaming_counter = edge_file_size / (IO_SIZE * sizeof(Edge)) + 1;
				int streaming_counter = chunk_size / (IO_SIZE * sizeof(Edge)) + 1;
				assert((chunk_size % sizeof(Edge)) == 0);

				long valid_io_size = 0;
				long offset = 0;
				int edge_unit = context.edge_unit;

				assert(edge_unit == sizeof(Edge));

				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {

					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
	//						valid_io_size = edge_file_size - IO_SIZE * (streaming_counter - 1);
//						valid_io_size = edge_file_size - IO_SIZE * sizeof(Edge) * (streaming_counter - 1);
						valid_io_size = chunk_size - IO_SIZE * sizeof(Edge) * (streaming_counter - 1);
					else
	//						valid_io_size = IO_SIZE;
						valid_io_size = IO_SIZE * sizeof(Edge);

					assert(valid_io_size % edge_unit == 0);

	//					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size);
//					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size, offset);
					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size, chunk_offset + offset);

					offset += valid_io_size;

					// for each streaming
					for(long pos = 0; pos < valid_io_size; pos += edge_unit) {
						// get an edge
						Edge * e = (Edge*)(edge_local_buf + pos);
	//					std::cout << e << std::endl;

						// get src vertex in vertex buf
						size_t offset = (e->src - vertex_start) * sizeof(VertexDataType);
						VertexDataType* src_vertex = reinterpret_cast<VertexDataType*>(vertex_local_buf + offset);

						UpdateType * update_info = generate_one_update(e, src_vertex);
	//					std::cout << update_info->target << std::endl;

						// insert into shuffle buffer accordingly
						int index = meta_info::get_index(update_info->target, context);
						global_buffer<UpdateType>* global_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
						global_buf->insert(update_info, index);

						delete update_info;
					}
				}

				// delete
				delete[] vertex_local_buf;
				free(edge_local_buf);

	//				//clear vertex_map
	//				for(auto it = vertex_map.cbegin(); it != vertex_map.cend(); ++it){
	//					delete it->second;
	//				}

				close(fd_vertex);
				close(fd_edge);

			}
			atomic_num_producers--;
		}

		void scatter_producer_no_vertex(std::function<UpdateType*(Edge*)> generate_one_update,
						global_buffer<UpdateType> ** buffers_for_shuffle, concurrent_queue<int> * task_queue) {
			int partition_id = -1;

			for(unsigned int i = 0; i < context.num_partitions; i++) {
				assert(buffers_for_shuffle[i]->get_capacity() == BUFFER_CAPACITY);
			}

			// pop from queue
			while(task_queue->test_pop_atomic(partition_id)){
				int fd = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd > 0);

				// get file size
				long file_size = io_manager::get_filesize(fd);
				Logger::print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + " of size " + std::to_string(file_size) + "\n");

				// streaming edges
				char * local_buf = (char*)memalign(PAGE_SIZE, IO_SIZE * sizeof(Edge));
				int streaming_counter = file_size / (IO_SIZE * sizeof(Edge)) + 1;

				assert((file_size % sizeof(Edge)) == 0);

				long valid_io_size = 0;
				long offset = 0;
				int edge_unit = context.edge_unit;

				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {

					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
	//						valid_io_size = file_size - IO_SIZE * (streaming_counter - 1);
						valid_io_size = file_size - IO_SIZE * sizeof(Edge) * (streaming_counter - 1);
					else
	//						valid_io_size = IO_SIZE;
						valid_io_size = IO_SIZE * sizeof(Edge);

					assert(valid_io_size % edge_unit == 0);

					io_manager::read_from_file(fd, local_buf, valid_io_size, offset);
					offset += valid_io_size;

					// for each streaming
					for(long pos = 0; pos < valid_io_size; pos += edge_unit) {
						// get an edge
						Edge * e = (Edge*)(local_buf + pos);
	//					std::cout << e << std::endl;

						// gen one update
						UpdateType * update_info = generate_one_update(e);
	//					std::cout << update_info->target << std::endl;

						int index = meta_info::get_index(update_info->target, context);
						assert(index >= 0);

						global_buffer<UpdateType>* global_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, index);
						global_buf->insert(update_info, index);

						delete update_info;
					}
				}

				free(local_buf);
				close(fd);

			}
			atomic_num_producers--;
		}

		void scatter_consumer(global_buffer<UpdateType> ** buffers_for_shuffle, Update_Stream update_count) {
			int counter = 0;

			while(atomic_num_producers != 0) {
				if(counter == context.num_partitions)
					counter = 0;

				int i = counter++;

//				const char * file_name = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count)).c_str();
				std::string file_name_str = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count));

				global_buffer<UpdateType>* g_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name_str, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;
				if(i >= 0){

					std::string file_name_str = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count));

					global_buffer<UpdateType>* g_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
					g_buf->flush_end(file_name_str, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}


		//============================================================================== Zhiqiang ======================================================================
public:
		void prune_graph(std::function<bool(VertexDataType&)> filter_vertex, std::string& out_file) {
//			Logger::print_thread_info_locked("--------------------Start Prune Phase--------------------\n\n");
			std::cout << Logger::generate_log_del("\n\n--------------------Start Prune Phase --------------------\n", 2);

			//----------------------------------------------
			//load vertices
			//load all edges in memory
//			Logger::print_thread_info_locked("--------------------Start Vertex Loading --------------------\n\n");
			std::cout << Logger::generate_log_del("\n\n--------------------Start Vertex Loading --------------------\n", 2);
			concurrent_queue<int> * read_task_queue = new concurrent_queue<int>(context.num_partitions);
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				read_task_queue->push(partition_id);
			}
			concurrent_set<VertexId>* vertices = new concurrent_set<VertexId>();
			std::vector<std::thread> read_threads;
			for(int i = 0; i < context.num_threads; i++)
				read_threads.push_back( std::thread([=] { this->vertices_loader(filter_vertex, vertices, read_task_queue); } ));

			for(auto &t : read_threads)
				t.join();

			delete read_task_queue;
			//----------------------------------------------

//			Logger::print_thread_info_locked("--------------------Start Edge Pruning --------------------\n");
//			Logger::print_thread_info_locked("#vertices: \t" + std::to_string(vertices->set.size()) + "\n\n");
			std::cout << Logger::generate_log_del("\n\n--------------------Start Edge Pruning --------------------\n", 2);
			std::cout << Logger::generate_log_del("#vertices: \t" + std::to_string(vertices->set.size()), 2);

			std::vector<std::pair<long, std::tuple<int, long, long>>> tasks;
			concurrent_queue<std::tuple<int, long, long>> * task_queue = new concurrent_queue<std::tuple<int, long, long>>(MAX_QUEUE_SIZE);

			// divide in update stream into smaller chuncks, to get better workload balance
			for(int partition_id = 0; partition_id < context.num_partitions; partition_id++) {
				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				long edge_file_size = io_manager::get_filesize(fd_edge);
				int streaming_counter = edge_file_size / (IO_SIZE * sizeof(Edge)) + 1;
				assert((edge_file_size % sizeof(Edge)) == 0);

				long valid_io_size = 0;
				long offset = 0;

				for(int counter = 0; counter < streaming_counter; counter++) {
					// last streaming
					if(counter == streaming_counter - 1)
						valid_io_size = edge_file_size - IO_SIZE * sizeof(Edge) * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE * sizeof(Edge);

					tasks.push_back(std::make_pair(valid_io_size, std::make_tuple(partition_id, offset, valid_io_size)));
					offset += valid_io_size;
				}

			}

			// sort tasks on size, larger size has a higher priority to run
			std::sort(tasks.begin(), tasks.end());
			for(int i = tasks.size() - 1; i >= 0; i--) {
				task_queue->push(tasks.at(i).second);
			}

			// allocate global buffers for shuffling
//			global_buffer<Edge> ** buffers_for_shuffle = buffer_manager<Edge>::get_global_buffers(context.num_partitions);

			// exec threads will produce updates and push into shuffle buffers

			concurrent_vector<Edge>* edges = new concurrent_vector<Edge>();
			std::vector<std::thread> exec_threads;
			for(int i = 0; i < context.num_exec_threads; i++)
				exec_threads.push_back( std::thread([=] { this->prune_graph_producer(task_queue, vertices, edges); } ));

			// join all threads
			for(auto & t : exec_threads)
				t.join();

//			delete[] buffers_for_shuffle;
			delete task_queue;
			delete vertices;

			//==============================================================
//			Logger::print_thread_info_locked("--------------------Start Converting to Adj-list --------------------\n");
//			Logger::print_thread_info_locked("#edges: \t" + std::to_string(edges->vector.size()) + "\n\n");
			std::cout << Logger::generate_log_del("\n\n--------------------Start Converting to Adj-list --------------------\n", 2);
			std::cout << Logger::generate_log_del("#edges: \t" + std::to_string(edges->vector.size()), 2);


			std::map<VertexId, std::unordered_set<VertexId>> adj_list;

			std::unordered_map<VertexId, VertexId> map;
			int index = 0;

			for(auto it = edges->vector.begin(); it != edges->vector.end(); ++it){

				VertexId src_old = it->src;
				VertexId src;

				VertexId dst_old = it->target;
				VertexId dst;

//				std::cout << "old: " << src_old << ", " << dst_old << std::endl;
//
				auto it_m = map.find(src_old);
				if(it_m != map.end()){
					src = it_m->second;
				}
				else{
					src = index;
					map[src_old] = index++;
				}

				it_m = map.find(dst_old);
				if(it_m != map.end()){
					dst = it_m->second;
				}
				else{
					dst = index;
					map[dst_old] = index++;
				}
//
//				std::cout << "new: " << src << ", " << dst<< std::endl;

					//				it = edges->vector.erase(it);

				if(adj_list.find(src) != adj_list.end()){
					adj_list[src].insert(dst);
				}
				else{
					std::unordered_set<VertexId> v;
					v.insert(dst);
					adj_list[src] = v;
				}

				if(adj_list.find(dst) != adj_list.end()){
					adj_list[dst].insert(src);
				}
				else{
					std::unordered_set<VertexId> v;
					v.insert(src);
					adj_list[dst] = v;
				}
			}

			delete edges;

//			Logger::print_thread_info_locked("--------------------Start Writing to file --------------------\n\n");
			std::cout << Logger::generate_log_del("\n\n--------------------Start Writing to file --------------------\n", 2);
			write_to_file(adj_list, out_file);


//			Logger::print_thread_info_locked("--------------------Finish Prune Phase-------------------\n\n");
			std::cout << Logger::generate_log_del("\n\n--------------------Finish Prune Phase --------------------\n", 2);
		}

		void write_to_file(std::map<VertexId, std::unordered_set<VertexId>> adj_list, std::string& out_file){
			std::ofstream o_file;
			o_file.open(out_file);

			for(auto it = adj_list.begin(); it != adj_list.end(); ++it){
				o_file << std::to_string(it->first) << " 0 ";
				for(auto it_v = it->second.begin(); it_v != it->second.end(); ++it_v){
					o_file << std::to_string(*it_v) << " ";
				}
				o_file << "\n";
			}

			o_file.close();
		}


		void vertices_loader(std::function<bool(VertexDataType&)> filter_vertex, concurrent_set<VertexId>* vertices, concurrent_queue<int> * read_task_queue){
			int partition_id = -1;
			while(read_task_queue->test_pop_atomic(partition_id)){
				int fd_vertex = open((context.filename + "." + std::to_string(partition_id) + ".vertex").c_str(), O_RDONLY);
				assert(fd_vertex > 0);
				long vertex_file_size = io_manager::get_filesize(fd_vertex);

				// edges are fully loaded into memory
				char * vertex_local_buf = (char *)malloc(vertex_file_size);
				io_manager::read_from_file(fd_vertex, vertex_local_buf, vertex_file_size, 0);

//				build_vertex_set(vertex_local_buf, vertices, vertex_file_size, 0);
				for(size_t pos = 0; pos < vertex_file_size; pos += context.vertex_unit) {
					// get a labeled edge
					VertexDataType v = *(VertexDataType*)(vertex_local_buf + pos);
					// e.src is the key
					if(!filter_vertex(v)){
						vertices->insert_atomic(((BaseVertex)v).id);
					}
				}

				free(vertex_local_buf);
				close(fd_vertex);
			}
		}


		void prune_graph_producer(concurrent_queue<std::tuple<int, long, long>> * task_queue, concurrent_set<VertexId>* vertices, concurrent_vector<Edge>* edges) {
			VertexId vertex_start = -1;
			assert(context.vertex_unit == sizeof(VertexDataType));

			int partition_id = -1;
			long chunk_offset = 0, chunk_size = 0;
			auto one_task = std::make_tuple(partition_id, chunk_offset, chunk_size);

			// pop from queue
			while(task_queue->test_pop_atomic(one_task)){
				partition_id = std::get<0>(one_task);
				chunk_offset = std::get<1>(one_task);
				chunk_size = std::get<2>(one_task);

				int fd_edge = open((context.filename + "." + std::to_string(partition_id)).c_str(), O_RDONLY);
				assert(fd_edge > 0 );

//				Logger::print_thread_info_locked("as a producer dealing with partition " + std::to_string(partition_id) + " of size " + std::to_string(edge_file_size) + "\n");
				int target_partition = 0;

				// streaming edges
				char * edge_local_buf = (char *)memalign(PAGE_SIZE, IO_SIZE * sizeof(Edge));
				int streaming_counter = chunk_size / (IO_SIZE * sizeof(Edge)) + 1;
				assert((chunk_size % sizeof(Edge)) == 0);

				long valid_io_size = 0;
				long offset = 0;
				int edge_unit = context.edge_unit;

				assert(edge_unit == sizeof(Edge));

				// for all streaming
				for(int counter = 0; counter < streaming_counter; counter++) {

					// last streaming
					if(counter == streaming_counter - 1)
						// TODO: potential overflow?
						valid_io_size = chunk_size - IO_SIZE * sizeof(Edge) * (streaming_counter - 1);
					else
						valid_io_size = IO_SIZE * sizeof(Edge);

					assert(valid_io_size % edge_unit == 0);

					io_manager::read_from_file(fd_edge, edge_local_buf, valid_io_size, chunk_offset + offset);

					offset += valid_io_size;

					// for each streaming
					for(long pos = 0; pos < valid_io_size; pos += edge_unit) {
						// get an edge
						Edge * e = (Edge*)(edge_local_buf + pos);
	//					std::cout << e << std::endl;

						if(vertices->contains(e->src) && vertices->contains(e->target)){
							edges->insert_atomic(*e);
						}
					}
				}

				// delete
				free(edge_local_buf);
				close(fd_edge);

			}
			atomic_num_producers--;
		}


		void prune_consumer(global_buffer<Edge> ** buffers_for_shuffle, Update_Stream update_count) {
			int counter = 0;

			while(atomic_num_producers != 0) {
				if(counter == context.num_partitions)
					counter = 0;

				int i = counter++;

				std::string file_name_str = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count));

				global_buffer<UpdateType>* g_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
				g_buf->flush(file_name_str, i);
			}

			//the last run - deal with all remaining content in buffers
			while(true){
				int i = --atomic_partition_number;
				if(i >= 0){

					std::string file_name_str = (context.filename + "." + std::to_string(i) + ".update_stream_" + std::to_string(update_count));

					global_buffer<UpdateType>* g_buf = buffer_manager<UpdateType>::get_global_buffer(buffers_for_shuffle, context.num_partitions, i);
					g_buf->flush_end(file_name_str, i);

					delete g_buf;
				}
				else{
					break;
				}
			}
		}

		//============================================================================== Zhiqiang ======================================================================
















		const Engine& context;
		std::atomic<int> atomic_num_producers;
		std::atomic<int> atomic_partition_number;


	};












}



#endif /* CORE_SCATTER_HPP_ */
