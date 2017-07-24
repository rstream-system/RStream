#ifndef PREPROC_HPP
#define PREPROC_HPP


#include <iostream>
#include "../common/RStreamCommon.hpp"

namespace RStream {
	class Preproc
	{
		std::string fname;
		int numParts;
		int numVerts;
		int vertsPerPart;
		int startVertex;
		int maxLineSize;	// len of largest line in input file
		bool edge_vals;
		bool undirected;
		int edge_unit;

		std::vector<int> vertLabels;

		public:
		inline int getVerPerPartition() { return vertsPerPart; }
		inline bool getEdgeType() { return edge_vals; }
		inline int getEdgeUnit() { return edge_unit; }

		Preproc(std::string& _fname, int _numVerts, int _numParts, bool _edge_vals, bool undirected) :
			fname(_fname), numVerts(_numVerts), numParts(_numParts), edge_vals(_edge_vals), undirected(_undirected)
		{
			vertLabels = std::vector<int>(numVerts, 0);
			vertsPerPart = numVerts / numParts;

			edge_unit = (edge_vals) ? sizeof(int) * 5 : sizeof(int) * 4;

			getVertValues();

			std::cout << "VPP: " << vertsPerPart <<  std::endl;
			std::cout << "START V: " << startVertex << std::endl;
			std::cout << "MAXSIZE: " << maxLineSize << "\n" << std::endl;

			inpToEdgeList();
			std::cout << "\nFinished generating partitions\n" << std::endl;

			std::cout << "Writing meta file..." << std::endl;
			write_meta_file();
			std::cout << "Meta file written\n" << std::endl;
		}

		void getVertValues()
		{
			// open main input file
			std::cout << "Getting vertex values..." << std::endl;
			FILE* input = fopen(fname.c_str(), "r");
			if (input == NULL) {
				std::cerr << "FILE NOT OPENED" << std::endl;
				return;
			}

			char line[1024], delims[] = "\t ";
			int vert;
			int val;
			int count = 0, size = 0, maxsize = 0;

			// read each line of file, getting first two values as 
			// vertex and vertex label
			while (fgets(line, 1024, input) != NULL) {
				int len = strlen(line);
				if (size == 0) {
					vert = std::stoi(strtok(line, delims));
					val = std::stoi(strtok(NULL, delims));
					
					if (count == 0) startVertex = vert;
					vertLabels[count++] = val;
				}

				size += len;
				if (line[len-1] == '\n') {
					if (size > maxsize) maxsize = size;

					size = 0;
				}
			}

			maxLineSize = maxsize;

			fclose(input);
			std::cout << "Vertex values saved\n" << std::endl;
		}

		void inpToEdgeList()
		{
			// Open input file
			FILE* input = fopen(fname.c_str(), "r");
			if (input == NULL) {
				std::cerr << "INPUT FILE NOT OPENED" << std::endl;
				return;
			}
			else std::cout << "Opening input file..." << std::endl;

			// Open initial output file
			int count = 0, part = 0;
			std::string name = fname + ".binary." + std::to_string((long long)part);
			FILE* output = fopen(name.c_str(), "wb");
			if (output == NULL) {
				std::cerr << "OUTPUT FILE NOT OPENED" << std::endl;
				return;
			}
			else std::cout << "Opening binary file " + std::to_string((long long)part) << std::endl;

			// Begin reading input
			char* adj_list = new char[maxLineSize+1];
			while (fgets(adj_list, maxLineSize+1, input) != NULL) {
				int len = strlen(adj_list);
				if (count > vertsPerPart) {		// if number of verts reached
					fclose(output);				// close file and make new partition
					std::cout << "Closing binary file..." << std::endl;
					name = fname + ".binary." + std::to_string((long long)++part);
					output = fopen(name.c_str(), "wb");
					if (output == NULL) {
						std::cerr << "OUTPUT FILE NOT OPENED" << std::endl;
						return;
					}
					else std::cout << "Opening binary file " + std::to_string((long long)part) << std::endl;
					count = 0;
				} 

				// read source and source label
				adj_list[len-1] = 0;
				char delims[] = "\t ";
				char *strp = strtok(adj_list, delims);
				int src = std::stoi(strp);
				strp = strtok(NULL, delims);
				int srcLab = std::stoi(strp);

				// read outgoing edges and edge labels
				// and write them to binary file
				while ((strp = strtok(NULL, delims)) != NULL) {
					int tgt = std::stoi(strp);
					int edgeVal = -1;

					if (edge_vals) {		// if there are edge values read them
						strp = strtok(NULL, delims);
						edgeVal = std::stoi(strp);
					}

					int tgtLab = vertLabels[tgt - startVertex];
					writeToFile(output, src, srcLab, tgt, tgtLab, edgeVal);
				}

				count++;
			}

			delete[] adj_list;
			fclose(input);
			fclose(output);

			std::cout << "Closing binary file..." << std::endl;
			std::cout << "Closing input file..." << std::endl;
		}

		void writeToFile(FILE* output, int src, int srcLab, int tgt, int tgtLab, int edgeVal)
		{
			fwrite((const void*) &src, sizeof(int), 1, output);
			fwrite((const void*) &srcLab, sizeof(int), 1, output);
			fwrite((const void*) &tgt, sizeof(int), 1, output);
			fwrite((const void*) &tgtLab, sizeof(int), 1, output);
			if (edge_vals) fwrite((const void*) &edgeVal, sizeof(int), 1, output);
		}

		void write_meta_file()
		{
			std::ofstream meta_file(fname +".meta");
			if (meta_file.is_open()) {
				meta_file << edge_vals << "\t" << edge_unit << std::endl;

				int start = startVertex, end = 0;
				for (int i = 0; i < numParts; i++) {
					if (i == numParts-1) {
						end = numVerts - (1 - startVertex);
						meta_file << start << "\t" << end << std::endl;
					}
					else {
						end = start + vertsPerPart;
						meta_file << start << "\t" << end << std::endl;
						start = end + 1;
					}
				}
			}
			else std::cerr << "Couldn't open meta file!" << std::endl;

			meta_file.close();
		}
	};
}

#endif
