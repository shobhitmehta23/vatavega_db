#ifndef A2BTEST_SORTEDFILE_H_
#define A2BTEST_SORTEDFILE_H_

#include "DBFileBase.h"
#include <iostream>
#include <fstream>
#include "Pipe.h"
#include "BigQ.h"

using namespace std;

class SortedFile: public DBFileBase {
public:
	SortedFile();
	~SortedFile();

	int Create(const char *fpath, void *startup);
	int Open(const char *fpath);
	int Close();

	void Load(Schema &myschema, const char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);
	void reinitialize_bigQ();

private:
	Pipe * input_pipe;
	Pipe * output_pipe;
	BigQ * sorting_queue;
	int runlen;
	OrderMaker *order_maker;
	int mode;
	void twoWayMerge();
	bool binary_search(Record& fetchMe, Record& literal,
						OrderMaker * query_order_maker,
						OrderMaker * cnf_literal_order_maker);
};

#endif /* A2BTEST_SORTEDFILE_H_ */
