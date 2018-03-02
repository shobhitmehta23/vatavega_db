#ifndef A2BTEST_SORTEDFILE_H_
#define A2BTEST_SORTEDFILE_H_

#include "DBFileBase.h"
#include <iostream>
#include <fstream>
#include "Pipe.h"

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

private:
	Pipe * input_pipe;
	Pipe * output_pipe;
	int runlen = 0;
	OrderMaker *order_maker;
	int mode;

	void merge();
};

#endif /* A2BTEST_SORTEDFILE_H_ */
