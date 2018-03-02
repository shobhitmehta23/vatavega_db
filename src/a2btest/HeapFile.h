#ifndef A2BTEST_HEAPFILE_H_
#define A2BTEST_HEAPFILE_H_

#include "DBFileBase.h"

class HeapFile: public DBFileBase {

public:
	HeapFile();
	~HeapFile() {
	}

	int Create(const char *fpath, void *startup);
	int Open(const char *fpath);
	int Close();

	void Load(Schema &myschema, const char *loadpath);

	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

#endif /* A2BTEST_HEAPFILE_H_ */
