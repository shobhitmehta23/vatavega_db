#ifndef A2BTEST_DBFILEBASE_H_
#define A2BTEST_DBFILEBASE_H_

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "File.h"
#include "cstdlib"

#define PIPE_SIZE 100
#define WRITE_MODE 1
#define READ_MODE 0

typedef struct {
	OrderMaker * order_maker;
	int runlen;
}SortInfo;

class DBFileBase {
protected:
	File file;
	Page page;
	off_t current_page_index;

public:
	DBFileBase ();
	virtual ~DBFileBase();

	virtual int Create (const char *fpath, void *startup) = 0;
	virtual int Open (const char *fpath) = 0;
	virtual int Close () = 0;

	virtual void Load (Schema &myschema, const char *loadpath) = 0;

	virtual void MoveFirst () = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};



#endif /* A2BTEST_DBFILEBASE_H_ */
