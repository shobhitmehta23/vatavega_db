#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone() = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages(int n) = 0;
};

typedef struct relOp_thread_arguments {
	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	Record *literal;
	DBFile *inFile;
	Schema *mySchema;
	int *keepMe;
	FILE *outFile;
	int numAttsInput;
	int numAttsOutput;
	Function* computeMe;
	int runlen;
	OrderMaker * orderMaker;
	Pipe *inPipeR;
} relOp_thread_arguments;

class SelectFile: public RelationalOp {

private:
	pthread_t thread;
	int runlen = 1;
	// Record *buffer;

public:

	void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);

};

class SelectPipe: public RelationalOp {
private:
	pthread_t thread;
	int runlen = 1;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Project: public RelationalOp {
private:
	pthread_t thread;
	int runlen = 1;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
			int numAttsOutput);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Join: public RelationalOp {
private:
	pthread_t thread;
	int runlen = 1;
public:
	void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
			Record &literal);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class DuplicateRemoval: public RelationalOp {
private:
	pthread_t thread;
	int runlen = 1;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class Sum: public RelationalOp {
private:
	pthread_t thread;
	int runlen = 1;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class GroupBy: public RelationalOp {
private:
	pthread_t thread;
	int runlen = 1;
public:
	void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
			Function &computeMe);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};

class WriteOut: public RelationalOp {
private:
	pthread_t thread;
	int runlen = 1;
public:
	void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone();
	void Use_n_Pages(int n);
};
#endif
