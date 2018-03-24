#include "RelOp.h"
#include <sstream>

void *selectFile(void *thread_args);
void *selectPipe(void *thread_args);
void *project(void *thread_args);
void *sum(void *thread_args);

/*
 * ---------------------------SelectFile----------------------------------------
 */

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp,
		Record &literal) {

	//create a struc object to hold all the arguments to be passed to the thread.
	relOp_thread_arguments *args = new relOp_thread_arguments;
	args->inFile = &inFile;
	args->outPipe = &outPipe;
	args->selOp = &selOp;
	args->literal = &literal;

	//spawn the thread and pass the arguments.
	pthread_create(&thread, NULL, selectFile, (void *) args);

	delete args;

}

void SelectFile::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {
	this->runlen = runlen;
}

void *selectFile(void *thread_args) {
	relOp_thread_arguments *args;
	args = (relOp_thread_arguments *) thread_args;
	DBFile *inFile = args->inFile;
	Pipe *outPipe = args->outPipe;
	Record *literal = args->literal;
	CNF *selOp = args->selOp;

	Record* temp_rec = new Record;
	while (inFile->GetNext(*temp_rec, *selOp, *literal)) {
		outPipe->Insert(temp_rec);
	}
	delete temp_rec;
	//Shut down output pipe in the end.
	outPipe->ShutDown();
}
/*
 * ---------------------------SelectPipe----------------------------------------
 */

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	//create a struc object to hold all the arguments to be passed to the thread.
	relOp_thread_arguments *args = new relOp_thread_arguments;
	args->inPipe = &inPipe;
	args->outPipe = &outPipe;
	args->selOp = &selOp;
	args->literal = &literal;

	//spawn the thread and pass the arguments.
	pthread_create(&thread, NULL, selectPipe, (void *) args);

	delete args;
}

/*
 * Implementation of the 'Select pipe' operation.
 * This function will be executed by the thead in the Run method.
 */
void *selectPipe(void *thread_args) {
	relOp_thread_arguments *args;
	args = (relOp_thread_arguments *) thread_args;
	Pipe *inPipe = args->inPipe;
	Pipe *outPipe = args->outPipe;
	Record *literal = args->literal;
	CNF *selOp = args->selOp;

	ComparisonEngine compEngine;

	Record* temp_rec = new Record;
	while (inPipe->Remove(temp_rec)) {
		if (compEngine.Compare(temp_rec, literal, selOp)) {
			outPipe->Insert(temp_rec);
		}
	}
	delete temp_rec;

	//Shut down output pipe in the end.
	outPipe->ShutDown();
}

void SelectPipe::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen) {
	this->runlen = runlen;
}

/*
 * ---------------------------Project----------------------------------------
 */
void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
		int numAttsOutput) {
	//create a struc object to hold all the arguments to be passed to the thread.
	relOp_thread_arguments *args = new relOp_thread_arguments;
	args->inPipe = &inPipe;
	args->outPipe = &outPipe;
	args->keepMe = keepMe;
	args->numAttsInput = numAttsInput;
	args->numAttsOutput = numAttsOutput;

	//spawn the thread and pass the arguments.
	pthread_create(&thread, NULL, project, (void *) args);

	delete args;
}

void Project::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int runlen) {
	this->runlen = runlen;
}

void *project(void *thread_args) {
	relOp_thread_arguments *args;
	args = (relOp_thread_arguments *) thread_args;
	Pipe *inPipe = args->inPipe;
	Pipe *outPipe = args->outPipe;
	int* keepMe = args->keepMe;
	int numAttsInput = args->numAttsInput;
	int numAttsOutput = args->numAttsOutput;

	Record* temp_rec = new Record;
	while (inPipe->Remove(temp_rec)) {
		temp_rec->Project(keepMe, numAttsOutput, numAttsInput);
		outPipe->Insert(temp_rec);
	}
	delete temp_rec;

	//Shut down output pipe in the end.
	outPipe->ShutDown();
}

/*
 * ---------------------------DuplicateRemoval----------------------------------------
 */
void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
}
void DuplicateRemoval::WaitUntilDone() {
	pthread_join(thread, NULL);
}
void DuplicateRemoval::Use_n_Pages(int n) {
	this->runlen = runlen;
}

/*
 * ---------------------------Sum----------------------------------------
 */
void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	relOp_thread_arguments *args = new relOp_thread_arguments;
	args->inPipe = &inPipe;
	args->outPipe = &outPipe;
	args->computeMe = &computeMe;

	//spawn the thread and pass the arguments.
	pthread_create(&thread, NULL, sum, (void *) args);
}

void *sum(void *thread_args) {
	relOp_thread_arguments *args;
	args = (relOp_thread_arguments *) thread_args;
	Pipe *inPipe = args->inPipe;
	Pipe *outPipe = args->outPipe;
	Function *computeMe = args->computeMe;

	Record* temp_rec = new Record;
	int intResult;
	double doubleResult;
	int intSum = 0;
	double doubleSum = 0.0;
	Type type;
	while (inPipe->Remove(temp_rec)) {
		type = computeMe->Apply(*temp_rec, intResult, doubleResult);
		intSum += intResult;
		doubleSum += doubleResult;
	}
	delete temp_rec;
	Attribute attr[1];
	attr[0].myType = type;
	attr[0].name = "SUM";
	std::stringstream strStream;
	//strStream << (type == Int) ? intSum : doubleSum;
	if (type == Int) {
		strStream << intSum;
	} else {
		strStream << doubleSum;
	}
	strStream << "|";

	Schema outSchema("temp_sum_schema", 1, attr);
	//cout << "type*********" << type << endl;
//	cout << "string*********" << strStream.str() << endl;
//	cout << "doubleSum*********" << doubleSum << endl;

	Record out_rec;
	out_rec.ComposeRecord(&outSchema, strStream.str().c_str());
	outPipe->Insert(&out_rec);

//Shut down output pipe in the end.
	outPipe->ShutDown();
	delete args;

}
void Sum::WaitUntilDone() {
	pthread_join(thread, NULL);
}
void Sum::Use_n_Pages(int n) {
	this->runlen = runlen;
}

