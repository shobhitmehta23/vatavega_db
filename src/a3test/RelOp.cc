#include "RelOp.h"

void *selectFile(void *thread_args);
void *selectPipe(void *thread_args);

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
