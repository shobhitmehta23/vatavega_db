#include "RelOp.h"
#include <sstream>
#include "BigQ.h"

//Method declarations:
void *selectFile(void *thread_args);
void *selectPipe(void *thread_args);
void *project(void *thread_args);
void *sum(void *thread_args);
void *removeDuplicate(void *thread_args);
void *writeOut(void *thread_args);
void *groupBy(void *thread_args);
void insertGroupByRecord(OrderMaker* order_maker, Type type, int intSum,
		double doubleSum, Record *prev_rec, Pipe* outPipe);
void *join(void *thread_args);

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
	delete args;
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
	delete args;
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
	delete args;
}

/*
 * ---------------------------WriteOut----------------------------------------
 */

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	//create a struc object to hold all the arguments to be passed to the thread.
	relOp_thread_arguments *args = new relOp_thread_arguments;
	args->inPipe = &inPipe;
	args->outFile = outFile;
	args->mySchema = &mySchema;

	//spawn the thread and pass the arguments.
	pthread_create(&thread, NULL, writeOut, (void *) args);
}

void *writeOut(void *thread_args) {
	relOp_thread_arguments *args;
	args = (relOp_thread_arguments *) thread_args;
	Pipe *inPipe = args->inPipe;
	FILE *outFile = args->outFile;
	Schema *mySchema = args->mySchema;

	Record* temp_rec = new Record;
	while (inPipe->Remove(temp_rec)) {
		temp_rec->Print(mySchema, outFile);
	}
	delete temp_rec;
	delete args;
}

void WriteOut::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int runlen) {
	this->runlen = runlen;
}

/*
 * ---------------------------DuplicateRemoval----------------------------------------
 */
void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	//create a struc object to hold all the arguments to be passed to the thread.
	relOp_thread_arguments *args = new relOp_thread_arguments;
	args->inPipe = &inPipe;
	args->outPipe = &outPipe;
	args->runlen = this->runlen;
	args->mySchema = &mySchema;

	//spawn the thread and pass the arguments.
	pthread_create(&thread, NULL, removeDuplicate, (void *) args);

}
void DuplicateRemoval::WaitUntilDone() {
	pthread_join(thread, NULL);
}
void DuplicateRemoval::Use_n_Pages(int n) {
	this->runlen = n;
}

void *removeDuplicate(void *thread_args) {
	relOp_thread_arguments *args;
	args = (relOp_thread_arguments *) thread_args;
	Pipe *inPipe = args->inPipe;
	Pipe *outPipe = args->outPipe;
	Schema *mySchema = args->mySchema;
	int runlen = args->runlen;
	OrderMaker * order_maker = new OrderMaker(mySchema);
	Pipe *tempPipe = new Pipe(outPipe->getBufferSize());
	BigQ *bigQ = new BigQ(*inPipe, *tempPipe, *order_maker, runlen);

	Record * current_rec = new Record;
	Record * prev_rec = NULL;
	Record * current_rec_copy = new Record;

	while (tempPipe->Remove(current_rec)) {
		if (prev_rec == NULL) {
			current_rec_copy->Copy(current_rec);
			outPipe->Insert(current_rec);
		} else {
			ComparisonEngine comparison_engine;
			if (comparison_engine.Compare(current_rec, prev_rec, order_maker)) {
				current_rec_copy->Copy(current_rec);
				outPipe->Insert(current_rec);
			}
		}

		prev_rec = current_rec_copy;
	}

	delete current_rec;
	delete current_rec_copy;

	//Shut down output pipe in the end.
	outPipe->ShutDown();
	delete order_maker;
	delete tempPipe;
	delete bigQ;
	delete args;
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
	this->runlen = n;
}

/*
 * ---------------------------GroupBy----------------------------------------
 */

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
		Function &computeMe) {
	//create a struc object to hold all the arguments to be passed to the thread.
	relOp_thread_arguments *args = new relOp_thread_arguments;
	args->inPipe = &inPipe;
	args->outPipe = &outPipe;
	args->computeMe = &computeMe;
	args->orderMaker = &groupAtts;
	args->runlen = this->runlen;

	//spawn the thread and pass the arguments.
	pthread_create(&thread, NULL, groupBy, (void *) args);
}

void *groupBy(void *thread_args) {
	relOp_thread_arguments *args;
	args = (relOp_thread_arguments *) thread_args;
	Pipe *inPipe = args->inPipe;
	Pipe *outPipe = args->outPipe;
	Function *computeMe = args->computeMe;
	OrderMaker *order_maker = args->orderMaker;
	int runlen = args->runlen;
	Pipe * tempPipe = new Pipe(outPipe->getBufferSize());
	BigQ * sorting_queue = new BigQ(*inPipe, *tempPipe, *order_maker, runlen);

	Record * current_rec = new Record;
	Record * prev_rec = NULL;
	Record * current_rec_copy = new Record;

	int intResult;
	double doubleResult;
	int intSum = 0;
	double doubleSum = 0.0;
	Type type;

	while (tempPipe->Remove(current_rec)) {
		if (prev_rec == NULL) {
			current_rec_copy->Copy(current_rec);
			//outPipe->Insert(current_rec);
		} else {
			ComparisonEngine comparison_engine;
			if (comparison_engine.Compare(current_rec, prev_rec, order_maker)) {
				current_rec_copy->Copy(current_rec);
				//construct a new record for the group and insert it in the out pipe.
				insertGroupByRecord(order_maker, type, intSum, doubleSum,
						prev_rec, outPipe);

				intSum = 0;
				doubleSum = 0.0;
			}
		}

		type = computeMe->Apply(*current_rec, intResult, doubleResult);
		intSum += intResult;
		doubleSum += doubleResult;

		prev_rec = current_rec_copy;
	}
	//construct a new record for the LAST group and insert it in the out pipe.
	insertGroupByRecord(order_maker, type, intSum, doubleSum, prev_rec,
			outPipe);
	outPipe->ShutDown();

	delete sorting_queue;
	delete tempPipe;
	delete current_rec;
	delete current_rec_copy;
	delete args;
}
/*
 * constructs a new record for the group and insert it in the out pipe.
 */
void insertGroupByRecord(OrderMaker* order_maker, Type type, int intSum,
		double doubleSum, Record *prev_rec, Pipe* outPipe) {
	int numGrpByAttr = order_maker->getNumberOfAttributes();
	int* grpByAttrs = order_maker->getAttributes();
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

	Record sum_rec;
	sum_rec.ComposeRecord(&outSchema, strStream.str().c_str());

	Record* out_rec = new Record;
	int attributes[1 + numGrpByAttr];
	int sum[1] = { 0 };
	copy(sum, sum + 1, attributes);
	copy(grpByAttrs, grpByAttrs + numGrpByAttr, attributes + 1);
	out_rec->MergeRecords(&sum_rec, prev_rec, 1, numGrpByAttr, attributes,
			1 + numGrpByAttr, 1);

	//construct a new record for the group and insert it in the out pipe.
	outPipe->Insert(out_rec);
	delete grpByAttrs;
}

void GroupBy::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int runlen) {
	this->runlen = runlen;
}
/*
 * ---------------------------Join----------------------------------------
 */

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp,
		Record &literal) {
	relOp_thread_arguments *args = new relOp_thread_arguments;
	args->inPipe = &inPipeL;
	args->inPipeR = &inPipeR;
	args->outPipe = &outPipe;
	args->selOp = &selOp;
	args->literal = &literal;
	args->runlen = runlen;

	//spawn the thread and pass the arguments.
	pthread_create(&thread, NULL, join, (void *) args);
}

void *join(void *thread_args) {
	relOp_thread_arguments *args;
	args = (relOp_thread_arguments *) thread_args;
	Pipe *inPipeL = args->inPipe;
	Pipe *inPipeR = args->inPipeR;
	Pipe *outPipe = args->outPipe;
	CNF *selOp = args->selOp;
	Record *literal = args->literal;
	int runlen = args->runlen;

	int creatKeepAttributeArray = 1;
	int *keepAttributeArray;

	ComparisonEngine comp_engine;

	OrderMaker left_order_maker;
	OrderMaker right_order_maker;
	int numberOfArttrs = selOp->GetSortOrders(left_order_maker,
			left_order_maker);

	//Do sort merge join.
	if (numberOfArttrs != 0) {
		Pipe left_out(outPipe->getBufferSize());
		Pipe right_out(outPipe->getBufferSize());
		BigQ left_big_q(*inPipeL, left_out, left_order_maker, runlen / 2);
		BigQ right_big_q(*inPipeR, right_out, right_order_maker, runlen / 2);

		Record* left = new Record;
		Record* right = new Record;

		int left_has_records = left_out.Remove(left);
		int right_has_records = right_out.Remove(right);

		while (left_has_records && right_has_records) {
			int equals = comp_engine.Compare(left, &left_order_maker, right,
					&right_order_maker);
			if (equals < 0) {
				left_has_records = left_out.Remove(left);
			} else if (equals > 0) {
				right_has_records = right_out.Remove(right);
			} else {
				//nested join for equal records
				vector<Record *> left_buffer;
				vector<Record *> right_buffer;

				Record *temp_left = new Record;

				while (left_has_records = left_out.Remove(temp_left)) {
					if (comp_engine.Compare(left, temp_left,
							&left_order_maker)) {
						left_buffer.push_back(left);
						left = temp_left;
						break;
					} else {
						left_buffer.push_back(temp_left);
						temp_left = new Record;
					}

				}

				Record *temp_right = new Record;

				while (right_has_records = right_out.Remove(temp_right)) {
					if (comp_engine.Compare(right, temp_right,
							&right_order_maker)) {
						right_buffer.push_back(right);
						right = temp_right;
						break;
					} else {
						right_buffer.push_back(temp_right);
						temp_right = new Record;
					}
				}

				for (Record * temp_left : left_buffer) {
					for (Record * temp_right : right_buffer) {
						//code to create a merged record and insert it in the out pipe

						Record *out_rec = new Record;
						int numAttLeft = temp_left->getNumberofAttributes();
						int numAttRight = temp_right->getNumberofAttributes();

						if (creatKeepAttributeArray) {

							creatKeepAttributeArray = 0;
							keepAttributeArray = new int[numAttLeft
									+ numAttRight];

							for (int i = 0; i < numAttLeft; i++) {
								keepAttributeArray[i] = i;
							}

							for (int i = numAttLeft;
									i < (numAttLeft + numAttRight); i++) {
								keepAttributeArray[i] = i - numAttLeft;
							}
						}

						out_rec->MergeRecords(temp_left, temp_right, numAttLeft,
								numAttRight, keepAttributeArray,
								(numAttLeft + numAttRight), numAttLeft);

						delete temp_right;

						outPipe->Insert(out_rec);
					}
					delete temp_left;
				}
			}
		}
	}
//Do block nested loop join.
	else {

	}

//Shut down output pipe in the end.
	outPipe->ShutDown();
	delete keepAttributeArray;
	delete args;
}

void Join::WaitUntilDone() {
	pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int runlen) {
	this->runlen = runlen;
}
