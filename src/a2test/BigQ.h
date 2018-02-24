#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class RecordWrapper {
public:
	int runIndex;
	Record record;
};

typedef struct thread_arguments {
	Pipe *in;
	Pipe *out;
	OrderMaker *sortorder;
	int runlen;
	char *filename;
	int runCount;
	int *run_end_page_idx;
} thread_arguments;

typedef struct record_sort_functor {

private:
	OrderMaker * ordermaker;
	ComparisonEngine comparison_engine;

public:
	record_sort_functor(OrderMaker * order_maker) {
		ordermaker = order_maker;
	}

	bool operator()(Record* record_one, Record* record_two) {
		return comparison_engine.Compare(record_one, record_two, ordermaker)
				< 0;
	}

} record_sort_functor;

typedef struct record_wrapper_sort_functor {
	OrderMaker * ordermaker;
	ComparisonEngine comparison_engine;
public:
	record_wrapper_sort_functor(OrderMaker * order_maker) {
		ordermaker = order_maker;
	}
	bool operator()(RecordWrapper *rw1, RecordWrapper *rw2) {
		return comparison_engine.Compare(&(rw1->record), &(rw2->record),
				ordermaker) >= 0;
	}
} record_wrapper_sort_functor;

class BigQ {

public:

	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ();

//private:

	/*void generate_runs();
	 void handle_vectorized_records_of_run(vector<Record>& record_list);
	 int calculate_space_in_run_for_records();
	 bool check_if_space_exists_in_run_for_record(int space_in_run,
	 const Record& record);
	 void handle_newly_read_record(Record record, int *space_in_run,
	 vector<Record>& record_list);
	 void merge_runs();
	 void *sort_externally(void *arg);*/


};

#endif
