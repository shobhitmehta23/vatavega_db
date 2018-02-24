#include "BigQ.h"

//#include <sys/_pthread/_pthread_t.h>
//#include <sys/_types/_null.h>
#include <algorithm>
#include <iterator>
#include <queue>

#include "Comparison.h"
#include "Defs.h"

using namespace std;

int calculate_space_in_run_for_records(int runlen);
bool check_if_space_exists_in_run_for_record(int space_in_run,
		Record* const & record);
void handle_newly_read_record(Record* record, int *space_in_run,
		vector<Record*>& record_list);
void handle_vectorized_records_of_run(vector<Record*>& record_list,
		thread_arguments *args, File file);
void generate_runs(thread_arguments *args);
void merge_runs(thread_arguments *args);
void *sort_externally(void *thread_args);

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	//this->in = &in;
//	this->out = &out;
//	this->sortorder = &sortorder;
//	this->runlen = runlen;
//	this->filename = "test";
//	this->runCount = 0;
	thread_arguments args;
	args.in = &in;
	args.out = &out;
	args.sortorder = &sortorder;
	args.runlen = runlen;
	args.filename = "test";
	args.runCount = 0;

	pthread_t thread;

	pthread_create(&thread, NULL, sort_externally, (void *) &args);

	pthread_exit(NULL);
}

BigQ::~BigQ() {
}

void *sort_externally(void *thread_args) {
	thread_arguments *args;
	args = (thread_arguments *) thread_args;
	generate_runs(args);
	merge_runs(args);
}

void generate_runs(thread_arguments *args) {

	File file;
	file.Open(0, args->filename);

	int space_in_run_for_records = calculate_space_in_run_for_records(
			args->runlen);

	Record temp_record;
	vector<Record*> record_list;

	while (args->in->Remove(&temp_record)) {
		Record *temp_record_ptr = new Record;
		temp_record_ptr->Copy(&temp_record);
		if (check_if_space_exists_in_run_for_record(space_in_run_for_records,
				temp_record_ptr)) {
			handle_newly_read_record(temp_record_ptr, &space_in_run_for_records,
					record_list);
		} else {
			handle_vectorized_records_of_run(record_list, args, file);
			space_in_run_for_records = calculate_space_in_run_for_records(
					args->runlen);
			handle_newly_read_record(temp_record_ptr, &space_in_run_for_records,
					record_list);
		}
	}

	handle_vectorized_records_of_run(record_list, args, file);
}

int calculate_space_in_run_for_records(int runlen) {
	int page_size = PAGE_SIZE;
	int total_space_in_run = runlen * page_size;
	return total_space_in_run - (sizeof(int) * runlen);
}

bool check_if_space_exists_in_run_for_record(int space_in_run,
		Record* const & record) {
	int record_size = record->get_record_size();
	return (space_in_run - record_size) >= 0;
}

void handle_newly_read_record(Record* record, int *space_in_run,
		vector<Record*>& record_list) {
	record_list.push_back(record);
	*space_in_run = *space_in_run - record->get_record_size();
}

void handle_vectorized_records_of_run(vector<Record*>& record_list,
		thread_arguments *args, File file) {

	args->runCount++; //increment run count
	sort(record_list.begin(), record_list.end(),
			record_sort_functor(args->sortorder));

	vector<Page> pages;
	vector<Record*>::iterator record_list_iterator;
	Page temp_page;

	for (record_list_iterator = record_list.begin();
			record_list_iterator != record_list.end(); record_list_iterator++) {
		if (!temp_page.Append(*record_list_iterator)) {
			file.AddPage(&temp_page, file.get_new_page_index());
			temp_page.EmptyItOut();
			temp_page.Append(*record_list_iterator);
		}
	}

	file.AddPage(&temp_page, file.get_new_page_index());

	record_list.clear();
}

void merge_runs(thread_arguments *args) {
	File tempFile;
	tempFile.Open(1, args->filename);

	//Create an array to hold starting page for each run.

	//Create a priority queue for merging.
	priority_queue<RecordWrapper *, vector<RecordWrapper *>,
			record_wrapper_sort_functor> priority_Q(args->sortorder);

	int last_run_length = tempFile.GetLength()
			- (args->runlen * (args->runCount - 1)); // - 1; //last -1 as each file has first (0th) page for special purpose.

	int lastRunIndex = args->runCount - 1;

	Page run_current_page[args->runCount];

	int run_current_page_index[args->runCount];

	//initialize the current pages and their current indexes for all the runs.
	//Also, initialize the PQ along with it.
	for (int i = 0; i < args->runCount; i++) {
		run_current_page_index[i] = i * args->runlen;
		tempFile.GetPage(&run_current_page[i], run_current_page_index[i]);

		RecordWrapper* rw = new RecordWrapper();
		rw->runIndex = i;
		run_current_page[i].GetFirst(&rw->record);
		priority_Q.push(rw);
	}

	/*Remove the head, add to out pipe, and push next record from corresponding run to the tail of priority
	 queue and repeat until all the runs are merged  i.e. PQ is empty.*/
	while (true) {
		if (priority_Q.empty()) {
			break;
		}

		//keep a handle of head
		RecordWrapper* head_record_wrapper = priority_Q.top();

		//remove head
		priority_Q.pop();

		//output the head of queue to out pipe.
		args->out->Insert(&head_record_wrapper->record);

		//Create a wrapper for new record for corresponding run to be inserted in PQ.
		RecordWrapper* tail_record_wrapper = new RecordWrapper;
		int runIndex = head_record_wrapper->runIndex;
		tail_record_wrapper->runIndex = runIndex;

		//If page end is reached, go to the next page.
		if (run_current_page[runIndex].GetFirst(&tail_record_wrapper->record)) {
			priority_Q.push(tail_record_wrapper);
		} else {
			run_current_page_index[runIndex]++;

			//No need to process if the run is over.
			if (!(run_current_page_index[runIndex] >= tempFile.GetLength()
					|| run_current_page_index[runIndex]
							>= ((runIndex + 1) * args->runlen))) {
				tempFile.GetPage(&run_current_page[runIndex],
						run_current_page_index[runIndex]);
				run_current_page[runIndex].GetFirst(
						&tail_record_wrapper->record);
				priority_Q.push(tail_record_wrapper);
			}

		}

	}

	tempFile.Close();
	args->out->ShutDown();
}

