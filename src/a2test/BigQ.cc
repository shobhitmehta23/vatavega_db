#include "BigQ.h"
#include "Defs.h"

void BigQ :: generate_runs() {

	int space_in_run_for_records = calculate_space_in_run_for_records();

	Record temp_record;
	vector<Record> record_list;

	while (in->Remove(&temp_record)) {
		if (check_if_space_exists_in_run_for_record(
				space_in_run_for_records, temp_record)) {
			handle_newly_read_record(temp_record, &space_in_run_for_records, record_list);
		} else {
			handle_vectorized_records_of_run(record_list);
			space_in_run_for_records = calculate_space_in_run_for_records();
			handle_newly_read_record(temp_record, &space_in_run_for_records, record_list);
		}
	}

	handle_vectorized_records_of_run(record_list);
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	this->in = &in;
	this->out = &out;
	this->sortorder = &sortorder;
	this->runlen = runlen;
	this->filename = "test";


	pthread_t sort;

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}

int BigQ :: calculate_space_in_run_for_records() {
	int page_size = PAGE_SIZE;
	int total_space_in_run = runlen * page_size;
	return total_space_in_run - (sizeof(int) * runlen);
}

bool BigQ :: check_if_space_exists_in_run_for_record(int space_in_run, Record const& record) {
	int record_size = record.get_record_size();
	return (space_in_run - record_size) >= 0;
}

void BigQ :: handle_newly_read_record(Record record, int *space_in_run, vector<Record>& record_list) {
	record_list.push_back(record);
	*space_in_run = *space_in_run - record.get_record_size();
}

void BigQ :: handle_vectorized_records_of_run(vector<Record>& record_list) {
	sort(record_list.begin(), record_list.end(), record_sort_functor(sortorder));

	vector<Page> pages;
	vector<Record> :: iterator record_list_iterator;
	Page temp_page;
	File file;
	file.Open(0, filename);

	for (record_list_iterator = record_list.begin();
			record_list_iterator != record_list.end(); record_list_iterator++) {
		if (!temp_page.Append(&(*record_list_iterator))) {
			file.AddPage(&temp_page, file.get_new_page_index());
			temp_page.EmptyItOut();
			temp_page.Append(&(*record_list_iterator));
		}
	}

	file.AddPage(&temp_page, file.get_new_page_index());

	record_list.clear();
}
