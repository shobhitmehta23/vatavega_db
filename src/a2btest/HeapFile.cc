#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"
#include <iostream>


HeapFile::HeapFile() {
	current_page_index = 0;
}

int HeapFile::Create(const char *f_path, void *startup) {
	string meta_file = f_path + ".metadata";
	ofstream meta_data_file;
	meta_data_file.open(meta_file.c_str());

	meta_data_file << heap << endl;
	meta_data_file.close();

	file.Open(0, (char*)f_path);  // the first option of Open() when set to 0 creates the file.
	return 1;
}

void HeapFile::Load(Schema &f_schema, const char *loadpath) {
	//open the given file
	FILE* inputFile = fopen(loadpath, "r");

	//exit if file cannot be opened
	if (inputFile == 0) {
		cerr << "could not open text file  " << loadpath << "\n";
		exit(1);
	}

	Page temp_page;
	Record temp_record;
	off_t temp_page_index = file.getLastPageIndex();

	if(temp_page_index < 0) {
		// This means the file has no pages. So create the first one.
		temp_page_index = 0;
		file.AddPage(&temp_page, 0);
	}


	//load the last page
	file.GetPage(&temp_page, temp_page_index);

	//call suck next record till EOF
	while (temp_record.SuckNextRecord(&f_schema, inputFile) == 1) {
		//add record to the end of page.
		if (temp_page.Append(&temp_record)) {
			continue;
		} else {
			// The page is full
			// write the page to the file.
			file.AddPage(&temp_page, temp_page_index);
			temp_page.EmptyItOut();
			temp_page_index++;  // increment the page index. This will be the new last.
			temp_page.Append(&temp_record);
		}
	}
	file.AddPage(&temp_page, temp_page_index); //Add the last page.

}

int HeapFile::Open(const char *f_path) {
	cout << "Open a DbFile" << endl;
	file.Open(1, (char*)f_path);
	MoveFirst();
	return 1;  // Success.
}

void HeapFile::MoveFirst() {
	current_page_index = 0; //set current page index to 0 for the first page.
	if (file.GetLength() != 0) {
		file.GetPage(&page, current_page_index);
	}
}

int HeapFile::Close() {
	//if file length is >=0 then return 0 else return 1.
	if (file.Close() >= 0) {
		return 1; //return 1 if success
	} else {
		return 0; // return 0 for failure.
	}
}

void HeapFile::Add(Record &rec) {

	Page temp_page; //temporary page to hold the record.
	//case of a new file.
	if (file.GetLength() == 0) {
		if (temp_page.Append(&rec) == 1) {  // append the record to the temp page.
			file.AddPage(&temp_page, 0);
		} else {
			exit(1);  // exit with error.
		}
	} else {
		file.GetPage(&temp_page, file.getLastPageIndex());
		if (temp_page.Append(&rec) == 1)  {  // append record to the last page
			file.AddPage(&temp_page, file.getLastPageIndex());  // write the last page.
		} else {
			temp_page.EmptyItOut();
			temp_page.Append(&rec);
			file.AddPage(&temp_page, file.get_new_page_index());
		}
	}
}

int HeapFile::GetNext(Record &fetchme) {
	// try to get the first record from the current page if success leave else try next page.
	if (page.GetFirst(&fetchme) == 0) {
		current_page_index++; //increment the current page index page for the next page.

		//check if next page exists.
		if (current_page_index < file.get_new_page_index()) {
			file.GetPage(&page, current_page_index); //load the next page.
			return page.GetFirst(&fetchme); // return the first record.
		} else {
			return 0; // no next record, end of the file EOF.
		}
	}
	return 1; //Success
}

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comparor;

	// Loop over the method above to get the next record and check until it is accepted by the
	// given selection predicate. Return first such record.

	while (GetNext(fetchme) == 1) {
		if (comparor.Compare(&fetchme, &literal, &cnf) == 1) {
			return 1;  // Success
		}
	}
	return 0;  // Failure
}
