#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile() {
	currentPageIndex = 0;
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
	char* path = f_path;
	file.Open(0, path); // the first option of Open() when set to 0 create the file.
	return 1;
	//FIXME exception handling is recommended for failure cases for graceful exit.
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
	//open the given file
	FILE inputFile = fopen(loadpath, "r");
	//exit if file cannot be opened
	if (inputFile == 0) {
		exit(1);
	}

	Page tempPage;
	Record tempRecord;
	off_t tempPageIndex = file.GetLength() - 2; // initialize to point to the current last page.

	//load the last page
	file.GetPage(&tempPage, tempPageIndex);

	//call suck next record till EOF
	while (tempRecord.SuckNextRecord(&f_schema, &inputFile) == 1) {
		//add record to the end of page.
		if (tempPage.Append(&tempRecord)) {
			continue;
		} else {
			//The page is full
			//write the page to the file.
			file.AddPage(&tempPage, tempPageIndex);
			tempPage.EmptyItOut();
			tempPageIndex++; // increment the page index. This will be the new last.
			tempPage.Append(&tempRecord);
		}
	}
	file.AddPage(&tempPage, tempPageIndex); //Add the last page.

}

int DBFile::Open(const char *f_path) {
	char *path = f_path;
	cout << "Open a DbFile" << endl;
	file.Open(1, path);
	MoveFirst();
	//FIXME exception handling is recommended for failure cases for graceful exit.
	return 1; // return 1 on success.
}

void DBFile::MoveFirst() {
	currentPageIndex = 0; //set current page index to 0 for the first page.
	if (file.GetLength() != 0) {
		file.GetPage(&page, currentPageIndex);
	}
}

int DBFile::Close() {
	//if file length is >=0 then return 0 else return 1.
	if (file.Close() >= 0) {
		return 1; //return 1 if success
	} else {
		return 0; // return 0 for failure.
	}
}

void DBFile::Add(Record &rec) {

	Page temp; //temporary page to hold the record.
	//case of a new file.
	if (file.GetLength() == 0) {
		if (temp.Append(&rec) == 1) { //append the record to the temp page.
			file.AddPage(&temp, 0);
		} else {
			exit(1); //exit with error.
		}
	} else {
		//FIXME actually we should  not deem that the current last page to be the one which is half filled, it can
		// be any page like second last or even before that. For now considering last page is the one to be added to.
		file.GetPage(&temp, file.GetLength() - 2);		// Get the last page. which is at -2
		if (temp.Append(&rec) == 1) //append record to the last page
				{
			file.AddPage(&temp, file.GetLength() - 2); //write the last page.
		} else {
			temp.EmptyItOut();
			temp.Append(&rec);
			file.AddPage(&temp, file.GetLength()-1); // the new page should go at last -1
		}
	}
	rec.bits = NULL; //consume the record
}

int DBFile::GetNext(Record &fetchme) {
	// try to get the first record from the current page if success leave else try next page.
	if (page.GetFirst(&fetchme) == 0) {
		currentPageIndex++; //increment the current page index page for the next page.

		//check if next page exists.
		if (currentPageIndex < file.GetLength()) {
			file.GetPage(&page, currentPageIndex); //load the next page.
			return page.GetFirst(&fetchme); // return the first record.
		} else {
			return 0; // no next record, end of the file EOF.
		}

	}
	return 1; //Success
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comparor;

	//Loop over the method above to get the next record and check until it is accepted by the
	//given selection predicate. Return first such record.

	while (GetNext(fetchme) == 1) {
		if (comparor.Compare(&fetchme, &literal, &cnf) == 1) {
			return 1;//Success
		}
	}
	return 0;	//Failure
}
