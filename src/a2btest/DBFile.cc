#include "DBFile.h"

#include <iostream>

#include "HeapFile.h"
#include "SortedFile.h"
#include <fstream>

DBFile::DBFile() {
	dbfile = NULL;
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {

	if (f_type == heap) {
		dbfile = new HeapFile;
		dbfile->Create(f_path, startup);
	} else if (f_type == sorted) {
		dbfile = new SortedFile;
		dbfile->Create(f_path, startup);
	} else if (f_type == tree) {
		//TODO: do nothing for now
		cerr << "invalid f_type: " << f_type << endl;
		exit(1);
	} else {
		cerr << "invalid f_type: " << f_type << endl;
		exit(1);
	}

	//file.Open(0, (char*)f_path);  // the first option of Open() when set to 0 creates the file.
	return 1;
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
	dbfile->Load(f_schema, loadpath);
}

int DBFile::Open(const char *f_path) {

	string meta_file = f_path + ".metadata";
	ifstream meta_data_file;
	meta_data_file.open(meta_file.c_str());

	int type;
	meta_data_file >> type;
	fType f_type = (fType) type;
	meta_data_file.close();

	if (f_type == heap) {
			dbfile = new HeapFile;
			dbfile->Open(f_path);
		} else if (f_type == sorted) {
			dbfile = new SortedFile;
			dbfile->Open(f_path);
		} else if (f_type == tree) {
			//TODO: do nothing for now
			cerr << "invalid f_type: " << f_type << endl;
			exit(1);
		} else {
			cerr << "invalid f_type: " << f_type << endl;
			exit(1);
		}

	return 1;  // Success.
}

void DBFile::MoveFirst() {
	dbfile->MoveFirst();
}

int DBFile::Close() {
	return dbfile->Close();
}

void DBFile::Add(Record &rec) {
	dbfile->Add(rec);
}

int DBFile::GetNext(Record &fetchme) {
	return dbfile->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	return dbfile->GetNext(fetchme, cnf, literal);
}
