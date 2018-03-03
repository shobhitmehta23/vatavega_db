#include "SortedFile.h"
#include "BigQ.h"

void AddRecordToFile(Record& rec, File *f);
int GetNextRecordFromFile(Record &fetchme, Page* p, File* f, off_t &page_index);
void appendSourceFileContents(File* dest_file, File* src_file, Page *src_page,
		off_t &page_index);
void appendOutPipeCOntents(File* dest_file, Pipe* out);

SortedFile::SortedFile() {
	mode = READ_MODE;
	runlen = 0;
	order_maker = NULL;
	output_pipe = NULL;
	input_pipe = NULL;
	sorting_queue = NULL;
}

SortedFile::~SortedFile() {
}

int SortedFile::Create(const char *fpath, void *startup) {
	fName = (char *) fpath;
	string meta_file(fpath);
	meta_file.append(string(".meta"));
	ofstream meta_data_file;
	meta_data_file.open(meta_file.c_str());

	meta_data_file << sorted << endl;

	SortInfo *sort_info = (SortInfo *) startup;
	sort_info->order_maker->serialize_to_ofstream(meta_data_file);

	//Assign it to the local order_maker.
	order_maker = sort_info->order_maker;
	runlen = sort_info->runlen;

	meta_data_file << sort_info->runlen << endl;

	meta_data_file.close();

	file.Open(0, (char*) fpath);

	return 1;
}

void SortedFile::Add(Record &addme) {

	if (mode == READ_MODE) {
		reinitialize_bigQ();
	}
	mode = WRITE_MODE;
	input_pipe->Insert(&addme);
}

void SortedFile::Load(Schema &myschema, const char *loadpath) {

	if (mode == READ_MODE) {
			reinitialize_bigQ();
	}
	mode = WRITE_MODE;

	//open the given file
	FILE* inputFile = fopen(loadpath, "r");

	//exit if file cannot be opened
	if (inputFile == 0) {
		cerr << "could not open text file  " << loadpath << "\n";
		exit(1);
	}

	Record temp_record;
	while (temp_record.SuckNextRecord(&myschema, inputFile) == 1) {
		Record* rec = new Record;
		rec->Copy(&temp_record);
		Add(*rec);
	}
}

int SortedFile::Open(const char *fpath) {
	cout << "Open a DbFile" << endl;
	mode = READ_MODE;

	fName = (char *) fpath;

	string meta_file(fpath);
	meta_file.append(string(".meta"));

	ifstream meta_data_file;
	meta_data_file.open(meta_file.c_str());

	int type;
	meta_data_file >> type;

	order_maker = new OrderMaker;
	order_maker->deserialize_from_isstream(meta_data_file);
	meta_data_file >> runlen;
	meta_data_file.close();
	cout << "Open a DbFile" << endl;
	file.Open(1, (char*) fpath);
	MoveFirst();

	return 1;
}

void SortedFile::twoWayMerge() {
	//Shutdown the input pipe
	input_pipe->ShutDown();

	//Read the current file contents.
	file.Close();
	string temp_name(fName);
	temp_name.append(".temp");
	rename(fName, temp_name.c_str());
	File srcFile;
	Page srcPage;
	off_t src_page_index = 0;
	srcFile.Open(1, (char*) temp_name.c_str());

	//Following is equivalent to MoveFirst()
	if (srcFile.GetLength() != 0) {
		srcFile.GetPage(&srcPage, 0);
	}

	file.Open(0, (char*) fName);
	//this->MoveFirst();

	Record rec1;
	Record rec2;

	if (srcFile.GetLength() == 0) {

		if (!output_pipe->Remove(&rec1)) {
			//page_index.Close();
			return;
		}
	}

	if (srcFile.GetLength() == 0) {
		AddRecordToFile(rec1, &file);

		while (output_pipe->Remove(&rec1)) {
			AddRecordToFile(rec1, &file);
		}
	} else if (!output_pipe->Remove(&rec1)) {
		GetNextRecordFromFile(rec2, &srcPage, &srcFile, src_page_index);
		AddRecordToFile(rec2, &file);
	} else {
		ComparisonEngine ce;
		GetNextRecordFromFile(rec2, &srcPage, &srcFile, src_page_index);
		while (true) {
			if (ce.Compare(&rec1, &rec2, order_maker) <= 0) {
				AddRecordToFile(rec1, &file);
				if (!output_pipe->Remove(&rec1)) {
					//append rec2 and the remaining src file to the destination file.
					AddRecordToFile(rec2, &file);
					appendSourceFileContents(&file, &srcFile, &srcPage,
							src_page_index);
					break;
				}
			} else {
				AddRecordToFile(rec2, &file);
				if (!GetNextRecordFromFile(rec2, &srcPage, &srcFile,
						src_page_index)) {
					//append rec1 and the remaining pipe contents to the destination file.
					AddRecordToFile(rec1, &file);
					appendOutPipeCOntents(&file, output_pipe);
					break;
				}
			}
		}
	}
	srcFile.Close();
}

void appendSourceFileContents(File* dest_file, File* src_file, Page *src_page,
		off_t &page_index) {
	Record rec;
	while (GetNextRecordFromFile(rec, src_page, src_file, page_index)) {
		AddRecordToFile(rec, dest_file);
	}
}

void appendOutPipeCOntents(File* dest_file, Pipe* out) {
	Record rec;
	while (out->Remove(&rec)) {
		AddRecordToFile(rec, dest_file);
	}
}

void AddRecordToFile(Record& temp, File *f) {
	Record rec;
	rec.Copy(&temp);

	Page temp_page; //temporary page to hold the record.
	//case of a new file.
	if (f->GetLength() == 0) {
		if (temp_page.Append(&rec) == 1) { // append the record to the temp page.
			f->AddPage(&temp_page, 0);
		} else {
			exit(1);  // exit with error.
		}
	} else {
		f->GetPage(&temp_page, f->getLastPageIndex());
		if (temp_page.Append(&rec) == 1) { // append record to the last page
			f->AddPage(&temp_page, f->getLastPageIndex()); // write the last page.
		} else {
			temp_page.EmptyItOut();
			temp_page.Append(&rec);
			f->AddPage(&temp_page, f->get_new_page_index());
		}
	}
}

int GetNextRecordFromFile(Record &fetchme, Page *p, File *f,
		off_t &page_index) {
	// try to get the first record from the current page if success leave else try next page.
	if (p->GetFirst(&fetchme) == 0) {
		page_index++; //increment the current page index page for the next page.

		//check if next page exists.
		if (page_index < f->get_new_page_index()) {
			f->GetPage(p, page_index); //load the next page.
			return p->GetFirst(&fetchme); // return the first record.
		} else {
			return 0; // no next record, end of the file EOF.
		}
	}
	return 1; //Success
}

int SortedFile::Close() {
	if (mode == WRITE_MODE) {
		twoWayMerge();
		delete sorting_queue;
		delete input_pipe;
		delete output_pipe;
	}
	if (file.Close() >= 0) {
		return 1; //return 1 if success
	} else {
		return 0; // return 0 for failure.
	}
}

void SortedFile::MoveFirst() {

}

int SortedFile::GetNext(Record &fetchme) {
	if (mode == WRITE_MODE) {
		twoWayMerge();
		MoveFirst();
		mode = READ_MODE;
	}

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

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
	return 1;
}

void SortedFile::reinitialize_bigQ() {
	delete sorting_queue;
	delete input_pipe;
	delete output_pipe;

	input_pipe = new Pipe(PIPE_SIZE);
	output_pipe = new Pipe(PIPE_SIZE);

	sorting_queue = new BigQ(*input_pipe, *output_pipe, *order_maker, runlen);
}
