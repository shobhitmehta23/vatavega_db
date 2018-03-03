#include "SortedFile.h"

SortedFile::SortedFile() {
	input_pipe = new Pipe(PIPE_SIZE);
	output_pipe = new Pipe(PIPE_SIZE);
	mode = READ_MODE;
	runlen = 0;
	order_maker = NULL;
}

SortedFile::~SortedFile() {

}

int SortedFile::Create(const char *fpath, void *startup) {
	string meta_file(fpath);
	meta_file.append(string(".meta"));
	ofstream meta_data_file;
	meta_data_file.open(meta_file.c_str());

	meta_data_file << sorted << endl;

	SortInfo *sort_info = (SortInfo *) startup;
	sort_info->order_maker->serialize_to_ofstream(meta_data_file);

	meta_data_file << sort_info->runlen << endl;

	meta_data_file.close();

	return 1;
}

void SortedFile::Add(Record &addme) {
	mode = WRITE_MODE;
	input_pipe->Insert(&addme);
}

void SortedFile::Load(Schema &myschema, const char *loadpath) {
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

	string meta_file(fpath);
	meta_file.append(string(".meta"));

	ifstream meta_data_file;
	meta_data_file.open(meta_file.c_str());

	int type;
	meta_data_file >> type;

	order_maker->deserialize_from_isstream(meta_data_file);
	meta_data_file >> runlen;
	meta_data_file.close();

	MoveFirst();

	return 1;
}
