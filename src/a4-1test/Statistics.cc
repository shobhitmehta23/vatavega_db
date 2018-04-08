#include "Statistics.h"
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iostream>

using namespace std;

Statistics::Statistics() {
}

Statistics::Statistics(Statistics &copyMe) {
	char * file_name = (char*) malloc(20 * sizeof(char));
	sprintf(file_name, "meta_data_%d", (rand() % 1000 + rand() % 1000));
	copyMe.Write(file_name);
	this->Read(file_name);
	remove(file_name);
	free(file_name);
}

Statistics::~Statistics() {
	for (auto x : group_to_table_info_map) {
		delete x.second;
	}
}

void Statistics::AddRel(char *relName, int numTuples) {
	string rel_name(relName);
	set<string> temp_set;
	temp_set.insert(rel_name);

	TableInfo * table_info = new TableInfo(numTuples, temp_set);
	relation_to_group_map[rel_name] = group_no;
	group_to_table_info_map[group_no++] = table_info;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
	string rel_name(relName);
	string att_name(attName);

	int grp_no = relation_to_group_map[rel_name];
	TableInfo * table_info = group_to_table_info_map[grp_no];
	table_info->attributes[att_name] = (numDistincts == -1? table_info->no_of_tuples : numDistincts);
}

void Statistics::CopyRel(char *oldName, char *newName) {
	string old_name(oldName);

	int old_group = relation_to_group_map[old_name];
	TableInfo * old_table_info = group_to_table_info_map[old_group];

	AddRel(newName, old_table_info->no_of_tuples);

	auto old_attribute_map = old_table_info->attributes;
	for (auto attribute : old_attribute_map) {
		this->AddAtt(newName, (char *)attribute.first.c_str(), attribute.second);
	}
}
	
void Statistics::Read(char *fromWhere) {
	ifstream meta_data_file;
	meta_data_file.open(fromWhere);

	// read group number
	meta_data_file >> group_no;

	// read first map
	int group_to_table_info_map_size;
	meta_data_file >> group_to_table_info_map_size;

	for (int i = 0; i < group_to_table_info_map_size; i++) {
		TableInfo * table_info = new TableInfo();
		int group_no;

		meta_data_file >> group_no;
		meta_data_file >> *table_info;

		group_to_table_info_map[group_no] = table_info;
	}

	// read second map
	int relation_to_group_map_size;
	meta_data_file >> relation_to_group_map_size;

	for (int i = 0; i< relation_to_group_map_size; i++) {
		string relation;
		int group_no;

		meta_data_file >> relation;
		meta_data_file >> group_no;

		relation_to_group_map[relation] = group_no;
	}

	meta_data_file.close();
}

void Statistics::Write(char *fromWhere) {
	ofstream meta_data_file;
	meta_data_file.open(fromWhere);

	// write group number
	meta_data_file << group_no << endl;

	// write first map
	int group_to_table_info_map_size = group_to_table_info_map.size();
	meta_data_file << group_to_table_info_map_size << endl;
	for (auto x : group_to_table_info_map) {
		meta_data_file << x.first << endl;
		meta_data_file << *x.second << endl;
	}

	// write second map
	int relation_to_group_map_size = relation_to_group_map.size();
	meta_data_file << relation_to_group_map_size << endl;
	for (auto x : relation_to_group_map) {
		meta_data_file << x.first << endl;
		meta_data_file << x.second << endl;
	}

	meta_data_file.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {
}

TableInfo::TableInfo(int no_of_tuples, set<string> table_set) {
		this->no_of_tuples = no_of_tuples;
		this->table_set = table_set;
}
