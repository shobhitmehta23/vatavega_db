#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <set>
#include <fstream>
#include <iostream>

using namespace std;

class TableInfo {
public:
	int no_of_tuples;
	set<string> table_set;
	unordered_map<string, long> attributes;
	TableInfo(int no_of_tuples, set<string> table_set);
	TableInfo() {
	}
	;
	~TableInfo() {
	}
	;

	friend ostream &operator<<(ostream &output, const TableInfo &table_info) {
		// no of tuples
		output << table_info.no_of_tuples << endl;

		// table set
		int table_set_size = table_info.table_set.size();
		output << table_set_size << endl;
		for (auto x : table_info.table_set) {
			output << x << endl;
		}

		// attribute map
		int att_map_size = table_info.attributes.size();
		output << att_map_size << endl;
		for (auto x : table_info.attributes) {
			output << x.first << endl;
			output << x.second << endl;
		}
		return output;
	}

	friend istream & operator >>(istream &input, TableInfo &table_info) {
		input >> table_info.no_of_tuples;

		int table_set_size;
		input >> table_set_size;
		for (int i = 0; i < table_set_size; i++) {
			string temp;
			input >> temp;
			table_info.table_set.insert(temp);
		}

		int att_map_size;
		input >> att_map_size;
		for (int i = 0; i < att_map_size; i++) {
			string attribute_name;
			int distinct_count;

			input >> attribute_name;
			input >> distinct_count;

			table_info.attributes[attribute_name] = distinct_count;
		}

		return input;
	}
};

class Statistics {

private:
	unordered_map<int, TableInfo *> group_to_table_info_map;
	unordered_map<string, int> relation_to_group_map;
	int group_no = 1;

	double getRowsinJoinedTableContainingGivenRelation(string relName);
	set<int> checkIfRelationsJoinedSatisfyConstraints(string rel_names[],
			int numToJoin);
	TableInfo* checkIfAttributeExistsInGivenRelations(set<int> groupIds,
			Operand* op, string &relation);
	void checkIfAttributeExistsInGivenRelations(set<int> groupIds, Operand* op);
	void updateTableInfoMaps(TableInfo * table_info);
	set<int> getGroupIdsForRelations(string relation_names[], int number_of_relation);

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
};

#endif
