#include "Statistics.h"
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>
#include <cmath>

string remove_relation_name_from_qualified_attribute_name(
		string qualified_attribute_name);
string convert_to_qualified_name(string attribute_name, string relation_name);
bool is_qualified_name(string attribute_name);

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
	string att_name = convert_to_qualified_name(string(attName), rel_name);

	int grp_no = relation_to_group_map[rel_name];
	TableInfo * table_info = group_to_table_info_map[grp_no];
	table_info->attributes[att_name] = (
			numDistincts == -1 ? table_info->no_of_tuples : numDistincts);
}

void Statistics::CopyRel(char *oldName, char *newName) {
	string old_name(oldName);

	int old_group = relation_to_group_map[old_name];
	TableInfo * old_table_info = group_to_table_info_map[old_group];

	AddRel(newName, old_table_info->no_of_tuples);

	auto old_attribute_map = old_table_info->attributes;
	for (auto attribute : old_attribute_map) {
		string new_att_name =
				remove_relation_name_from_qualified_attribute_name(
						attribute.first);
		this->AddAtt(newName, (char *) new_att_name.c_str(), attribute.second);
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

	for (int i = 0; i < relation_to_group_map_size; i++) {
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

set<int> Statistics::checkIfRelationsJoinedSatisfyConstraints(
		string rel_names[], int numToJoin) {
	set<int> group_set;
	set<string> expected_relation_set;
	set<string> input_relation_set;

	for (int i = 0; i < numToJoin; i++) {
		// add to input relation set
		input_relation_set.insert(rel_names[i]);

		int group = relation_to_group_map[rel_names[i]];
		group_set.insert(group);
		set<string> table_set = group_to_table_info_map[group]->table_set;
		expected_relation_set.insert(table_set.begin(), table_set.end());
	}

	if (!(expected_relation_set == input_relation_set)) {
		cerr << "the joins on table do not follow the required constraints"
				<< endl;
		exit(-1);
	}

	return group_set;
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[],
		int numToJoin) {
	string rel_names[numToJoin];

	for (int i = 0; i < numToJoin; i++) {
		rel_names[i] = string(relNames[i]);
	}

	set<int> groupIds = checkIfRelationsJoinedSatisfyConstraints(rel_names,
			numToJoin);

	//Cross-product
	if ((!parseTree) && groupIds.size() > 1) {
		//create a new table info for cross-prod
		TableInfo * cross_product_table_info = new TableInfo();

		//initialize num of tuple to 1 for multiplication
		int num_of_cross_prod_tuples = 1;
		//iterate over all the groups and combine them.
		for (int groupId : groupIds) {
			TableInfo* tb = group_to_table_info_map[groupId];

			//Add all the attributes to the map, their distinct values will remain intact.
			for (unordered_map<string, long>::iterator it =
					tb->attributes.begin(); it != tb->attributes.end(); ++it) {

				cross_product_table_info->attributes[it->first] = it->second;
			}

			//combine the relations involved in the groups.
			cross_product_table_info->table_set.insert(tb->table_set.begin(),
					tb->table_set.end());
			num_of_cross_prod_tuples *= tb->no_of_tuples;
		}

		//update the number of tuples in the final cross-product
		cross_product_table_info->no_of_tuples = num_of_cross_prod_tuples;

		//Apply the cross product and uopdate the Statistics object.
		updateTableInfoMaps(cross_product_table_info);
	}

	while (parseTree) {
		OrList *orList = parseTree->left;
		TableInfo* temp_table_info1 = NULL;
		TableInfo* temp_table_info2 = NULL;

		while (orList) {

			ComparisonOp* cmp = orList->left;
			if (cmp) {
				int code = cmp->code;
				Operand *op1 = cmp->left;
				Operand *op2 = cmp->right;
				TableInfo* tb1 = NULL;
				TableInfo* tb2 = NULL;
				string rel_name1;
				string rel_name2;

				groupIds = getGroupIdsForRelations(rel_names, numToJoin);
				if (op1->code == NAME) {
					tb1 = checkIfAttributeExistsInGivenRelations(groupIds, op1,
							rel_name1);
				}
				if (op2->code == NAME) {
					tb2 = checkIfAttributeExistsInGivenRelations(groupIds, op2,
							rel_name2);
				}
				//vector<TableInfo*> tableInfos;
				switch (code) {
				case EQUALS:
					//check if it is a join or selection
					//1) join
					if (op1->code == NAME && op2->code == NAME) {
						double distValL =
								tb1->attributes[convert_to_qualified_name(
										string(op1->value), rel_name1)];
						double distValR =
								tb2->attributes[convert_to_qualified_name(
										string(op2->value), rel_name2)];
						double tupPerValL = tb1->no_of_tuples / distValL;
						double tupPerValR = tb2->no_of_tuples / distValR;
						long minDistValue = std::min(distValL, distValR);
						long newNumOfTuples = (tupPerValL * tupPerValR
								* minDistValue);

						TableInfo * new_table_info = new TableInfo();
						new_table_info->no_of_tuples = newNumOfTuples;

						for (unordered_map<string, long>::iterator it =
								tb1->attributes.begin();
								it != tb1->attributes.end(); ++it) {

							new_table_info->attributes[it->first] = std::min(
									newNumOfTuples, it->second);
						}
						for (unordered_map<string, long>::iterator it =
								tb2->attributes.begin();
								it != tb2->attributes.end(); ++it) {

							new_table_info->attributes[it->first] = std::min(
									newNumOfTuples, it->second);
						}

						new_table_info->table_set.insert(tb1->table_set.begin(),
								tb1->table_set.end());
						new_table_info->table_set.insert(tb2->table_set.begin(),
								tb2->table_set.end());

						temp_table_info1 ?
								temp_table_info2 = new_table_info :
								temp_table_info1 = new_table_info;

					}
					//2) selection
					else {
						TableInfo* tbInfo;
						string* rel_name;
						Operand * op;
						if (op1->code == NAME) {
							tbInfo = tb1;
							rel_name = &rel_name1;
							op = op1;
						} else {
							tbInfo = tb2;
							rel_name = &rel_name2;
							op = op2;
						}

						TableInfo * new_table_info = new TableInfo();

						double distVal =
								tbInfo->attributes[convert_to_qualified_name(
										string(op->value), *rel_name)];
						long newNumOfTuples = tbInfo->no_of_tuples / distVal;
						new_table_info->no_of_tuples = newNumOfTuples;

						new_table_info->table_set.insert(
								tbInfo->table_set.begin(),
								tbInfo->table_set.end());

						for (unordered_map<string, long>::iterator it =
								tbInfo->attributes.begin();
								it != tbInfo->attributes.end(); ++it) {

							new_table_info->attributes[it->first] = std::min(
									newNumOfTuples, it->second);
						}

						new_table_info->attributes[convert_to_qualified_name(
								string(op->value), *rel_name)] = 1;

						temp_table_info1 ?
								temp_table_info2 = new_table_info :
								temp_table_info1 = new_table_info;

					}
					break;
				case LESS_THAN:
				case GREATER_THAN:

					TableInfo* tbInfo;
					string* rel_name;
					Operand * op;
					if (op1->code == NAME) {
						tbInfo = tb1;
						rel_name = &rel_name1;
						op = op1;
					} else {
						tbInfo = tb2;
						rel_name = &rel_name2;
						op = op2;
					}
					TableInfo * new_table_info = new TableInfo();

					long newNumOfTuples = tbInfo->no_of_tuples / 3.0;

					new_table_info->no_of_tuples = newNumOfTuples;

					new_table_info->table_set.insert(tbInfo->table_set.begin(),
							tbInfo->table_set.end());

					for (unordered_map<string, long>::iterator it =
							tbInfo->attributes.begin();
							it != tbInfo->attributes.end(); ++it) {

						new_table_info->attributes[it->first] = std::min(
								newNumOfTuples, it->second);
					}

					new_table_info->attributes[convert_to_qualified_name(
							string(op->value), rel_name2)] =
							tbInfo->attributes[convert_to_qualified_name(
									string(op->value), *rel_name)] / 3.0;

					temp_table_info1 ?
							temp_table_info2 = new_table_info :
							temp_table_info1 = new_table_info;
					break;

				}

			}
			//TODO Combine the results of OR if mupltiple OR conditions.
			if (temp_table_info1 && temp_table_info2) {
				std::set<string>::iterator it =
						temp_table_info1->table_set.begin();
				int group = relation_to_group_map[*it];
				long original_no_of_tuples =
						group_to_table_info_map[group]->no_of_tuples;

				long new_num_of_tuples = (temp_table_info1->no_of_tuples
						+ temp_table_info2->no_of_tuples);

				double sub;

				if (temp_table_info1->no_of_tuples
						% temp_table_info2->no_of_tuples == 0) {
					sub = 0.0;
				} else {
					sub = floor(
							((double) temp_table_info1->no_of_tuples
									/ original_no_of_tuples)
									* temp_table_info2->no_of_tuples);

				}

				new_num_of_tuples = new_num_of_tuples - sub;

				TableInfo * new_table_info = new TableInfo();
				new_table_info->no_of_tuples = new_num_of_tuples;

				new_table_info->table_set.insert(
						temp_table_info1->table_set.begin(),
						temp_table_info1->table_set.end());

				for (unordered_map<string, long>::iterator it =
						temp_table_info1->attributes.begin();
						it != temp_table_info1->attributes.end(); ++it) {

					new_table_info->attributes[it->first] = std::min(
							new_num_of_tuples, it->second);
				}
				for (unordered_map<string, long>::iterator it =
						temp_table_info2->attributes.begin();
						it != temp_table_info2->attributes.end(); ++it) {

					new_table_info->attributes[it->first] = std::min(
							new_num_of_tuples, it->second);
				}

				delete temp_table_info1;
				delete temp_table_info2;
				temp_table_info1 = new_table_info;
				temp_table_info2 = NULL;
			}
			orList = orList->rightOr;
			if (!orList) {
				//TODO Update the state for next and.
				updateTableInfoMaps(temp_table_info1);
				temp_table_info1 = NULL;
			}
		}

		parseTree = parseTree->rightAnd;
	}
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames,
		int numToJoin) {
	Statistics s(*this);
	s.Apply(parseTree, relNames, numToJoin);
	return s.getRowsinJoinedTableContainingGivenRelation(string(relNames[0]));
}

double Statistics::getRowsinJoinedTableContainingGivenRelation(string relName) {
	int group_number = relation_to_group_map[relName];
	TableInfo * table_info = group_to_table_info_map[group_number];
	return table_info->no_of_tuples;
}

TableInfo::TableInfo(int no_of_tuples, set<string> table_set) {
	this->no_of_tuples = no_of_tuples;
	this->table_set = table_set;
}

string remove_relation_name_from_qualified_attribute_name(
		string qualified_attribute_name) {

	if (!is_qualified_name(qualified_attribute_name)) {
		return qualified_attribute_name;
	}

	size_t pos = qualified_attribute_name.find(".", 0);

	return qualified_attribute_name.substr(pos + 1,
			qualified_attribute_name.npos);
}

string convert_to_qualified_name(string attribute_name, string relation_name) {
	return relation_name.append(".").append(
			remove_relation_name_from_qualified_attribute_name(attribute_name));
}

bool is_qualified_name(string attribute_name) {
	size_t pos = attribute_name.find(".", 0);
	if (pos == attribute_name.npos) {
		return false;
	}

	return true;
}

TableInfo* Statistics::checkIfAttributeExistsInGivenRelations(set<int> groupIds,
		Operand* op, string &relation) {
//bool found = false;
	string att_name = remove_relation_name_from_qualified_attribute_name(
			string(op->value));
	if (is_qualified_name(string(op->value))) {
		string rel = strtok((char*) string(op->value).c_str(), ".");
		int grp = relation_to_group_map[rel];
		TableInfo* tb = group_to_table_info_map[grp];
		auto it = tb->attributes.find(string(op->value));

		if (!(it == tb->attributes.end())) {
			//found = true;
			relation = rel;
			return tb;
		}
	}

	for (int groupId : groupIds) {
		TableInfo* tb = group_to_table_info_map[groupId];

		for (auto rel_name : tb->table_set) {
			auto it = tb->attributes.find(
					convert_to_qualified_name(att_name, rel_name));

			if (!(it == tb->attributes.end())) {
				//found = true;
				relation = rel_name;
				return tb;
			}

		}
	}

//if (!found) {
	cerr << "The attribute " << string(op->value)
			<< " is not present in the given relations." << endl;
	exit(-1);
//}
}

void Statistics::updateTableInfoMaps(TableInfo * table_info) {
	set<string> table_set = table_info->table_set;

	for (auto table : table_set) {
		int old_group_to_be_removed = relation_to_group_map[table];
		group_to_table_info_map.erase(old_group_to_be_removed);
		relation_to_group_map[table] = group_no;
	}

	group_to_table_info_map[group_no++] = table_info;
}

set<int> Statistics::getGroupIdsForRelations(string relation_names[],
		int number_of_relation) {
	set<int> group_ids;

	for (int i = 0; i < number_of_relation; i++) {
		group_ids.insert(relation_to_group_map[relation_names[i]]);
	}

	return group_ids;
}

void Statistics::writeStatisticsForTPCH() {

	char table_name[8][9] = { "region", "nation", "part", "supplier",
			"partsupp", "customer", "orders", "lineitem" };
	Statistics s;
	s.AddRel(table_name[0], 5);
	s.AddRel(table_name[1], 25);
	s.AddRel(table_name[2], 200000);
	s.AddRel(table_name[3], 10000);
	s.AddRel(table_name[4], 800000);
	s.AddRel(table_name[5], 150000);
	s.AddRel(table_name[6], 1500000);
	s.AddRel(table_name[7], 6001215);

	// region
	s.AddAtt(table_name[0], "r_regionkey", 5); // r_regionkey=5
	s.AddAtt(table_name[0], "r_name", 5); // r_name=5
	s.AddAtt(table_name[0], "r_comment", 5); // r_comment=5
	// nation
	s.AddAtt(table_name[1], "n_nationkey", 25); // n_nationkey=25
	s.AddAtt(table_name[1], "n_name", 25);  // n_name=25
	s.AddAtt(table_name[1], "n_regionkey", 5);  // n_regionkey=5
	s.AddAtt(table_name[1], "n_comment", 25);  // n_comment=25
	// part
	s.AddAtt(table_name[2], "p_partkey", 200000); // p_partkey=200000
	s.AddAtt(table_name[2], "p_name", 200000); // p_name=199996
	s.AddAtt(table_name[2], "p_mfgr", 200000); // p_mfgr=5
	s.AddAtt(table_name[2], "p_brand", 200000); // p_brand=25
	s.AddAtt(table_name[2], "p_type", 200000); // p_type=150
	s.AddAtt(table_name[2], "p_size", 200000); // p_size=50
	s.AddAtt(table_name[2], "p_container", 200000); // p_container=40
	s.AddAtt(table_name[2], "p_retailprice", 200000); // p_retailprice=20899
	s.AddAtt(table_name[2], "p_comment", 200000); // p_comment=127459
	// supplier
	s.AddAtt(table_name[3], "s_suppkey", 10000);
	s.AddAtt(table_name[3], "s_name", 10000);
	s.AddAtt(table_name[3], "s_address", 10000);
	s.AddAtt(table_name[3], "s_nationkey", 25);
	s.AddAtt(table_name[3], "s_phone", 10000);
	s.AddAtt(table_name[3], "s_acctbal", 9955);
	s.AddAtt(table_name[3], "s_comment", 10000);
	// partsupp
	s.AddAtt(table_name[4], "ps_partkey", 200000);
	s.AddAtt(table_name[4], "ps_suppkey", 10000);
	s.AddAtt(table_name[4], "ps_availqty", 9999);
	s.AddAtt(table_name[4], "ps_supplycost", 99865);
	s.AddAtt(table_name[4], "ps_comment", 799123);
	// customer
	s.AddAtt(table_name[5], "c_custkey", 150000);
	s.AddAtt(table_name[5], "c_name", 150000);
	s.AddAtt(table_name[5], "c_address", 150000);
	s.AddAtt(table_name[5], "c_nationkey", 25);
	s.AddAtt(table_name[5], "c_phone", 150000);
	s.AddAtt(table_name[5], "c_acctbal", 140187);
	s.AddAtt(table_name[5], "c_mktsegment", 5);
	s.AddAtt(table_name[5], "c_comment", 149965);
	// orders
	s.AddAtt(table_name[6], "o_orderkey", 1500000);
	s.AddAtt(table_name[6], "o_custkey", 99996);
	s.AddAtt(table_name[6], "o_orderstatus", 3);
	s.AddAtt(table_name[6], "o_totalprice", 1464556);
	s.AddAtt(table_name[6], "o_orderdate", 2406);
	s.AddAtt(table_name[6], "o_orderpriority", 5);
	s.AddAtt(table_name[6], "o_clerk", 1000);
	s.AddAtt(table_name[6], "o_shippriority", 1);
	s.AddAtt(table_name[6], "o_comment", 1478684);
	// lineitem
	s.AddAtt(table_name[7], "l_orderkey", 1500000);
	s.AddAtt(table_name[7], "l_partkey", 200000);
	s.AddAtt(table_name[7], "l_suppkey", 10000);
	s.AddAtt(table_name[7], "l_linenumber", 7);
	s.AddAtt(table_name[7], "l_quantity", 50);
	s.AddAtt(table_name[7], "l_extendedprice", 933900);
	s.AddAtt(table_name[7], "l_discount", 11);
	s.AddAtt(table_name[7], "l_tax", 9);
	s.AddAtt(table_name[7], "l_returnflag", 3);
	s.AddAtt(table_name[7], "l_linestatus", 2);
	s.AddAtt(table_name[7], "l_shipdate", 2526);
	s.AddAtt(table_name[7], "l_commitdate", 2466);
	s.AddAtt(table_name[7], "l_receiptdate", 2554);
	s.AddAtt(table_name[7], "l_shipinstruct", 4);
	s.AddAtt(table_name[7], "l_shipmode", 7);
	s.AddAtt(table_name[7], "l_comment", 4501941);

	s.Write("stats.txt");
}
