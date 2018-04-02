#include "Statistics.h"
#include <cstring>

Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples) {
	string rel_name(relName);
	set<string> temp_set;
	temp_set.insert(rel_name);

	TableInfo table_info(numTuples);
	table_set_look_up_map[rel_name] = temp_set;
	table_map[temp_set] = table_info;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
}
void Statistics::CopyRel(char *oldName, char *newName)
{
}
	
void Statistics::Read(char *fromWhere)
{
}
void Statistics::Write(char *fromWhere)
{
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}


AttributeInfo::AttributeInfo(char *attribute_name, int distinct_count) {
	this->attribute_name.assign(attribute_name);
	this->distinct_count = distinct_count;
}

bool AttributeInfo::operator==(const AttributeInfo &other_attribute) {
	return this->attribute_name.compare(other_attribute.attribute_name) == 0;
}

TableInfo::TableInfo(int no_of_tuples, set<string> table_set) {
		this->no_of_tuples = no_of_tuples;
		this->table_set = table_set;
}
