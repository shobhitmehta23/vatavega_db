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

void Statistics::AddRel(char *relName, int numTuples)
{
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
