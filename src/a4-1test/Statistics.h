#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <set>


using namespace std;

class AttributeInfo {
public:
	string attribute_name;
	int distinct_count;

	AttributeInfo(char *attribute_name, int distinct_count);
	bool operator==(const AttributeInfo &other_attribute);
};


class TableInfo {
public:
	int no_of_tuples;
	set<string> table_set;
	unordered_map<string, AttributeInfo> attributes;
	TableInfo() {};
	TableInfo(int no_of_tuples, set<string> table_set);
};

struct SetHasher {
  std::size_t operator()(const std::set<string>& k) const {
	string temp = "";

    for (auto x: k) {
    		temp += x;
    }

    return hash<string>()(temp);
  }
};

class Statistics {

private:
	unordered_map<set<string>, TableInfo, SetHasher> table_map;
	unordered_map<string, set<string>> table_set_look_up_map;

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
