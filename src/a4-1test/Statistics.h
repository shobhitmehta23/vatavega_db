#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <set>


using namespace std;

class Statistics
{

private:

	set<string> tables;
	unordered_map<set<string>, TableInfo, >
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
	unordered_map<string, AttributeInfo> attributes;
};

/*struct SetHasher
{
  std::size_t operator()(const std::set<string>& k) const
  {
    using std::size_t;
    using std::hash;
    using std::string;

    return ((hash<string>()(k.first)
             ^ (hash<string>()(k.second) << 1)) >> 1)
             ^ (hash<int>()(k.third) << 1);

    for ()
  }
};*/
#endif
