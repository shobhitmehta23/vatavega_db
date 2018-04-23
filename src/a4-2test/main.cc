#include <iostream>
#include <vector>
#include "ParseTree.h"
#include "QueryPlanNode.h"
#include "Statistics.h"

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

int main() {

	yyparse();

	Statistics stats;

	vector<QueryPlanNode*> nodes;

	//Create select nodes for all the tables, apply any comparison/condition/filter/where-clause, and add node to the list.
	//this is a File select node.
	while (tables != NULL) {
		//the constructor of SelectFileNode will apply the corresponding comparisons in the AndList for the given table and update the statistic object.
		SelectFileNode *node = new SelectFileNode(tables, boolean, stats);
		nodes.push_back(node);
		tables = tables->next;
	}

	//Now start processing Joins

}

