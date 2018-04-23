#include <iostream>
#include "ParseTree.h"
#include "QueryPlanNode.h"
#include <vector>

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

	vector<QueryPlanNode*> nodes;

	while (tables != NULL) {

		SelectFileNode *node = new SelectFileNode();

		nodes.push_back(node);
		tables = tables->next;
	}

}

