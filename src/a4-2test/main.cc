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

void segregateJoinsAndMultiTableSelects(vector<AndList*> &multiTableSelects);

int main() {

	yyparse();

	Statistics::writeStatisticsForTPCH();
	Statistics stats;
	stats.Read("stats.txt");

	vector<QueryPlanNode*> nodes;

	//Create select nodes for all the tables, apply any comparison/condition/filter/where-clause, and add node to the list.
	//this is a File select node.
	while (tables != NULL) {
		//the constructor of SelectFileNode will apply the corresponding comparisons in the AndList for the given table and update the statistic object.
		SelectFileNode *node = new SelectFileNode(tables, boolean, stats);
		nodes.push_back(node);
		tables = tables->next;
	}

	vector<AndList*> multiTableSelects;
	//check if joins exist.
	if (nodes.size() > 1) {
		//Now start processing Joins

		//segregate joins and selects on joins.
		segregateJoinsAndMultiTableSelects(multiTableSelects);

	}

	//process select with OR and two tables (suppose to be on a joined table), this should be done along with joins.

	//process groupby and sum

	//process projection

	//process distinct

	//process write out
}

void segregateJoinsAndMultiTableSelects(vector<AndList*> &multiTableSelects) {
	AndList* andList = boolean;
	while (andList != NULL) {
		bool hasSelects = false;
		OrList* orList = andList->left;
		while (orList != NULL) {
			ComparisonOp* cmp = orList->left;
			Operand* op1 = cmp->left;
			Operand* op2 = cmp->right;

			if (op1->code == NAME && op2->code == NAME) {
				orList = orList->rightOr;
				continue;
			}
			hasSelects = true;
			break;
		}
		if (hasSelects) {
			AndList* selectAnd = andList;

			if (andList->rightAnd != NULL) {
				andList->left = andList->rightAnd->left;
				andList->rightAnd = andList->rightAnd->rightAnd;

			} else {
				andList = andList->rightAnd;
			}
			selectAnd->rightAnd = NULL;
			multiTableSelects.push_back(selectAnd);
			continue;
		}

		andList = andList->rightAnd;

	}
}

