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

QueryPlanNode * constructTree(QueryPlanNode * rootUptillNow);
GroupByNode * constructGroupByNode(QueryPlanNode * child);
SumNode * constructSumNode(QueryPlanNode * child);
ProjectNode * constructProjectNode(QueryPlanNode * child);
distinctNode * constructDistinctNode(QueryPlanNode * child);
writeOutNode * constructWriteOutNode(QueryPlanNode * child);

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
	//Join nodes and join dependent selects are only possible if joins exist. Else just ignore them.
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

writeOutNode * constructWriteOutNode(QueryPlanNode * child) {
	Schema *oldSchema = child->outSchema;
	Pipe * oldPipe = child->outputPipe;
	writeOutNode * write_out_node = new writeOutNode;
	write_out_node->inputPipe = oldPipe;
	write_out_node->outSchema = oldSchema;
	write_out_node->filePointer = stdout;
	write_out_node->left = child;
	return write_out_node;
}

ProjectNode * constructProjectNode(QueryPlanNode * child) {
	Schema *oldSchema = child->outSchema;
		Pipe * oldPipe = child->outputPipe;
	ProjectNode * projectNode = new ProjectNode;
	projectNode->inputPipe = oldPipe;
	projectNode->outputPipe = new Pipe(100, ++QueryPlanNode::pipeIdCounter);

	Attribute *atts = oldSchema->GetAtts();
	int numOfAtts = oldSchema->GetNumAtts();
	vector<int> keepAttributes = *(new vector<int>()); // will keep selected attributes
	vector<Attribute> selectedAttributes;
	for (int i = 0; i < numOfAtts; i++) {
		char * attName = atts[i].name;
		NameList * tempNameList = attsToSelect;

		while (tempNameList != NULL) {
			if (!strcmp(tempNameList->name, attName)) {
				keepAttributes.push_back(i);
				selectedAttributes.push_back(atts[i]);
				break;
			}
			tempNameList = tempNameList->next;
		}
	}

	projectNode->inputPipe = oldPipe;
	projectNode->outputPipe = new Pipe(100, ++QueryPlanNode::pipeIdCounter);
	projectNode->numOfAttsOutput = keepAttributes.size();
	projectNode->numOfAttsInput = numOfAtts;
	projectNode->keepme = keepAttributes.data();

	projectNode->outSchema = new Schema("dummy", selectedAttributes.size(),
			selectedAttributes.data());

	projectNode->left = child;
	return projectNode;
}

distinctNode * constructDistinctNode(QueryPlanNode * child) {
	Schema *oldSchema = child->outSchema;
		Pipe * oldPipe = child->outputPipe;
	distinctNode * distinct_node = new distinctNode;
	distinct_node->inputPipe = oldPipe;
	distinct_node->outputPipe = new Pipe(100, ++QueryPlanNode::pipeIdCounter);
	distinct_node->outSchema = oldSchema;
	return distinct_node;
}

SumNode * constructSumNode(QueryPlanNode * child) {
	Schema *oldSchema = child->outSchema;
		Pipe * oldPipe = child->outputPipe;

	if (finalFunction == NULL) {
		return NULL;
	}

	SumNode * sumNode = new SumNode;
	sumNode->inputPipe = oldPipe;
	sumNode->outputPipe = new Pipe(100, ++QueryPlanNode::pipeIdCounter);
	sumNode->function = new Function;
	(sumNode->function)->GrowFromParseTree(finalFunction, *oldSchema);
	Attribute * att = new Attribute[1];
	att->name = "SUM";

	if (sumNode->function->returnsInt) {
		att->myType = Int;
	} else {
		att->myType = Double;
	}

	sumNode->outSchema = new Schema("dummy", 1, att);
	sumNode->left = child;
	return sumNode;
}

GroupByNode * constructGroupByNode(QueryPlanNode * child) {
	Schema *oldSchema = child->outSchema;
		Pipe * oldPipe = child->outputPipe;
	if (groupingAtts == NULL) {
		return NULL;
	}

	GroupByNode * groupByNode = new GroupByNode;
	groupByNode->function = new Function;
	(groupByNode->function)->GrowFromParseTree(finalFunction, *oldSchema);
	groupByNode->inputPipe = oldPipe;
	groupByNode->outputPipe = new Pipe(100, ++QueryPlanNode::pipeIdCounter);
	vector<Attribute> selectedAttributes = *(new vector<Attribute>());
	OrderMaker * orderMaker = new OrderMaker;

	Attribute groupBySum;
	groupBySum.name = "SUM";

	if (groupByNode->function->returnsInt) {
		groupBySum.myType = Int;
	} else {
		groupBySum.myType = Double;
	}

	selectedAttributes.push_back(groupBySum);

	Attribute * oldAtts = oldSchema->GetAtts();
	int oldAttsLen = oldSchema->GetNumAtts();

	NameList * tempNameList = groupingAtts;

	while (tempNameList != NULL) {
		int i = 0;
		for (; i < oldAttsLen; i++) {
			if (!strcmp(tempNameList->name, oldAtts[i].name)) {
				selectedAttributes.push_back(oldAtts[i]);
				break;
			}
		}
		orderMaker->whichAtts[orderMaker->numAtts] = i;
		orderMaker->whichTypes[(orderMaker->numAtts)++] = oldAtts[i].myType;
		tempNameList = tempNameList->next;
	}

	groupByNode->orderMaker = orderMaker;
	groupByNode->outSchema = new Schema("dummy", selectedAttributes.size(), selectedAttributes.data());

	groupByNode->left = child;
	return groupByNode;
}

QueryPlanNode * constructTree(QueryPlanNode * rootUptillNow) {
	QueryPlanNode * root = rootUptillNow;

	GroupByNode * groupByNode = constructGroupByNode(root);
	if (groupByNode != NULL) {
		root = groupByNode;
		root = constructProjectNode(root);
	} else {
		SumNode * sumNode = constructSumNode(root);
		if (sumNode != NULL) {
			root = sumNode;
		} else {
			root = constructProjectNode(root);
		}
	}

	// at this point root node either points to project node or sum node.
	if (distinctAtts || distinctFunc) {
		root = constructDistinctNode(root);
	}

	return constructWriteOutNode(root);
}
