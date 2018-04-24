#include <iostream>
#include <vector>
#include <limits>
#include "ParseTree.h"
#include "QueryPlanNode.h"
#include "Statistics.h"
#include "AndListPermutationIterator.h"

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
TableInfo* getTableInfo(QueryPlanNode* node);
void popNextTwoJoinNodes(vector<QueryPlanNode*> copyList, AndList* clause,
		QueryPlanNode* node1, QueryPlanNode* node2);
void findAndApplyBestJoinPlan(vector<QueryPlanNode*> nodes,
		AndList* joinConditions, Statistics &stats);

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

		//permute and find best join plan
		findAndApplyBestJoinPlan(nodes, boolean, stats);
		//process select with OR and two tables (suppose to be on a joined table), this should be done along with joins.
		SelectPipeNode *node = new SelectPipeNode(multiTableSelects,
				(JoinNode*) nodes.at(0), stats);
		nodes.pop_back();
		nodes.push_back(node);

	}

	//process groupby and sum

	//process projection

	//process distinct

	//process write out

	constructTree(nodes.at(0));
}

void findAndApplyBestJoinPlan(vector<QueryPlanNode*> nodes,
		AndList* joinConditions, Statistics &stats) {
	//Get the next permutation
	AndList* bestPermutation = NULL;
	double estimate = std::numeric_limits<double>::max();
	AndListPermutationIterator permutations(joinConditions);
	AndList* permutation = permutations.getNext();

	while (permutation != NULL) {
		Statistics st(stats);
		vector<QueryPlanNode*> copyList(nodes);
		double temp_est = 0;
		AndList* andList = permutation;
		while (andList != NULL) {
			QueryPlanNode* node1;
			QueryPlanNode* node2;
			popNextTwoJoinNodes(copyList, andList, node1, node2);
			JoinNode* new_join_node = new JoinNode(node1, node2, false, st,
					andList);
			copyList.push_back(new_join_node);
			temp_est += new_join_node->estimate;
			if (temp_est >= estimate) {
				break;
			}

			andList = andList->rightAnd;
		}

		if (estimate > temp_est) {

			//check if there are more than one nodes left in the list, if yes, do the cross product.
			while (copyList.size() > 1) {
				//cross product
				QueryPlanNode* node1;
				QueryPlanNode* node2;
				node1 = copyList.back();
				copyList.pop_back();
				node2 = copyList.back();
				copyList.pop_back();
				JoinNode* new_join_node = new JoinNode(node1, node2, false, st,
				NULL);
				temp_est += new_join_node->estimate;
				copyList.push_back(new_join_node);

			}
			if (estimate > temp_est) {
				estimate = temp_est;
				if (bestPermutation != NULL) {
					permutations.destroyPermutation(bestPermutation);
				}
				bestPermutation = permutation;
			} else {
				permutations.destroyPermutation(permutation);
			}

		}
		permutations.getNext();
	}

	//Apply the best permutation:

	while (bestPermutation != NULL) {
		QueryPlanNode* node1;
		QueryPlanNode* node2;
		popNextTwoJoinNodes(nodes, bestPermutation, node1, node2);
		JoinNode* new_join_node = new JoinNode(node1, node2, false, stats,
				bestPermutation);
		nodes.push_back(new_join_node);

		bestPermutation = bestPermutation->rightAnd;
	}
	while (nodes.size() > 1) {
		//cross product
		QueryPlanNode* node1;
		QueryPlanNode* node2;
		node1 = nodes.back();
		nodes.pop_back();
		node2 = nodes.back();
		nodes.pop_back();
		JoinNode* new_join_node = new JoinNode(node1, node2, false, stats,
		NULL);

		nodes.push_back(new_join_node);

	}

}

//returns the two nodes from the vector involving the given AND condition.
void popNextTwoJoinNodes(vector<QueryPlanNode*> copyList, AndList* clause,
		QueryPlanNode* node1, QueryPlanNode* node2) {
	ComparisonOp* op = clause->left->left;
	Operand* left = op->left;
	Operand* right = op->right;

	bool node1Found = false;
	bool node2Found = false;

	for (int i = 0; i < copyList.size(); i++) {
		QueryPlanNode* node = copyList.at(i);
		TableInfo* tbl_info = getTableInfo(node);
		if (!node1Found) {

			auto it = tbl_info->attributes.find(string(left->value));

			if (!(it == tbl_info->attributes.end())) {
				node1Found = true;
				node1 = node;
				copyList.erase(copyList.begin() + i);
				i--;
				continue;
			}
		}
		if (!node2Found) {
			auto it = tbl_info->attributes.find(string(right->value));

			if (!(it == tbl_info->attributes.end())) {
				node2Found = true;
				node2 = node;
				copyList.erase(copyList.begin() + i);
				i--;
				continue;
			}
		}

	}

}

TableInfo * getTableInfo(QueryPlanNode * node) {
	if (node->nodeType == Select_File_Node) {
		SelectFileNode* n = (SelectFileNode*) node;
		return n->tableInfo;
	}
	JoinNode* n = (JoinNode*) node;
	return n->tableInfo;
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
	groupByNode->outSchema = new Schema("dummy", selectedAttributes.size(),
			selectedAttributes.data());

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
