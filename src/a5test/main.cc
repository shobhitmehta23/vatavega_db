#include <iostream>
#include <cstdlib>
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

extern int dbFileType; //0 for heap, 1 for sorted file.
extern int isDDLQuery; //Set to 1 if  the query is of type CREATE, DROP, INSERT, etc.
//extern int isCreateTableQuery = 0; //set to 1 for create table.
//extern int isInsertTableQuery = 0; //set to 1 for insert table.
//extern int isDropTableQuery = 0; //set to 1 for drop table.
//extern int isSetOutputQuery = 0; //set to 1 for set output mode.
extern string DDLQueryTableName;
extern struct CreateAttributes;
extern vector<CreateAttributes> createAttrList;
extern string filePath;
extern int outputMode; //0 is default for STDOUT, 1 for output to file, 2 for NONE - i.e. print query plan

QueryPlanNode * constructTree(QueryPlanNode * rootUptillNow);
GroupByNode * constructGroupByNode(QueryPlanNode * child);
SumNode * constructSumNode(QueryPlanNode * child);
ProjectNode * constructProjectNode(QueryPlanNode * child);
DistinctNode * constructDistinctNode(QueryPlanNode * child);
WriteOutNode * constructWriteOutNode(QueryPlanNode * child);

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

void segregateJoinsAndMultiTableSelects(vector<AndList*> &multiTableSelects);
TableInfo* getTableInfo(QueryPlanNode* node);
void popNextTwoJoinNodes(vector<QueryPlanNode*> &copyList, AndList* clause,
		QueryPlanNode* &node1, QueryPlanNode* &node2);
void findAndApplyBestJoinPlan(vector<QueryPlanNode*> &nodes,
		AndList* joinConditions, Statistics &stats);

char *test1 = "SELECT SUM (ps.ps_supplycost), s.s_suppkey \ 
FROM part AS p, supplier AS s, partsupp AS ps \
WHERE (p.p_partkey = ps.ps_partkey) AND \
(s.s_suppkey = ps.ps_suppkey) AND (s.s_acctbal > 2500) \
GROUP BY s.s_suppkey";

char *test2 =
		"SELECT SUM (c.c_acctbal),c.c_name \
		FROM customer AS c, orders AS o \
		WHERE (c.c_custkey = o.o_custkey) AND (o.o_totalprice < 10000) \
		GROUP BY c.c_name";

char *test3 =
		"SELECT l.l_orderkey, l.l_partkey, l.l_suppkey \
		FROM lineitem AS l \
		WHERE (l.l_returnflag = 'R') AND \ 
		      (l.l_discount < 0.04 OR l.l_shipmode = 'MAIL')";


char *test4 =
		"SELECT DISTINCT c1.c_name, c1.c_address, c1.c_acctbal \ 
		FROM customer AS c1, customer AS c2  \
		WHERE (c1.c_nationkey = c2.c_nationkey) AND \
			  (c1.c_name ='Customer#000070919')";


char *test5 = "SELECT SUM(l.l_discount) \
FROM customer AS c, orders AS o, lineitem AS l \
WHERE (c.c_custkey = o.o_custkey) AND \
      (o.o_orderkey = l.l_orderkey) AND \
      (c.c_name = 'Customer#000070919') AND \ 
	  (l.l_quantity > 30) AND (l.l_discount < 0.03)";

char *test6 =
		"SELECT l.l_orderkey \
		FROM lineitem AS l \
		WHERE (l.l_quantity > 30)";

char *test7 =
		"SELECT DISTINCT c.c_name \
		FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r \
		WHERE (l.l_orderkey = o.o_orderkey) AND \
		      (o.o_custkey = c.c_custkey) AND \
			  (c.c_nationkey = n.n_nationkey) AND \
			  (n.n_regionkey = r.r_regionkey)";

char *test8 =
		"SELECT l.l_discount \
		FROM lineitem AS l, orders AS o, customer AS c, nation AS n, region AS r \ 
		WHERE (l.l_orderkey = o.o_orderkey) AND \
			  (o.o_custkey = c.c_custkey) AND \
			  (c.c_nationkey = n.n_nationkey) AND \
			  (n.n_regionkey = r.r_regionkey) AND \
			  (r.r_regionkey = 1) AND (o.o_orderkey < 10000)";



char *test9 =
		"SELECT SUM (l.l_discount) \
		FROM customer AS c, orders AS o, lineitem AS l \
		WHERE (c.c_custkey = o.o_custkey) AND (o.o_orderkey = l.l_orderkey) AND \
			  (c.c_name = 'Customer#000070919') AND (l.l_quantity > 30) AND \
			  (l.l_discount < 0.03)";

char *test10 =
		"SELECT SUM (l.l_extendedprice * l.l_discount) \
	FROM lineitem AS l \
	WHERE (l.l_discount<0.07) AND (l.l_quantity < 24)";

char * tests[10] = {test1, test2, test3, test4, test5, test6, test7, test8, test9, test10};

int main(int args, char** argv) {

	if (args == 1) {
		yyparse();
	} else {
		yy_scan_string (tests[atoi(argv[1]) - 1]);
		yyparse();
	}

	Statistics::writeStatisticsForTPCH();
	Statistics stats;
	stats.Read("stats.txt");

	//If DDL query, e.g. CREATE, INSERT, DROP or SET OUTPUT.
	if (isDDLQuery > 0) {
		switch (isDDLQuery) {
		case 1:   // CREATE
			cout << "Table name: " << DDLQueryTableName << endl;
			cout << "DB file type: " << dbFileType << endl;
			cout << "New Attributes: " << dbFileType << endl;
			for (struct CreateAttributes c : createAttrList) {
				cout << "---create attr::  " << c.name << "  :  " << c.type
						<< endl;
			}

			if (dbFileType == 1) {
				NameList * tempNameList = attsToSelect;
				cout << " Sorted file on attributes: " << dbFileType << endl;
				while (tempNameList != NULL) {
					cout << "---attr::  " << tempNameList->name << endl;
					tempNameList = tempNameList->next;
				}
			}
			break;
		case 2:   //INSERT INTO
			cout << "Table name: " << DDLQueryTableName << endl;
			cout << "File path: " << filePath << endl;
			break;
		case 3:   // DROP TABLE
			cout << "Table name: " << DDLQueryTableName << endl;
			break;
		case 4:   // SET OUTPUT
			cout << "outputMode: " << outputMode << endl;
			break;
		default:
			break;

		}
	} else {

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

			if (multiTableSelects.size() > 0) {
				SelectPipeNode *node = new SelectPipeNode(multiTableSelects,
						(JoinNode*) nodes.at(0), stats);
				nodes.pop_back();
				nodes.push_back(node);
			}

		}

		//process groupby and sum

		//process projection

		//process distinct

		//process write out
		//cout<<"nodes.size(): "<< nodes.size()<<endl;
		//nodes.at(0)->printQueryTree();

		QueryPlanNode* root = constructTree(nodes.at(0));
		root->printQueryTree();
	}

}

void findAndApplyBestJoinPlan(vector<QueryPlanNode*> &nodes,
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
			AndList* query = new AndList();
			query->left = andList->left;
			JoinNode* new_join_node = new JoinNode(node1, node2, false, st,
					query);
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
		permutation = permutations.getNext();
	}

	//Apply the best permutation:

	while (bestPermutation != NULL) {
		QueryPlanNode* node1;
		QueryPlanNode* node2;
		popNextTwoJoinNodes(nodes, bestPermutation, node1, node2);
		SelectFileNode* n1 = (SelectFileNode*) node1;
		SelectFileNode* n2 = (SelectFileNode*) node2;
		AndList* query = new AndList();
		query->left = bestPermutation->left;
		JoinNode* new_join_node = new JoinNode(node1, node2, true, stats,
				query);
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
		JoinNode* new_join_node = new JoinNode(node1, node2, true, stats,
		NULL);

		nodes.push_back(new_join_node);

	}

}

//returns the two nodes from the vector involving the given AND condition.
void popNextTwoJoinNodes(vector<QueryPlanNode*> &copyList, AndList* clause,
		QueryPlanNode* &node1, QueryPlanNode* &node2) {
	ComparisonOp* op = clause->left->left;
	Operand* left = op->left;
	Operand* right = op->right;

	bool node1Found = false;
	bool node2Found = false;

	for (int i = 0; i < copyList.size(); i++) {
		QueryPlanNode* node = copyList.at(i);
		TableInfo* tbl_info = getTableInfo(node);
		if (!node1Found) {
			string str = string(left->value);
			auto it = tbl_info->attributes.find(str);

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

		if (node2Found && node1Found) {
			break;
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
	AndList* prev = NULL;
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
			AndList* selectAnd = new AndList();
			selectAnd->left = andList->left;
			multiTableSelects.push_back(selectAnd);

			if (andList->rightAnd != NULL) {
				andList->left = andList->rightAnd->left;
				andList->rightAnd = andList->rightAnd->rightAnd;

			} else {
				if (prev != NULL) {
					prev->rightAnd = NULL;
				}
				andList = andList->rightAnd;
			}

			continue;
		}
		prev = andList;
		andList = andList->rightAnd;

	}
}

WriteOutNode * constructWriteOutNode(QueryPlanNode * child) {
	Schema *oldSchema = child->outSchema;
	Pipe * oldPipe = child->outputPipe;
	WriteOutNode * write_out_node = new WriteOutNode;
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

	// incase of group by, sum would not be selected if we do not do this.
	if (groupingAtts != NULL && finalFunction != NULL) {
		keepAttributes.push_back(0);
		selectedAttributes.push_back(atts[0]);
	}

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

	projectNode->numOfAttsOutput = keepAttributes.size();
	projectNode->numOfAttsInput = numOfAtts;
	projectNode->keepme = keepAttributes.data();

	projectNode->outSchema = new Schema("dummy", selectedAttributes.size(),
			selectedAttributes.data());

	projectNode->left = child;
	return projectNode;
}

DistinctNode * constructDistinctNode(QueryPlanNode * child) {
	Schema *oldSchema = child->outSchema;
	Pipe * oldPipe = child->outputPipe;
	DistinctNode * distinct_node = new DistinctNode;
	distinct_node->inputPipe = oldPipe;
	distinct_node->outputPipe = new Pipe(100, ++QueryPlanNode::pipeIdCounter);
	distinct_node->outSchema = oldSchema;
	distinct_node->left = child;
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
