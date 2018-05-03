#ifndef QUERYPLANNODE_H_
#define QUERYPLANNODE_H_

#include "DBFile.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Pipe.h"
#include "Function.h"
#include<vector>

enum QueryNodeType {
	Select_File_Node,
	Select_Pipe_Node,
	Join_Node,
	GroupBy_Node,
	Sum_Node,
	Project_Node,
	distinct_Node,
	writeOut_Node
};
class QueryPlanNode {
private:
	void printQueryTreeHelper(QueryPlanNode *queryPlanNode);
	void executeQueryTreeHelper(QueryPlanNode *queryPlanNode);
public:
	QueryPlanNode(QueryNodeType nodeType);
	virtual ~QueryPlanNode();
	virtual void printNode();
	virtual void executeNode();
	void printQueryTree();
	void executeQueryTree();

	Schema* outSchema = NULL;
	QueryPlanNode * left = NULL;
	QueryPlanNode * right = NULL;
	QueryNodeType nodeType;
	static int pipeIdCounter;
	Pipe *outputPipe;
	CNF* cnf;
	Record* literal;

};

class SelectFileNode: public QueryPlanNode {
private:
	//identifies and applies the conditions in the where clause for the given table for file select operation.
	void applySelectCondition(AndList* andList, Statistics &stats);
public:
	DBFile inputFile;
	TableList* table;
	TableInfo* tableInfo;
	double estimate;

	SelectFileNode(TableList* tbl, AndList* andList, Statistics &stats);
	~SelectFileNode() {
	}
	;
	void printNode();
	void executeNode();

};

class JoinNode: public QueryPlanNode {
public:
	Pipe *inputPipe1;
	Pipe *inputPipe2;
	double estimate;
	vector<char*> relNames;
	TableInfo* tableInfo;

	JoinNode(QueryPlanNode* node1, QueryPlanNode* node2, bool doApply,
			Statistics &stats, AndList* query);
	~JoinNode() {
	}
	void printNode();
	void executeNode();
};

class SelectPipeNode: public QueryPlanNode {
public:
	Pipe *inputPipe;

	SelectPipeNode(vector<AndList*> multiTableSelects, JoinNode* node,
			Statistics &stats);
	~SelectPipeNode() {
	}
	;
	void printNode();
	void executeNode();
};

class GroupByNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	OrderMaker * orderMaker;
	Function * function;
	GroupByNode() :
			QueryPlanNode(GroupBy_Node) {
	}
	;
	~GroupByNode() {
	}
	;
	void printNode();
	void executeNode();
};
class SumNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	Function * function;
	SumNode() :
			QueryPlanNode(Sum_Node) {
	}
	~SumNode() {
	}
	;
	void printNode();
	void executeNode();
};
class ProjectNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	int *keepme;
	int numOfAttsOutput;
	int numOfAttsInput;
	ProjectNode() :
			QueryPlanNode(Project_Node) {
	}
	~ProjectNode() {
	}
	;
	void printNode();
	void executeNode();
};
class DistinctNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	DistinctNode() :
			QueryPlanNode(distinct_Node) {
	}
	;
	~DistinctNode() {
	}
	;
	void printNode();
	void executeNode();
};
class WriteOutNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	FILE *filePointer;
	WriteOutNode() :
			QueryPlanNode(writeOut_Node) {
	}
	;
	~WriteOutNode() {
	}
	;
	void printNode();
	void executeNode();
};
#endif /* QUERYPLANNODE_H_ */
