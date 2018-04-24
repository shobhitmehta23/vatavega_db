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
public:
	QueryPlanNode(QueryNodeType nodeType);
	virtual ~QueryPlanNode();
	virtual void printNode();
	void printQueryTree();

	Schema* outSchema = NULL;
	QueryPlanNode * left = NULL;
	QueryPlanNode * right = NULL;
	QueryNodeType nodeType;
	static int pipeIdCounter;
	Pipe *outputPipe;
	CNF cnf;

};

class SelectFileNode: public QueryPlanNode {
private:
	//identifies and applies the conditions in the where clause for the given table for file select operation.
	void applySelectCondition(AndList* andList, Statistics &stats);
public:
	DBFile inputFile;
	TableList* table;
	TableInfo* tableInfo;
	CNF selectCNF;
	double estimate;

	SelectFileNode(TableList* tbl, AndList* andList, Statistics &stats);
	~SelectFileNode() {
	}
	;
	void printNode();

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
};

class SelectPipeNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	CNF selectPipeCNF;

	SelectPipeNode(vector<AndList*> multiTableSelects, JoinNode* node,
			Statistics &stats);
	~SelectPipeNode() {
	}
	;
	void printNode();
};

class GroupByNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	Pipe *outputPipe;
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
};
class SumNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	Pipe *outputPipe;
	Function * function;
	SumNode() :
			QueryPlanNode(Sum_Node) {
	}
	~SumNode() {
	}
	;
	void printNode();
};
class ProjectNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	Pipe *outputPipe;
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
};
class distinctNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	Pipe *outputPipe;
	distinctNode() :
			QueryPlanNode(distinct_Node) {
	}
	;
	~distinctNode() {
	}
	;
	void printNode();
};
class writeOutNode: public QueryPlanNode {
public:
	Pipe *inputPipe;
	FILE *filePointer;
	writeOutNode() :
			QueryPlanNode(writeOut_Node) {
	}
	;
	~writeOutNode() {
	}
	;
	void printNode();
};
#endif /* QUERYPLANNODE_H_ */
