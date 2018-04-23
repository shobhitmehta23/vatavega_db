/*
 * QueryPlanNode.h
 *
 *  Created on: Apr 22, 2018
 *
 */

#ifndef QUERYPLANNODE_H_
#define QUERYPLANNODE_H_

#include "DBFile.h"
#include "Schema.h"
#include "ParseTree.h"

class QueryPlanNode {
public:
	QueryPlanNode();
	virtual ~QueryPlanNode();
	virtual void printNode() =0;

	string outPipeName;
	Schema outSchema;

};

class SelectFileNode: public QueryPlanNode {
	SelectFileNode();
	~SelectFileNode();
	void printNode();
	DBFile inputFile;
	TableList table;

};

class SelectPipeNode: public QueryPlanNode {
	SelectPipeNode();
	~SelectPipeNode();
	void printNode();
};

class JoinNode: public QueryPlanNode {
	JoinNode();
	~JoinNode();
	void printNode();
};

class GroupByNode: public QueryPlanNode {
	GroupByNode();
	~GroupByNode();
	void printNode();
};
class SumNode: public QueryPlanNode {
	SumNode();
	~SumNode();
	void printNode();
};
class ProjectNode: public QueryPlanNode {
	ProjectNode();
	~ProjectNode();
	void printNode();
};
class distinctNode: public QueryPlanNode {
	distinctNode();
	~distinctNode();
	void printNode();
};
#endif /* QUERYPLANNODE_H_ */
