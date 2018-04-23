#ifndef QUERYPLANNODE_H_
#define QUERYPLANNODE_H_

#include "DBFile.h"
#include "Schema.h"
#include "ParseTree.h"
#include "Statistics.h"

class QueryPlanNode {
private:
	void printQueryTreeHelper(QueryPlanNode *queryPlanNode);
public:
	QueryPlanNode();
	virtual ~QueryPlanNode();
	virtual void printNode() =0;
	void printQueryTree();

	string outPipeName;
	Schema outSchema;
	QueryPlanNode * left = NULL;
	QueryPlanNode * right = NULL;

};

class SelectFileNode: public QueryPlanNode {
private:
	//identifies and applies the conditions in the where clause for the given table for file select operation.
	void applySelectCondition(AndList* andList, Statistics &stats);
public:
	DBFile inputFile;
	TableList* table;
	TableInfo* tableInfo;

	SelectFileNode(TableList* tbl, AndList* andList, Statistics &stats) {
		this->table = tbl;
		char* tableName =
				table->aliasAs == NULL ? table->tableName : table->aliasAs;
		if (table->aliasAs != NULL) {
			stats.CopyRel(table->tableName, table->aliasAs);
		}
		this->tableInfo =
				stats.group_to_table_info_map[stats.relation_to_group_map[string(
						tableName)]];
		applySelectCondition(andList, stats);
	}
	~SelectFileNode();
	void printNode();

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
