#include "QueryPlanNode.h"

QueryPlanNode::QueryPlanNode() {
	// TODO Auto-generated constructor stub

}

QueryPlanNode::~QueryPlanNode() {
	// TODO Auto-generated destructor stub
}

void QueryPlanNode::printQueryTree() {
	printQueryTreeHelper(this);
}

void QueryPlanNode::printQueryTreeHelper(QueryPlanNode *queryPlanNode) {
	if (queryPlanNode == NULL) {
		return;
	}

	printQueryTreeHelper(queryPlanNode->left);
	queryPlanNode->printNode();
	printQueryTreeHelper(queryPlanNode->right);
}

void SelectFileNode::printNode() {

}

void SelectPipeNode::printNode() {

}

void JoinNode::printNode() {

}

void GroupByNode::printNode() {

}

void SumNode::printNode() {

}

void ProjectNode::printNode() {

}

void distinctNode::printNode() {

}
