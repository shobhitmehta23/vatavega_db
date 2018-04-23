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

void SelectFileNode::applySelectCondition(AndList* andList, Statistics &stats) {
	AndList* ands = andList;

	AndList* selectAndList = new AndList();
	AndList* currentAndListPtr = selectAndList;
	while (ands != NULL) {
		bool goToNextAnd = false;
		OrList ors = ands->left;
		while (ors != NULL) {
			if (ors.left != NULL) {
				Operand *left = ors.left->left;
				Operand *right = ors.left->right;

				if (left->code == NAME && right->code == NAME) {
					goToNextAnd = true;
					break;
				}

			}
			ors = ors.rightOr;
		}
		if (goToNextAnd) {
			continue;
		}
		currentAndListPtr->left = ands->left;
		ands = ands->rightAnd;
	}
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
