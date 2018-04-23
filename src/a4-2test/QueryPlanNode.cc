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
	AndList* prev = ands;
	string tableName = string(
			table->aliasAs == NULL ? table->tableName : table->aliasAs);
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
				Operand* nameOperand;
				if (left->code == NAME) {
					nameOperand = left;
				} else {
					nameOperand = right;
				}

				auto it = tableInfo->attributes.find(
						string(nameOperand->value));

				if ((it == tableInfo->attributes.end())) {
					auto it2 = tableInfo->attributes.find(
							tableName + "." + string(nameOperand->value));
					if ((it == tableInfo->attributes.end())) {
						goToNextAnd = true;
						break;
					}
				}

			}
			ors = ors.rightOr;
		}
		if (goToNextAnd) {
			ands = ands->rightAnd;
			continue;
		}
		//just to handle the first time OR
		if (currentAndListPtr->left == NULL) {
			currentAndListPtr->left = ands->left;
		} else {
			currentAndListPtr->rightAnd = ands;
			currentAndListPtr = currentAndListPtr->rightAnd;
		}

		if (ands->rightAnd != NULL) {
			ands->left = ands->rightAnd->left;
			ands->rightAnd = ands->rightAnd->rightAnd;
		} else {
			break; // ands->rightAnd == NULL
		}

	}
	currentAndListPtr->rightAnd = NULL;

	char * ch[] = {tableName.c_str()};
	//calculate and apply the estimates
	stats.Apply(currentAndListPtr, ch, 1);
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
