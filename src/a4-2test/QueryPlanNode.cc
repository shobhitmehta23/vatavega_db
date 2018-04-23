#include "QueryPlanNode.h"

void printSchema(Schema& schema);
void printNodeBoundary();
void printPipe(Pipe *pipe, bool ifInputPipe);

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
	printNodeBoundary();
	cout << "SELECT FILE OPERATION ON  " << string(table->tableName) << endl;
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	// print cnf
	printNodeBoundary();
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
		OrList* ors = ands->left;
		while (ors != NULL) {
			if (ors->left != NULL) {
				Operand *left = ors->left->left;
				Operand *right = ors->left->right;

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
			ors = ors->rightOr;
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
		//remove the AND clause from the Andlist.
		if (ands->rightAnd != NULL) {
			ands->left = ands->rightAnd->left;
			ands->rightAnd = ands->rightAnd->rightAnd;
		} else {
			break; // ands->rightAnd == NULL
		}

	}
	currentAndListPtr->rightAnd = NULL;

	char * ch[] = { (char*) tableName.c_str() };
	//calculate and apply the estimates
	stats.Apply(currentAndListPtr, ch, 1);

	// suck up the schema from the file
	Schema* sch = new Schema("catalog", table->tableName); //FIXME hardcoded schema name
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree(currentAndListPtr, sch, literal);
	outSchema = sch;
}

void SelectPipeNode::printNode() {
	printNodeBoundary();
	cout << "SELECT PIPE OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	// print cnf
	printNodeBoundary();
}

void JoinNode::printNode() {
	printNodeBoundary();
	cout << "JOIN OPERATION " << endl;
	printPipe(inputPipe1, true);
	printPipe(inputPipe2, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	// print cnf
	printNodeBoundary();
}

void GroupByNode::printNode() {
	printNodeBoundary();
	cout << "GROUPBY OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << "order maker: " << endl;
	orderMaker->Print();
	cout << "function: " << endl;
	function->Print();
	printNodeBoundary();
}

void SumNode::printNode() {
	printNodeBoundary();
	cout << "SUM OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << "function: " << endl;
	function->Print();
	printNodeBoundary();
}

void ProjectNode::printNode() {
	printNodeBoundary();
	cout << "PROJECT OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << "attributes to keep" << endl;
	for (int i = 0; i < numOfAttsOutput; i++) {
		cout << keepme[i] << '\t';
	}
	cout << endl;
	printNodeBoundary();
}

void distinctNode::printNode() {
	printNodeBoundary();
	cout << "DUPLICATE REMOVAL OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	printNodeBoundary();
}

void writeOutNode::printNode() {
	printNodeBoundary();
	cout << "WRITE OUT OPERATION " << endl;
	printPipe(inputPipe, true);
	printSchema(*outSchema);
	printNodeBoundary();
}

void printSchema(Schema& schema) {
	cout << "Output Schema" << endl;
	int numberOfAtt = schema.GetNumAtts();

	Attribute *att = schema.GetAtts();
	for (int i = 0; i < numberOfAtt; i++) {
		string temp = "Att" + to_string((i + 1)) + ":\t";

		switch (att[i].myType) {
		case Double:
			cout << "double ";
			break;
		case Int:
			cout << "int ";
			break;
		case String:
			cout << "string ";
			break;
		}
	}
}

void printPipe(Pipe *pipe, bool ifInputPipe) {
	if (ifInputPipe) {
		cout << "Input pipe ID ";
	} else {
		cout << "Output pipe ID ";
	}
	cout << pipe->getPipeId() << endl;
}

void printNodeBoundary() {
	cout << "***************************" << endl;
}
