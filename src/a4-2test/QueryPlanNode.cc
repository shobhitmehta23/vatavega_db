#include "QueryPlanNode.h"
#include <cstring>

void printSchema(Schema& schema);
void printNodeBoundary();
void printPipe(Pipe *pipe, bool ifInputPipe);
char* getRelationName(TableList* table);
char * mergeThreeString(char *str1, char *str2, char *str3);

int QueryPlanNode::pipeIdCounter = 0;
QueryPlanNode::QueryPlanNode(QueryNodeType nodeType) {
	this->nodeType = nodeType;
}

QueryPlanNode::~QueryPlanNode() {
	// TODO Auto-generated destructor stub
}

void QueryPlanNode::printQueryTree() {
	printQueryTreeHelper(this);
}
void QueryPlanNode::printNode() {

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
	cout << "Select CNF" << endl;
	selectCNF.Print();
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
	int numberOfAtt = sch->GetNumAtts();
	Attribute * attributes = sch->GetAtts();

	for (int i = 0; i < numberOfAtt; i++) {
		char * tempName = mergeThreeString(table->aliasAs, ".",
				attributes[i].name);
		attributes[i].name = tempName;
	}

	Record literal;
	cnf.GrowFromParseTree(currentAndListPtr, sch, literal);
	outSchema = sch;
}

void SelectPipeNode::printNode() {
	printNodeBoundary();
	cout << "SELECT PIPE OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << "Select CNF" << endl;
	cnf.Print();
	printNodeBoundary();
}

void JoinNode::printNode() {
	printNodeBoundary();
	cout << "JOIN OPERATION " << endl;
	printPipe(inputPipe1, true);
	printPipe(inputPipe2, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << "Select CNF" << endl;
	cnf.Print();
	printNodeBoundary();
}

JoinNode::JoinNode(QueryPlanNode* node1, QueryPlanNode* node2, bool doApply,
		Statistics &stats, AndList* query) :
		QueryPlanNode(Join_Node) {
	inputPipe1 = node1->outputPipe;
	inputPipe2 = node2->outputPipe;
	estimate = 0;
	vector<char*> relations;

	if (node1->nodeType == Select_File_Node) {
		SelectFileNode* node = (SelectFileNode*) node1;
		relations.push_back(getRelationName(node->table));
	} else if (node1->nodeType == Join_Node) {
		JoinNode* node = (JoinNode*) node1;
		for (char* tbl : node->relNames) {
			relations.push_back(tbl);
		}
	}
	char** arr;
	std::copy(relations.begin(), relations.end(), arr);

	estimate = stats.Estimate(query, arr, relations.size());
	relNames = relations;

	//only execute while applying permanent changes.
	if (doApply) {
		stats.Apply(query, arr, relations.size());

		outputPipe = new Pipe(100, ++pipeIdCounter);

		int numberOfAtt1 = node1->outSchema->GetNumAtts();
		int numberOfAtt2 = node2->outSchema->GetNumAtts();
		Attribute attributes[numberOfAtt1 + numberOfAtt2];
		Attribute * attributes1 = node1->outSchema->GetAtts();

		for (int i = 0; i < numberOfAtt1; i++) {
			attributes[i] = attributes1[i];
		}
		for (int i = numberOfAtt1; i < numberOfAtt2 + numberOfAtt1; i++) {
			attributes[i] = attributes1[i - numberOfAtt1];
		}

		outSchema = new Schema(
				(char*) std::to_string(outputPipe->getPipeId()).c_str(),
				numberOfAtt2 + numberOfAtt1, attributes);

		Record literal;
		cnf.GrowFromParseTree(query, outSchema, literal);
		left = node1;
		right = node2;
	}

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

char* getRelationName(TableList* table) {
	return table->aliasAs == NULL ? table->tableName : table->aliasAs;
}

char * mergeThreeString(char *str1, char *str2, char *str3) {
	char *result = (char *) malloc(
			(strlen(str1) + strlen(str2) + strlen(str3) + 1) * sizeof(char));
	strcpy(result, str1);
	strcat(result, str2);
	strcat(result, str3);
	return result;
}
