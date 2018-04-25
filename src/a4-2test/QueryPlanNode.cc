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

SelectFileNode::SelectFileNode(TableList* tbl, AndList* andList,
		Statistics &stats) :
		QueryPlanNode(Select_File_Node) {
	this->table = new TableList();
	this->table->aliasAs = tbl->aliasAs;
	this->table->tableName = tbl->tableName;
	char* tableName =
			table->aliasAs == NULL ? table->tableName : table->aliasAs;
	if (table->aliasAs != NULL) {
		stats.CopyRel(table->tableName, table->aliasAs);
	}
	this->tableInfo =
			stats.group_to_table_info_map[stats.relation_to_group_map[string(
					tableName)]];
	applySelectCondition(andList, stats);
	outputPipe = new Pipe(100, ++pipeIdCounter);
	estimate = 0;
}

void SelectFileNode::printNode() {
	printNodeBoundary();
	cout << "SELECT FILE OPERATION ON  " << string(table->tableName) << endl;
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << endl;
	cout << "Select CNF" << endl;
	cnf.Print();
	printNodeBoundary();
}

void SelectFileNode::applySelectCondition(AndList* andList, Statistics &stats) {
	AndList* ands = andList;
	AndList* prev = ands;
	string tableName = string(
			table->aliasAs == NULL ? table->tableName : table->aliasAs);
	AndList* selectAndList = new AndList();
	AndList* currentAndListPtr = selectAndList;
	currentAndListPtr->left = NULL;
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
			prev = ands;
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
			prev->rightAnd = NULL;
			break; // ands->rightAnd == NULL
		}

	}
	currentAndListPtr->rightAnd = NULL;

	char * ch[] = { (char*) tableName.c_str() };
	//calculate and apply the estimates
	stats.Apply(selectAndList, ch, 1);

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
	cnf.GrowFromParseTree(selectAndList, sch, literal);
	outSchema = sch;
}

void SelectPipeNode::printNode() {
	printNodeBoundary();
	cout << "SELECT PIPE OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << endl;
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
	cout << endl;
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

	if (node2->nodeType == Select_File_Node) {
		SelectFileNode* node = (SelectFileNode*) node2;
		relations.push_back(getRelationName(node->table));
	} else if (node2->nodeType == Join_Node) {
		JoinNode* node = (JoinNode*) node2;
		for (char* tbl : node->relNames) {
			relations.push_back(tbl);
		}
	}
	char** arr = relations.data();

	estimate = stats.Estimate(query, arr, relations.size());

	relNames = relations;

	//only execute while applying permanent changes.

	stats.Apply(query, arr, relations.size());
	//TODO Test it
	tableInfo =
			stats.group_to_table_info_map[stats.relation_to_group_map[relNames.at(
					0)]];
	if (doApply) {
		outputPipe = new Pipe(100, ++pipeIdCounter);

		int numberOfAtt1 = node1->outSchema->GetNumAtts();
		int numberOfAtt2 = node2->outSchema->GetNumAtts();
		Attribute attributes[numberOfAtt1 + numberOfAtt2];
		Attribute * attributes1 = node1->outSchema->GetAtts();
		Attribute * attributes2 = node2->outSchema->GetAtts();

		for (int i = 0; i < numberOfAtt1; i++) {
			attributes[i] = attributes1[i];
		}
		for (int i = numberOfAtt1; i < numberOfAtt2 + numberOfAtt1; i++) {
			attributes[i] = attributes2[i - numberOfAtt1];
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

SelectPipeNode::SelectPipeNode(vector<AndList*> multiTableSelects,
		JoinNode* node, Statistics &stats) :
		QueryPlanNode(Select_Pipe_Node) {

	AndList* query = new AndList();
	AndList* handle = query;
	for (AndList* list : multiTableSelects) {
		handle->left = list->left;
		handle->rightAnd = new AndList();
		handle = handle->rightAnd;
	}
	handle->rightAnd = NULL;
	inputPipe = node->outputPipe;
	outSchema = node->outSchema;
	left = node;
	outputPipe = new Pipe(100, ++pipeIdCounter);
	char** arr = node->relNames.data();
	stats.Apply(query, arr, node->relNames.size());

	Record literal;
	cnf.GrowFromParseTree(query, outSchema, literal);

}

void GroupByNode::printNode() {
	printNodeBoundary();
	cout << "GROUPBY OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << endl;
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
	cout << endl;
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
	cout << endl;
	printNodeBoundary();
}

void writeOutNode::printNode() {
	printNodeBoundary();
	cout << "WRITE OUT OPERATION " << endl;
	printPipe(inputPipe, true);
	printSchema(*outSchema);
	cout << endl;
	printNodeBoundary();
}

void printSchema(Schema& schema) {
	cout << "Output Schema" << endl;
	int numberOfAtt = schema.GetNumAtts();

	Attribute *att = schema.GetAtts();
	for (int i = 0; i < numberOfAtt; i++) {
		string temp = "Att" + to_string((i + 1)) + ":";
		cout << temp;

		switch (att[i].myType) {
		case Double:
			cout << "(double)";
			break;
		case Int:
			cout << "(int)";
			break;
		case String:
			cout << "(string)";
			break;
		}

		cout << string(att[i].name) << "\t";
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
