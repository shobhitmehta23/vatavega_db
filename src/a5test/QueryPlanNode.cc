#include "QueryPlanNode.h"
#include "RelOp.h"
#include <cstring>

void printSchema(Schema& schema);
void printNodeBoundary();
void printPipe(Pipe *pipe, bool ifInputPipe);
char* getRelationName(TableList* table);
char * mergeThreeString(char *str1, char *str2, char *str3);
int print_pipe_to_stdio(Pipe &in_pipe, Schema *schema, bool print);

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

void QueryPlanNode::executeQueryTree() {
	executeQueryTreeHelper(this);
}
void QueryPlanNode::printNode() {

}

void QueryPlanNode::executeNode() {

}

void QueryPlanNode::printQueryTreeHelper(QueryPlanNode *queryPlanNode) {
	if (queryPlanNode == NULL) {
		return;
	}

	printQueryTreeHelper(queryPlanNode->left);
	queryPlanNode->printNode();
	printQueryTreeHelper(queryPlanNode->right);
}

//Similar to printQueryTreeHelper(), this executes query instead. calls execute on each node in post order fashion.
void QueryPlanNode::executeQueryTreeHelper(QueryPlanNode *queryPlanNode) {
	if (queryPlanNode == NULL) {
		return;
	}

	executeQueryTreeHelper(queryPlanNode->left);
	executeQueryTreeHelper(queryPlanNode->right);
	cout << "Node Type: " << queryPlanNode->nodeType << endl;
	queryPlanNode->executeNode();
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
	//applySelectCondition(andList, stats);
	/***************************************************************************************************
	 *
	 */

	AndList* ands = andList;
	AndList* prev = ands;
	//string tableName = string(
	//		table->aliasAs == NULL ? table->tableName : table->aliasAs);
	AndList* selectAndList = new AndList();
	AndList* currentAndListPtr = selectAndList;
	AndList* newAndList = new AndList();
	AndList* newAndListCopy = newAndList;
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
							string(tableName) + "." + string(nameOperand->value));
					if ((it == tableInfo->attributes.end())) {
						goToNextAnd = true;
						break;
					}
				}

			}
			ors = ors->rightOr;
		}
		if (goToNextAnd) {
			if(newAndListCopy->left == NULL){
				newAndListCopy->left = ands->left;
			}else{
				newAndListCopy->rightAnd = new AndList;
				newAndListCopy = newAndListCopy->rightAnd;
				newAndListCopy->left = ands->left;
			}
			//ands = ands->rightAnd;
			//continue;
		}
		else{
		//just to handle the first time OR
			if (currentAndListPtr->left == NULL) {
				currentAndListPtr->left = ands->left;
			} else {
				currentAndListPtr->rightAnd = new AndList;
				currentAndListPtr = currentAndListPtr->rightAnd;
				currentAndListPtr->left = ands->left;
				//currentAndListPtr->rightAnd = ands;
				//currentAndListPtr = currentAndListPtr->rightAnd;
			}
		}
		//remove the AND clause from the Andlist.
		/*if (ands->rightAnd != NULL) {
			ands->left = ands->rightAnd->left;
			ands->rightAnd = ands->rightAnd->rightAnd;
		} else {
			ands->left = NULL;
			ands->rightAnd = NULL;
			//prev->rightAnd = NULL;
			break; // ands->rightAnd == NULL
		}*/

		ands = ands->rightAnd;

	}
	currentAndListPtr->rightAnd = NULL;
	newAndListCopy->rightAnd = NULL;
	andList->left = newAndList->left;
	andList->rightAnd = newAndList->rightAnd;

	char * ch[] = { tableName };
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
	literal = new Record;
	cnf = new CNF;
	cnf->GrowFromParseTree(selectAndList, sch, *literal);

	outSchema = sch;


	//*************************************************************************************************
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
	cnf->Print();
	printNodeBoundary();
}

void SelectFileNode::executeNode() {
	/*Execute the relational operation.*/
	SelectFile* selectFileOp = new SelectFile;
	DBFile* tableFile = new DBFile;
	tableFile->Open(string(strcat(table->tableName, ".bin")).c_str());
	selectFileOp->Run(*tableFile, *outputPipe, *cnf, *literal);
	//tableFile.Close();
	//selectFileOp.WaitUntilDone();
	/*Rel Op execution done.*/
}

void SelectFileNode::applySelectCondition(AndList* andList, Statistics &stats) {
	AndList* ands = andList;
	AndList* prev = ands;
	string tableName = string(
			table->aliasAs == NULL ? table->tableName : table->aliasAs);
	AndList* selectAndList = new AndList();
	AndList* currentAndListPtr = selectAndList;
	AndList* newAndList = new AndList();
	AndList* newAndListCopy = newAndList;
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
			if(newAndListCopy->left == NULL){
				newAndListCopy->left = ands->left;
			}else{
				newAndListCopy->rightAnd = new AndList;
				newAndListCopy = currentAndListPtr->rightAnd;
				newAndListCopy->left = ands->left;
			}
			//ands = ands->rightAnd;
			//continue;
		}
		else{
		//just to handle the first time OR
			if (currentAndListPtr->left == NULL) {
				currentAndListPtr->left = ands->left;
			} else {
				currentAndListPtr->rightAnd = new AndList;
				currentAndListPtr = currentAndListPtr->rightAnd;
				currentAndListPtr->left = ands->left;
				//currentAndListPtr->rightAnd = ands;
				//currentAndListPtr = currentAndListPtr->rightAnd;
			}
		}
		//remove the AND clause from the Andlist.
		/*if (ands->rightAnd != NULL) {
			ands->left = ands->rightAnd->left;
			ands->rightAnd = ands->rightAnd->rightAnd;
		} else {
			ands->left = NULL;
			ands->rightAnd = NULL;
			//prev->rightAnd = NULL;
			break; // ands->rightAnd == NULL
		}*/
		
		ands = ands->rightAnd;

	}
	currentAndListPtr->rightAnd = NULL;
	newAndListCopy->rightAnd = NULL;
	andList->left = newAndList->left;
	andList->rightAnd = newAndList->rightAnd;

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
	literal = new Record;
	cnf = new CNF;
	cnf->GrowFromParseTree(selectAndList, sch, *literal);

	outSchema = sch;
}

void JoinNode::printNode() {
	printNodeBoundary();
	cout << "JOIN OPERATION " << endl;
	printPipe(inputPipe1, true);
	printPipe(inputPipe2, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << endl;
	cout << "Join CNF" << endl;
	cnf->Print();
	printNodeBoundary();
}

void JoinNode::executeNode() {
	/*Execute the relational operation.*/
	Join *joinOp = new Join;
	joinOp->Run(*inputPipe1, *inputPipe2, *outputPipe, *cnf, *literal);
	//joinOp.WaitUntilDone();
	/*Rel Op execution done.*/
}

JoinNode::JoinNode(QueryPlanNode* node1, QueryPlanNode* node2, bool doApply,
		Statistics &stats, AndList* query) :
		QueryPlanNode(Join_Node) {
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
		inputPipe1 = node1->outputPipe;
		inputPipe2 = node2->outputPipe;
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
		literal = new Record;
		cnf = new CNF;
		cnf->GrowFromParseTree(query, node1->outSchema, node2->outSchema,
				*literal);
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
	literal = new Record;
	cnf = new CNF;
	cnf->GrowFromParseTree(query, outSchema, *literal);
}

void SelectPipeNode::printNode() {
	printNodeBoundary();
	cout << "SELECT PIPE OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << endl;
	cout << "Select CNF" << endl;
	cnf->Print();
	printNodeBoundary();
}

void SelectPipeNode::executeNode() {
	/*Execute the relational operation.*/
	SelectPipe *selectPipeOp = new SelectPipe;
	selectPipeOp->Run(*inputPipe, *outputPipe, *cnf, *literal);
	//joinOp.WaitUntilDone();
	/*Rel Op execution done.*/
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

void GroupByNode::executeNode() {
	/*Execute the relational operation.*/
	GroupBy *groupByOp = new GroupBy;
	groupByOp->Run(*inputPipe, *outputPipe, *orderMaker, *function);
	//groupByOp.WaitUntilDone();
	/*Rel Op execution done.*/
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

void SumNode::executeNode() {
	/*Execute the relational operation.*/
	Sum *sumOp = new Sum;
	sumOp->Run(*inputPipe, *outputPipe, *function);
	//sumOp.WaitUntilDone();
	/*Rel Op execution done.*/
}

void ProjectNode::printNode() {
	printNodeBoundary();
	cout << "PROJECT OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << endl;
	cout << "attributes to keep" << endl;
	for (int i = 0; i < numOfAttsOutput; i++) {
		cout << keepme[i] << '\t';
	}
	cout << endl;
	printNodeBoundary();
}

void ProjectNode::executeNode() {
	/*Execute the relational operation.*/
	Project *projectOp = new Project;
	projectOp->Run(*inputPipe, *outputPipe, keepme, numOfAttsInput,
			numOfAttsOutput);
	//projectOp.WaitUntilDone();
	/*Rel Op execution done.*/
}

void DistinctNode::printNode() {
	printNodeBoundary();
	cout << "DUPLICATE REMOVAL OPERATION " << endl;
	printPipe(inputPipe, true);
	printPipe(outputPipe, false);
	printSchema(*outSchema);
	cout << endl;
	printNodeBoundary();
}

void DistinctNode::executeNode() {
	/*Execute the relational operation.*/
	DuplicateRemoval* dupRemovalOp = new DuplicateRemoval;
	dupRemovalOp->Run(*inputPipe, *outputPipe, *outSchema); //projectOp.WaitUntilDone();
	/*Rel Op execution done.*/
}

void WriteOutNode::printNode() {
	printNodeBoundary();
	cout << "WRITE OUT OPERATION " << endl;
	printPipe(inputPipe, true);
	printSchema(*outSchema);
	cout << endl;
	printNodeBoundary();
}

void WriteOutNode::executeNode() {
	cout << "Inside write out **************************************" << endl;
	/*Execute the relational operation.*/
	WriteOut *writeOp = new WriteOut;
	writeOp->Run(*inputPipe, filePointer, *outSchema);
	writeOp->WaitUntilDone();

	/*Rel Op execution done.*/
}

int print_pipe_to_stdio(Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove(&rec)) {
		if (print) {
			rec.Print(schema);
		}
		cnt++;
	}
	cout << "Number of records returned: " << cnt << endl;
	return cnt;
}

void printSchema(Schema& schema) {
	cout << "Output Schema" << endl;
	int numberOfAtt = schema.GetNumAtts();

	Attribute *att = schema.GetAtts();
	for (int i = 0; i < numberOfAtt; i++) {
		string temp = "Att" + to_string(i) + ":";
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
	static int count = 0;
	static int nodeNo = 1;

	if (++count % 2) {
		cout << "Node number " << nodeNo++ << endl;
	}

	for (int i = 0; i < 7; i++) {
		cout << "*************************";
	}
	cout << "******";
	if (count % 2) {
		cout << endl;
	} else {
		cout << endl << endl;
	}

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
