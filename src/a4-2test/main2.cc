#include <iostream>
#include "ParseTree.h"

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

int main() {

	char *test = "SELECT SUM DISTINCT (a.b + b), d.g FROM a AS b WHERE ('foo' > this.that OR 2 = 3) AND (12 > 5) GROUP BY a.f, c.d, g.f";
	yy_scan_string(test);
	yyparse();

	if (distinctAtts == 1) {
		cout << "distinctAtts is 1" << endl;
	}

	if (distinctFunc == 1) {
		cout << "distinctfunc is 1" << endl;
	}

	printf("\n");

	while (attsToSelect != NULL) {
		printf("attsToSelect %s\n", attsToSelect->name);
		attsToSelect = attsToSelect->next;
	}

	printf("\n");

	while (groupingAtts != NULL) {
			printf("groupingAtts %s\n", groupingAtts->name);
			groupingAtts = groupingAtts->next;
	}

	printf("\n");

	while (boolean != NULL) {
		OrList * lol = boolean->left;
		while (lol != NULL) {
			// print comparison op
			ComparisonOp * temp = lol->left;
			cout << "code " << temp->code << endl;

			cout << "left operand" << endl;
			cout << "code " << temp->left->code << " value " << string(temp->left->value) << endl;

			cout << "right operand" << endl;
			cout << "code " << temp->right->code << " value " << string(temp->right->value) << endl;
			lol = lol->rightOr;
		}
		boolean = boolean->rightAnd;
		printf("\n");
	}

	while (tables != NULL) {
			printf("tables %s\n", tables->tableName);
			printf("tables alias %s\n", tables->aliasAs);
			tables = tables->next;
	}
}

