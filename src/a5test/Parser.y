 
%{

	#include "ParseTree.h" 
	#include <stdio.h>
	#include <string.h>
	#include <stdlib.h>
	#include <iostream>
	#include <vector>
	#include <string>
	
	using namespace std;

	extern "C" int yylex();
	extern "C" int yyparse();
	extern "C" void yyerror(char *s);
  
	// these data structures hold the result of the parsing
	struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
	struct TableList *tables; // the list of tables and aliases in the query
	struct AndList *boolean; // the predicate in the WHERE clause
	struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
	struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query
	
	int dbFileType=0; //0 for heap, 1 for sorted file.
	int isDDLQuery=0; //Set to 1 if  the query is of type CREATE, DROP, INSERT, etc. 
	/*
		isDDLQuery = 
		1 - for CREATE
		2 - for INSERT
		3 - for DROP
		4 - for SET OUTPUT 
	*/
	//int isCreateTableQuery = 0; //set to 1 for create table.
	//int isInsertTableQuery = 0; //set to 1 for insert table.
	//int isDropTableQuery = 0; //set to 1 for drop table.
	//int isSetOutputQuery = 0; //set to 1 for set output mode.
	string DDLQueryTableName;
	
	vector<CreateAttributes> createAttrList;
	
	string filePath;
	int outputMode=0; //0 is default for STDOUT, 1 for output to file, 2 for NONE - i.e. print query plan
	

%}

// this stores all of the types returned by production rules
%union {
 	struct FuncOperand *myOperand;
	struct FuncOperator *myOperator; 
	struct TableList *myTables;
	struct ComparisonOp *myComparison;
	struct Operand *myBoolOperand;
	struct OrList *myOrList;
	struct AndList *myAndList;
	struct NameList *myNames;
	struct CreateAttributes *myAttributes;
	char *actualChars;
	char whichOne;
	int attributeType;
}

%token <actualChars> Name
%token <actualChars> Float
%token <actualChars> Int
%token <actualChars> String
%token <actualChars> MyFile
%token SELECT
%token GROUP 
%token DISTINCT
%token BY
%token FROM
%token WHERE
%token SUM
%token AS
%token AND
%token OR
%token CREATE
%token TABLE
%token <attributeType> TYPE_INTEGER
%token <attributeType> TYPE_DOUBLE
%token <attributeType> TYPE_STRING
%token HEAP
%token SORTED
%token ON
%token INSERT
%token INTO
%token DROP
%token SET
%token OUTPUT
%token STDOUT
%token NONE

%type <myOrList> OrList
%type <myAndList> AndList
%type <myOperand> SimpleExp
%type <myOperator> CompoundExp
%type <whichOne> Op 
%type <myComparison> BoolComp
%type <myComparison> Condition
%type <myTables> Tables
%type <myBoolOperand> Literal
%type <myNames> Atts
%type <myAttributes> Attributes
%type <attributeType> AttributeType

%start SQL


//******************************************************************************
// SECTION 3
//******************************************************************************
/* This is the PRODUCTION RULES section which defines how to "understand" the 
 * input language and what action to take for each "statment"
 */

%%

SQL: SELECT WhatIWant FROM Tables WHERE AndList
{
	isDDLQuery = 0;
	tables = $4;
	boolean = $6;	
	groupingAtts = NULL;
}

| SELECT WhatIWant FROM Tables WHERE AndList GROUP BY Atts
{
	isDDLQuery = 0;
	tables = $4;
	boolean = $6;	
	groupingAtts = $9;
}

| CREATE TABLE Name '(' Attributes ')' AS FileType
{
	isDDLQuery = 1;
	//isCreateTableQuery = 1;
  	DDLQueryTableName = $3;
  	//reverse(createAttrList.begin(), createAttrList.end()); // our grammar parses the attributes in the reverse order.
}

| INSERT String INTO Name //MyFile
{
  	isDDLQuery = 2;
  	//isInsertTableQuery = 1;
 	filePath = $2;
  	DDLQueryTableName = $4;
}

| DROP TABLE Name
{
	isDDLQuery = 3;
  	//isDropTableQuery = 1;
  	DDLQueryTableName = $3;
}

| SET OUTPUT OutPutOption
{
	isDDLQuery = 4;
	//isSetOutputQuery = 1;
  //outputMode = 0;
};

OutPutOption: STDOUT
{
  	outputMode = 0;
  	filePath = "";
}

| NONE
{
  	outputMode = 2;
  	filePath = "";
}

| String  //MyFile
{
  	outputMode = 1;
  	//string fn($1);
  	filePath = $1;
};

Attributes: Name AttributeType ',' Attributes // non terminal, when more than one attributes present
{
  	CreateAttributes attr;
  	attr.name = $1;
  	attr.type = $2;
  	createAttrList.push_back(attr);
}
| Name AttributeType // last attribute
{
  	CreateAttributes attr;
  	attr.name = $1;
  	attr.type = $2;
  	createAttrList.push_back(attr);
};

AttributeType : TYPE_INTEGER
{
  $$ = 0;
}
| TYPE_DOUBLE
{
  $$ = 1;
}
| TYPE_STRING
{
  $$ = 2;
};

FileType : HEAP
{
  	dbFileType = 0;
}

| SORTED ON Atts
{
  	dbFileType = 1;
  	attsToSelect = $3;
};

WhatIWant: Function ',' Atts 
{
	attsToSelect = $3;
	distinctAtts = 0;
}

| Function
{
	attsToSelect = NULL;
}

| Atts 
{
	distinctAtts = 0;
	finalFunction = NULL;
	attsToSelect = $1;
}

| DISTINCT Atts
{
	distinctAtts = 1;
	finalFunction = NULL;
	attsToSelect = $2;
	finalFunction = NULL;
};

Function: SUM '(' CompoundExp ')'
{
	distinctFunc = 0;
	finalFunction = $3;
}

| SUM DISTINCT '(' CompoundExp ')'
{
	distinctFunc = 1;
	finalFunction = $4;
};

Atts: Name
{
	$$ = (struct NameList *) malloc (sizeof (struct NameList));
	$$->name = $1;
	$$->next = NULL;
} 

| Atts ',' Name
{
	$$ = (struct NameList *) malloc (sizeof (struct NameList));
	$$->name = $3;
	$$->next = $1;
}

Tables: Name AS Name 
{
	$$ = (struct TableList *) malloc (sizeof (struct TableList));
	$$->tableName = $1;
	$$->aliasAs = $3;
	$$->next = NULL;
}

| Tables ',' Name AS Name
{
	$$ = (struct TableList *) malloc (sizeof (struct TableList));
	$$->tableName = $3;
	$$->aliasAs = $5;
	$$->next = $1;
}



CompoundExp: SimpleExp Op CompoundExp
{
	$$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));	
	$$->leftOperator = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));
	$$->leftOperator->leftOperator = NULL;
	$$->leftOperator->leftOperand = $1;
	$$->leftOperator->right = NULL;
	$$->leftOperand = NULL;
	$$->right = $3;
	$$->code = $2;	

}

| '(' CompoundExp ')' Op CompoundExp
{
	$$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));	
	$$->leftOperator = $2;
	$$->leftOperand = NULL;
	$$->right = $5;
	$$->code = $4;	

}

| '(' CompoundExp ')'
{
	$$ = $2;

}

| SimpleExp
{
	$$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));	
	$$->leftOperator = NULL;
	$$->leftOperand = $1;
	$$->right = NULL;	

}

| '-' CompoundExp
{
	$$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));	
	$$->leftOperator = $2;
	$$->leftOperand = NULL;
	$$->right = NULL;	
	$$->code = '-';

}
;

Op: '-'
{
	$$ = '-';
}

| '+'
{
	$$ = '+';
}

| '*'
{
	$$ = '*';
}

| '/'
{
	$$ = '/';
}
;

AndList: '(' OrList ')' AND AndList
{
        // here we need to pre-pend the OrList to the AndList
        // first we allocate space for this node
        $$ = (struct AndList *) malloc (sizeof (struct AndList));

        // hang the OrList off of the left
        $$->left = $2;

        // hang the AndList off of the right
        $$->rightAnd = $5;

}

| '(' OrList ')'
{
        // just return the OrList!
        $$ = (struct AndList *) malloc (sizeof (struct AndList));
        $$->left = $2;
        $$->rightAnd = NULL;
}
;

OrList: Condition OR OrList
{
        // here we have to hang the condition off the left of the OrList
        $$ = (struct OrList *) malloc (sizeof (struct OrList));
        $$->left = $1;
        $$->rightOr = $3;
}

| Condition
{
        // nothing to hang off of the right
        $$ = (struct OrList *) malloc (sizeof (struct OrList));
        $$->left = $1;
        $$->rightOr = NULL;
}
;

Condition: Literal BoolComp Literal
{
        // in this case we have a simple literal/variable comparison
        $$ = $2;
        $$->left = $1;
        $$->right = $3;
}
;

BoolComp: '<'
{
        // construct and send up the comparison
        $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
        $$->code = LESS_THAN;
}

| '>'
{
        // construct and send up the comparison
        $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
        $$->code = GREATER_THAN;
}

| '='
{
        // construct and send up the comparison
        $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
        $$->code = EQUALS;
}
;

Literal : String
{
        // construct and send up the operand containing the string
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = STRING;
        $$->value = $1;
}

| Float
{
        // construct and send up the operand containing the FP number
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = DOUBLE;
        $$->value = $1;
}

| Int
{
        // construct and send up the operand containing the integer
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = INT;
        $$->value = $1;
}

| Name
{
        // construct and send up the operand containing the name
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = NAME;
        $$->value = $1;
}
;


SimpleExp: 

Float
{
        // construct and send up the operand containing the FP number
        $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
        $$->code = DOUBLE;
        $$->value = $1;
} 

| Int
{
        // construct and send up the operand containing the integer
        $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
        $$->code = INT;
        $$->value = $1;
} 

| Name
{
        // construct and send up the operand containing the name
        $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
        $$->code = NAME;
        $$->value = $1;
}
;

%%

