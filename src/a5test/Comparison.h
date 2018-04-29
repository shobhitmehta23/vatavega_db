#ifndef COMPARISON_H
#define COMPARISON_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <iostream>
#include <fstream>

// This stores an individual comparison that is part of a CNF
class Comparison {

	friend class ComparisonEngine;
	friend class CNF;
	friend class SortedFile;

	Target operand1;
	int whichAtt1;
	Target operand2;
	int whichAtt2;

	Type attType;

	CompOperator op;

public:

	Comparison();

	//copy constructor
	Comparison(const Comparison &copyMe);

	// print to the screen
	void Print();
};

class Schema;

// This structure encapsulates a sort order for records
class OrderMaker {

	friend class ComparisonEngine;
	friend class CNF;

public:

	int numAtts = 0;

	int whichAtts[MAX_ANDS];
	Type whichTypes[MAX_ANDS];
	// creates an empty OrdermMaker
	OrderMaker();

	// create an OrderMaker that can be used to sort records
	// based upon ALL of their attributes
	OrderMaker(Schema *schema);

	// print to the screen
	void Print();

	void serialize_to_ofstream(std::ofstream& ofs);
	void deserialize_from_isstream(std::ifstream& ifs);
	int * getAttributes();
	int getNumberOfAttributes();
};

class Record;

// This structure stores a CNF expression that is to be evaluated
// during query execution

class CNF {

	friend class ComparisonEngine;
	friend class SortedFile;

	Comparison orList[MAX_ANDS][MAX_ORS];

	int orLens[MAX_ANDS];
	int numAnds;

public:

	// this returns an instance of the OrderMaker class that
	// allows the CNF to be implemented using a sort-based
	// algorithm such as a sort-merge join.  Returns a 0 if and
	// only if it is impossible to determine an acceptable ordering
	// for the given comparison
	int GetSortOrders(OrderMaker &left, OrderMaker &right);

	// print the comparison structure to the screen
	void Print();

	// this takes a parse tree for a CNF and converts it into a 2-D
	// matrix storing the same CNF expression.  This function is applicable
	// specifically to the case where there are two relations involved
	void GrowFromParseTree(struct AndList *parseTree, Schema *leftSchema,
			Schema *rightSchema, Record &literal);

	// version of the same function, except that it is used in the case of
	// a relational selection over a single relation so only one schema is used
	void GrowFromParseTree(struct AndList *parseTree, Schema *mySchema,
			Record &literal);

};

#endif
