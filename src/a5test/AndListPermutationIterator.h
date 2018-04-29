#ifndef A4_2TEST_ANDLISTPERMUTATIONITERATOR_H_
#define A4_2TEST_ANDLISTPERMUTATIONITERATOR_H_

#include "ParseTree.h"

#include <vector>

using namespace std;

class AndListPermutationIterator {
private:
	struct AndList *andList;
	int countOfNodes;
	long numberOfPermutations;
	vector<vector<int>> *permutations;
	struct AndList **arrayOfAnds;
	int permutationCounter = 0;

	vector<vector<int>> * getPermutations(int *arr, int size);
	int getCountOfNodes();
	long getNumberOfPermutations(int n);
	struct AndList * copyAndListNode(struct AndList *);
public:
	AndListPermutationIterator(struct AndList *andList);
	~AndListPermutationIterator();
	struct AndList * getNext();
	void destroyPermutation(struct AndList * andList);
};

#endif /* A4_2TEST_ANDLISTPERMUTATIONITERATOR_H_ */
