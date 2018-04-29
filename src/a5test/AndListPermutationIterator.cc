#include "AndListPermutationIterator.h"

#include <iostream>
#include <vector>

AndListPermutationIterator::AndListPermutationIterator(
		struct AndList *andList) {
	this->andList = andList;

	countOfNodes = getCountOfNodes();
	numberOfPermutations = getNumberOfPermutations(countOfNodes);

	int *temp = (int *) malloc(sizeof(int) * countOfNodes);
	arrayOfAnds = (struct AndList **) malloc(sizeof(struct AndList *) * countOfNodes);
	for (int i = 0; i < countOfNodes; i++) {
		temp[i] = i;
		arrayOfAnds[i] = andList;
		andList = andList->rightAnd;
	}

	permutations = getPermutations(temp, countOfNodes);
	free(temp);
}

struct AndList * AndListPermutationIterator::getNext() {
	if ((permutationCounter + 1) > numberOfPermutations) {
		return NULL;
	}

	vector<int> sequence = (*permutations)[permutationCounter++];

	struct AndList * prevAndList = NULL;
	struct AndList *andListHead = NULL;
	for (int x : sequence) {
		struct AndList * nextAndList = copyAndListNode(arrayOfAnds[x]);
		if (prevAndList != NULL) {
			prevAndList->rightAnd = nextAndList;
		} else {
			andListHead = nextAndList;
		}
		prevAndList = nextAndList;
	}

	return andListHead;
}

int AndListPermutationIterator::getCountOfNodes() {
	struct AndList * tempAndList = andList;

	int count = 0;
	while (tempAndList != NULL) {
		count++;
		tempAndList = tempAndList->rightAnd;
	}

	return count;
}

long AndListPermutationIterator::getNumberOfPermutations(int n) {
	long temp = 1;

	for (int i = 2; i <= n; i++) {
		temp *= i;
	}

	return temp;
}

vector<vector<int>> * AndListPermutationIterator::getPermutations(int *arr,
		int size) {
	vector<vector<int>> *permutations = new vector<vector<int>>();

	if (size == 0) {
		vector<int> temp;
		permutations->push_back(temp);
		return permutations;
	}

	vector<vector<int>> * previous_permutations;
	if (size == 1) {
		previous_permutations = getPermutations(NULL, 0);
	} else {
		int temp_arr[size - 1];
		memcpy(temp_arr, arr + 1, (size - 1) * sizeof(int));
		previous_permutations = getPermutations(temp_arr, size - 1);
	}

	for (vector<int> temp : *previous_permutations) {
		for (int i = 0; i <= temp.size(); i++) {
			vector<int> new_vector = temp;
			new_vector.insert(new_vector.begin() + i, arr[0]);
			permutations->push_back(new_vector);
		}
	}

	delete previous_permutations;
	return permutations;
}

struct AndList * AndListPermutationIterator::copyAndListNode(struct AndList *andList) {
	struct AndList * newAndList = (struct AndList *) malloc(sizeof(struct AndList));
	newAndList->left = andList->left;
	newAndList->rightAnd = NULL;
	return newAndList;
}

AndListPermutationIterator::~AndListPermutationIterator() {
	free(arrayOfAnds);
	delete permutations;
}

void AndListPermutationIterator::destroyPermutation(struct AndList * andList) {

	while (andList != NULL) {
		struct AndList * nextNode = andList->rightAnd;
		free(andList);
		andList = nextNode;
	}
}
