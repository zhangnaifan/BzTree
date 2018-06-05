#include "test.h"
#include <iostream>
using namespace std;

int main() {
	freopen("console.txt", "w", stdout);

	unit_test<uint64_t> tcase;

	for (int i = 0; i < 1; ++i) {
		int test_cnt = 6;
		//upsert
		cout << "tree" << endl;
		tcase.run(true, false, false, false, false, false, false, false, false, true, 9500, 4, 1);
	}

	for (int i = 0; i < 0; ++i) {
		int test_cnt = 6;
		//Ç¿¶È»ìºÏ
		cout << "mix" << endl;
		tcase.run(true, true, true, true, true, true, false, false, false, false, 0, test_cnt * 2);
		//insert
		cout << "insert" << endl;
		tcase.run(true, true, false, false, true, false, false, false, false, false, 0, test_cnt);
		//update
		cout << "update" << endl;
		tcase.run(false, false, false, true, true, false, false, false, false, false, 0, test_cnt);
		//upsert
		cout << "upsert" << endl;
		tcase.run(false, false, false, false, true, true, false, false, false, false, 0, test_cnt * 2);
		//delete
		cout << "delete" << endl;
		tcase.run(false, false, true, false, true, false, false, false, false, false, 0, test_cnt);
		//consolidate
		cout << "consolidate" << endl;
		tcase.run(false, false, false, false, false, false, true, false, false, false, 0, 1, 1);
		//upsert
		cout << "upsert" << endl;
		tcase.run(false, false, false, false, true, true, false, false, false, false, 0, test_cnt * 2);
		//split
		cout << "split and merge" << endl;
		tcase.run(false, false, false, false, false, false, false, true, true, false, 0, 1, 1);
	}
	system("pause");
	return 0;
}