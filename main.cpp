#include "test.h"
#include <iostream>
using namespace std;

int main() {
	freopen("console.txt", "w", stdout);
	
	bz_test<char> tcase;

	for (int i = 0; i < 10; ++i) {
		int test_cnt = 6;
		//Ç¿¶È»ìºÏ
		cout << "mix" << endl;
		tcase.run(true, true, true, true, true, true, false, test_cnt * 2);
		//insert
		cout << "insert" << endl;
		tcase.run(true, true, false, false, true, false, false, test_cnt);
		//update
		cout << "update" << endl;
		tcase.run(false, false, false, true, true, false, false, test_cnt);
		//upsert
		cout << "upsert" << endl;
		tcase.run(false, false, false, false, true, true, false, test_cnt * 2);
		//delete
		cout << "delete" << endl;
		tcase.run(false, false, true, false, true, false, false, test_cnt);
		//consolidate
		cout << "consolidate" << endl;
		tcase.run(false, false, false, false, false, false, true, 1, 1);

	}
	system("pause");
	return 0;
}