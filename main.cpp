#include "test.h"
#include <iostream>
using namespace std;

int main() {
	freopen("console.txt", "w", stdout);
	for (int i = 0; i < 0; ++i)
		pmwcas_test().run(6,24);
	bz_test<char> tcase;
	for (int i = 0; i < 10; ++i) {
		//tcase.run(true, true, false, false, false, false, 8, 1);
		tcase.run(true, true, false, true, false, false, 8, 6);
	}
	for (int i = 0; i < 10; ++i) {
		int test_cnt = 6;
		//Ç¿¶È»ìºÏ
		cout << "mix" << endl;
		tcase.run(true, true, true, true, true, true, test_cnt * 2);
		//insert
		cout << "insert" << endl;
		tcase.run(true, true, false, false, true, false, test_cnt);
		//update
		cout << "update" << endl;
		tcase.run(false, false, false, true, true, false, test_cnt);
		//upsert
		cout << "upsert" << endl;
		tcase.run(false, false, false, false, true, true, test_cnt * 2);
		//delete
		cout << "delete" << endl;
		tcase.run(false, false, true, false, true, false, test_cnt);
	}
	system("pause");
	return 0;
}