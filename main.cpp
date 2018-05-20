#include "test.h"
#include <iostream>
using namespace std;

int main() {
	//freopen("log.txt", "w", stdout);
	pmem_test().run(true, true, true, true, false, true, 96000, 24);
	pmem_test().run(true, true, false, false, true, false, 96000, 12);
	pmem_test().run(false, false, false, true, true, true, 96000, 12);
	//pmem_test().run(false, false, false, true, true, 9600, 6);
	//pmem_test().run(false, false, true, false, true, 9600, 8);
	system("pause");
	return 0;
}