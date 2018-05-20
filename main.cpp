#include "test.h"
#include <iostream>
using namespace std;

int main() {
	pmem_test().run(true, true, true, true, false, 9600, 24);
	pmem_test().run(true, true, false, false, true, 9600, 12);
	//pmem_test().run(false, false, false, true, true, 9600, 6);
	//pmem_test().run(false, false, true, false, true, 9600, 8);
	system("pause");
	return 0;
}