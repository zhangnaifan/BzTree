#include "test.h"
#include <iostream>
using namespace std;

int main() {
	pmem_test().run(true, true, true, 9600, 24);
	pmem_test().run(true, true, false, 9600, 24);
	pmem_test().run(false, false, true, 9600, 12);
	system("pause");
	return 0;
}