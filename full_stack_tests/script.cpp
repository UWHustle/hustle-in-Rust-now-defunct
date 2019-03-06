#include<fstream>
#include<string>
#include<iostream>

using namespace std;

int main(){
    ifstream file("../full_stack_tests/end_to_end_tests.txt");
    string line;
    while (std::getline(file, line))
    {
        cout<<line<<endl;
    }
    return 0;
}
