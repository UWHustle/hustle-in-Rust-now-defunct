#include <iostream>
using namespace std;

extern "C" void execute_plan(char*);

int main()
{
    cout << "Test" << endl;
    char* plan = "Physical Plan\
TopLevelPlan\
+-plan=Selection[has_repartition=false]\
| +-input=TableReference[relation=T,alias=test]\
| | +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\
| | +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\
| +-project_expressions=\
|   +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\
|   +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\
+-output_attributes=\
  +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\
  +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\
"
  ;

    execute_plan(plan);

	return 0;
}