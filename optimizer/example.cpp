#include <iostream>
using namespace std;

extern "C" void execute_plan(char*);

int main()
{
    char* plan_all = "Physical Plan\n\
TopLevelPlan\n\
+-plan=Selection[has_repartition=false]\n\
| +-input=TableReference[relation=T,alias=test]\n\
| | +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\n\
| | +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\n\
| +-project_expressions=\n\
|   +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\n\
|   +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\n\
+-output_attributes=\n\
  +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\n\
  +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\n\
"
  ;

    char* plan_a = "Physical Plan\n\
TopLevelPlan\n\
+-plan=Selection[has_repartition=false]\n\
| +-input=TableReference[relation=T,alias=test]\n\
| | +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\n\
| | +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\n\
| +-project_expressions=\n\
|   +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\n\
+-output_attributes=\n\
  +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\n\
"
  ;

    char* plan_b = "Physical Plan\n\
TopLevelPlan\n\
+-plan=Selection[has_repartition=false]\n\
| +-input=TableReference[relation=T,alias=test]\n\
| | +-AttributeReference[id=0,name=a,relation=test,type=Int NULL]\n\
| | +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\n\
| +-project_expressions=\n\
|   +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\n\
+-output_attributes=\n\
  +-AttributeReference[id=1,name=b,relation=test,type=Int NULL]\n\
"
  ;

    execute_plan(plan_a);

	return 0;
}