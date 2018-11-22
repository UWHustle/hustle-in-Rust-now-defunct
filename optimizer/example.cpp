#include <iostream>
#include "./example.hpp"
#include "quickstep/query_optimizer/HustleOptimizer.hpp"
using namespace std;


extern "C" void execute_plan(char*);
/*
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
*/

extern "C" {
int optimizer(char *input) {
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

  std::cout << "Optimizer input: " << input << std::endl;
  const char t[50] = "select t from t;";
  std::string pplan = hustle_optimize();

  char * w = new char[pplan.size() + 1];
  std::copy(pplan.begin(), pplan.end(), w);
  w[pplan.size()] = '\0';

  std::cout << "Optimizer outhfrgr: " << pplan << std::endl;

  execute_plan(w);

  return 0;
}
}