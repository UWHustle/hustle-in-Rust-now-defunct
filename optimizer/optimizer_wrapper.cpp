#include <iostream>
#include "./optimizer_wrapper.hpp"
#include "quickstep/query_optimizer/HustleOptimizer.hpp"
using namespace std;


extern "C" void execute_plan(char*);

int optimizer(const shared_ptr<ParseNode> &syntax_tree, const string &sql) {
  std::string pplan = hustle_optimize(syntax_tree, sql);
  char *pplan_char = new char[pplan.size() + 1];
  std::copy(pplan.begin(), pplan.end(), pplan_char);
  pplan_char[pplan.size()] = '\0';
//  std::cout << pplan_char << std::endl;
  execute_plan(pplan_char);
//  std::cout << pplan_char << std::endl;
  return 0;
}
