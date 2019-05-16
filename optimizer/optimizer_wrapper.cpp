#include <iostream>
#include <memory>
#include <string>
#include "quickstep/query_optimizer/HustleOptimizer.hpp"

using namespace std;

int optimizer(char *sql) {
    std::unique_ptr<quickstep::OptimizerWrapper> optmizer =
            std::make_unique<quickstep::OptimizerWrapper>();

    std::string sql_string(sql);

    std::string pplan = optmizer->hustle_optimize(sql_string);
    if (pplan.size() == 0) { return -1; } // Quickstep optimizer failed

    char *pplan_char = new char[pplan.size() + 1];
    std::copy(pplan.begin(), pplan.end(), pplan_char);
    pplan_char[pplan.size()] = '\0';
    std::cout << pplan_char << std::endl;
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc != 2) return 1;
    optimizer(argv[1]);
    return 0;
}
