#include <iostream>
#include <string>

#include "parser/ParserDriver.h"
#include "optimizer/optimizer_wrapper.hpp"

#define HUSTLE_VERSION "0.1.0"
#define BUFFER_SIZE 1024
#define PROMPT "hustle> "

using namespace std;

int main(int argc, char **argv) {
    char buffer[BUFFER_SIZE];

    cout << "Hustle version " << HUSTLE_VERSION << endl;

    while (!feof(stdin)) {
        cout << PROMPT;
        if (fgets(buffer, BUFFER_SIZE, stdin)) {
            ParserDriver parser_driver;
            try {
                parser_driver.parse(buffer);
            } catch (const string &msg) {
                cerr << msg << endl;
                cerr << "Falling back on Quickstep parser/resolver..." << endl;
                optimizer(nullptr, buffer);
            }

        }
    }

    cout << endl;
    return 0;
}