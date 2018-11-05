#include <stdio.h>

#include "parse_node.h"
#include "stringify.h"

int main(int argc, char **argv) {

    // Simulate the query "SELECT * FROM test"
    parse_node *set_operation_statement = alloc_node("SetOperationStatement");

    parse_node *set_operation = alloc_node("SetOperation");
    add_child(set_operation_statement, "set_operation_query", set_operation);
    add_attribute(set_operation, "set_operation_type", "Select");

    dynamic_array *children = alloc_array();
    add_child_list(set_operation, "children", children);
    parse_node *select = alloc_node("Select");
    add_last(children, select);

    parse_node *select_clause = alloc_node("SelectStar");
    add_child(select, "select_clause", select_clause);

    parse_node *from_clause = alloc_node("");
    add_child(select, "from_clause", from_clause);

    parse_node *table_ref = alloc_node("TableReference");
    add_child(from_clause, "", table_ref);
    add_attribute(table_ref, "table", "test");

    char* json_string = json_stringify(set_operation_statement);
    printf("%s\n", json_string);
    free(json_string);

    char *quickstep_string = quickstep_stringify(set_operation_statement);
    printf("%s", quickstep_string);
    free(quickstep_string);

    free_node(set_operation_statement);
    free_node(set_operation);
    free_array(children);
    free_node(select);
    free_node(select_clause);
    free_node(from_clause);
    free_node(table_ref);
}