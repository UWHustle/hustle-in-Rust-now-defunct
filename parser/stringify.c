//
// Created by Matthew Dutson on 11/2/18.
//

#include "stringify.h"
#include "string_builder.h"

void new_line(string_builder *builder, int indent) {
    append_char(builder, '\n');
    for (int i = 0; i < indent * 3; i++) {
        append_char(builder, ' ');
    }
}

// todo: find ways to make this method clearer and more compact
void traverse(string_builder *builder, parse_node *node, int indent) {
    indent++;

    append_string(builder, "{  ");

    append_string(builder, "\"name\": \"");
    append_string(builder, node->name);
    append_char(builder, '\"');


    if (!is_empty(node->attribute_names)) {
        list_node *attr_name = node->attribute_names->first->next;
        list_node *attr_value = node->attribute_values->first->next;
        do {
            append_char(builder, ',');
            new_line(builder, indent);

            append_char(builder, '\"');
            append_string(builder, attr_name->contents);
            append_string(builder, "\": \"");
            append_string(builder, attr_value->contents);
            append_char(builder, '\"');

            attr_name = attr_name->next;
            attr_value = attr_value->next;
        } while (attr_name->next != NULL);
    }

    if (!is_empty(node->child_names)) {
        list_node *child_name = node->child_names->first->next;
        list_node *child_value = node->child_values->first->next;
        do {
            append_char(builder, ',');
            new_line(builder, indent);
            append_char(builder, '\"');
            append_string(builder, child_name->contents);
            append_string(builder, "\": ");
            new_line(builder, indent + 1);
            traverse(builder, child_value->contents, indent);

            child_name = child_name->next;
            child_value = child_value->next;
        } while (child_name->next != NULL);
    }

    if (!is_empty(node->child_list_names)) {
        list_node *child_list_name = node->child_list_names->first->next;
        list_node *child_list = node->child_lists->first->next;
        do {
            append_char(builder, ',');
            new_line(builder, indent);
            append_char(builder, '\"');
            append_string(builder, child_list_name->contents);
            append_string(builder, "\": [ ");

            list_node *child_list_value = ((linked_list *) child_list->contents)->first->next;
            if (!is_empty(child_list->contents)) {
                do {
                    new_line(builder, indent + 1);
                    traverse(builder, child_list_value->contents, indent);

                    child_list_value = child_list_value->next;
                    if (child_list_value->next != NULL) {
                        append_char(builder, ',');
                    }
                } while (child_list_value->next != NULL);
            }

            append_string(builder, " ]");

            child_list_name = child_list_name->next;
            child_list = child_list->next;
        } while (child_list_name->next != NULL);
    }

    append_string(builder, " }");
}

char *json_stringify(parse_node *node) {
    string_builder *builder = alloc_builder();
    traverse(builder, node, 0);
    char *output = to_string(builder);
    free_builder(builder);
    return output;
}