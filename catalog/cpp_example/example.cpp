#include <iostream>

extern "C" char* lock_db(char*);
extern "C" char* release_db(char*);
extern "C" char* get_db_name(char*);
extern "C" char* add_relation(char*, char*);
extern "C" char** get_relations(char*);
extern "C" int get_relation_count(char*);

extern "C" int get_relation_column_count(char*, char*);
extern "C" char** get_relation_columns(char*, char*);
extern "C" char* add_column(char*, char*, char*);

int main()
{
    char* db_name = "testdb";
    char* relation_name = "myrelation!";
    char* column_name = "mycolumn!";

    char* db_token = lock_db(db_name);

    char* db_name_returned = get_db_name(db_token);

    printf("Returned db name:\n");
    printf(db_name_returned);
    printf("\n\n");


    printf("Adding a relation!\n");
    db_token = add_relation(db_token, relation_name);

    int relation_count = get_relation_count(db_token);
    char** relations = get_relations(db_token);

    printf("Now returned ");
    printf("%i", relation_count);
    printf(" relation names:\n");
    for(int i =0; i<relation_count; i++){
        printf(relations[i]);
        printf("\n");
    }
    printf("\n");


    printf("Adding a column!\n");
    db_token = add_column(db_token, relation_name, column_name);

    int column_count = get_relation_column_count(db_token, relation_name);
    char** columns = get_relation_columns(db_token, relation_name);

    printf("Now returned ");
    printf("%i", column_count);
    printf(" relation names:\n");
    for(int i =0; i<column_count; i++){
        printf(columns[i]);
        printf("\n");
    }
    printf("\n");

    release_db(db_token);

	return 0;
}