#!/bin/bash
set -x
rm -f ../full_stack_tests/test_output.txt
rm -f diff.txt
rm -f ./test-data/test_table.hsl
rm -f ./test-data/test_table_project.hsl

OUT="../full_stack_tests/test_output.txt"
CMD="../full_stack_tests/end_to_end_tests.txt"
CMP="../full_stack_tests/compare_output.txt"

while IFS='' read -r line || [[ -n "$line" ]]; do
echo "$line" >> $OUT
./hustle <<<$line >> $OUT 2>&1
done < $CMD

DIFF=$(diff $OUT $CMP)
echo "$DIFF"
echo "$DIFF" >> diff.txt

if [ -z "$DIFF" ]
then
    exit 0
fi

exit 1

