#!/bin/bash
rm -f test_output.txt
cd ../build
rm -f test-data/test_table.hsl
OUT="../full_stack_tests/test_output.txt"
CMD="../full_stack_tests/end_to_end_tests.txt"
while IFS='' read -r line || [[ -n "$line" ]]; do
	echo "$line" >> test_output.txt
	./hustle <<<$line >> $OUT 2>&1
done < $CMD
