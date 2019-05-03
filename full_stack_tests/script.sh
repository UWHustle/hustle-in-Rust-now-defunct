#!/bin/bash
cd ../build
rm -f diff.txt

OUT="../full_stack_tests/test_output.txt"
CMD="../full_stack_tests/end_to_end_tests.txt"
CMP="../full_stack_tests/compare_output.txt"

rm -f $OUT

while IFS='' read -r line || [[ -n "$line" ]]; do
echo "$line" >> $OUT
./hustle <<<$line >> $OUT 2>&1
done < $CMD

DIFF=$(diff $OUT $CMP)
echo "$DIFF"
echo "$DIFF" >> diff.txt

rm -f $OUT

if [ -z "$DIFF" ]
then
    exit 0
fi

exit 1

