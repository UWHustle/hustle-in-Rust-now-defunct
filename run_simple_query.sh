echo "SELECT a FROM t;" > sqlscript.sql
./hustle < sqlscript.sql
rm sqlscript.sql
