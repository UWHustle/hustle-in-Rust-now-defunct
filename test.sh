echo "SELECT t FROM t;" > sqlscript.sql
./hustle < sqlscript.sql
rm sqlscript.sql
