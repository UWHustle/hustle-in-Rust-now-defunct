11/26/2018
Parser[Kevin]
+ Aggregated for query 2 are supported checked in.

Optimizer[Yannis]
+ Hustle builds with Quickstep.
+ Work on commonprefix, resolver.

Execution Engine [Somya, Robert]
+ Making datatypes flexible in size (current defaults to fixed sizes and only supports int and ipaddress)
+ Explore multi-threading support.

General
+ CI is in place in travis.

---
11/12/2018
Parser [Matt, Kevin]
+ Send the example plan for the Optimizer.
+ Wrote a cli.
+ Ongoing work expanding parser support.

Optimizer [Yannis]
+ Produce a plan at Quickstep's physical plan level for now, in the future discuss if we should use the execution level abstraction or the physical plan to pass on to the execution engine in the future.
+ Working on physical plan serialization for execution engine.
+ Meeting with Parser and Execution teams on Tuesday to coordinate.

Execution Engine [Robert, Somya]
+ Will work on Tuesday to work on execution plan de-serializer.
+ Working on custom type aggregation support.
+ Working on a quick "project" operator for initial query.

Compiling [Somya]
+ Switched to Makefile instead of CMake.
+ Created example of how compiler structure will look.
+ Waiting on other efforts to implement final version.

---
11/7/2018

Worked on the plan for the first query.

Parser [Matt, Kevin]
+ Write the cli.
+ Support the first query in the parser.
+ Decide who should own the resolver with Yannis.
+ Create a parse tree example.

Optimizer [Yannis]
+ Deserialize the input from the parser in a passthrough resolver class. 
+ Give an example of the output of the optimizer for the first query.

Execution Engine [Robert]
+ Get ready for first query
+ Worked on CMake investigation.

Compiling [Somya]
+ Investigate CMake, and Makefile and compile the modules at once.
---
10/28/2018
Catalog [Matt, Kevin]
+ Quickstep's typesystem is too complicated for a standalone parser? How can we make it simpler? 
+ Meet with Jianqiao and figure out how we can simplify the typesystem.

 
Execution Engine [Robert]
+ Physical Plans can be executed.
+ You can validate the result of a query execution with sqlite.
+ This week: explore how to integrate predicates.

---
10/22/2018

Catalog, Execution Engine [Robert]
+ Pushed v1 of the catalog to github
+ Reorganized github
+ Evaluate window queries from the start, look at TPC-DS look at this paper: http://www.vldb.org/pvldb/vol8/p702-tangwongsan.pdf
+ Find the other paper: ...
+ Cleared up the issue when C++ called Rust
+ Next step: simple scheduler

Parser [Matt]
+ Working with a POC in lemon, problems with huge query space for testing
+ Automatically parse lemon rules and produce all possible sql commands up to a certain depth
+ Python that parses the lemon output into a structure, useful to test
+ Lemon rules can be easily exported to bison
+ RAGS: test generation of queries for given workloads

General 
+ Quantum Resistant Cryptography, what does the space look like? What kind of properties do you get? Is oder maintained?

---
10/15/2018

Parser [Kevin, Matt]
+ Met with Jianqiao.
+ Worked on a Rust interface to call C++ function. 
+ Working on testing to cover SQLite parser.
+ Test SQLite queries in Quickstep's parser and figure out what can be parsed.
+ By thanksgiving: produce ASTs for the quickstep surface, test json serialization and cleanly separate the parser.

Catalog, Storage Manager [Robert]
+ Catalog done by by the end of the week.
+ Simple select query without predicates can run now on the storage manager.
+ By thanksgiving run simple select * query (no aggregations).


Optimizer [Yannis]
+ Working through the Quickstep's code. No new progress to report.

General
+ Target: TPC-DS run on Hustle.
+ Architecture Comments: Keep CC and resolver under a latch to avoid races. In the future we should push CC downstream.

CRISP Report
+ Two key operations for ML applications, matrix multiplication (sparsity), transpose, matrix algebra.
+ We should support matrix operations and we should start from read, write, transpose a matrix.
+ CAPA project: Simple filter query on a key value (key: 8bytes, value: 100bytes) and see what the memory can do.


---
10/08/2018

General [Yannis]
+ Proposed module design, flow and input and ouput of modules.
![](./images/module_design.png)

---
10/01/2018

Parser [Matt, Kevin]
+ Decided to re-implement the parser.
+ Examine if we can use Quickstep's parser.
+ Use SQLite parser's test to verify our parser, start by successfully 
parsing the 7 million test queries.


Catalog, Hustle[Robert]
+ Create a skeleton implementation (coordinate with Matt and Kevin) that creates and deletes a table.
+ The Rust side of the catalog is almost done, run into a garbage collection problem. 
Arguments passed from C++ to Rust are garbage collected.
+ Need different concurrency control for stats and schema.
+ Catalog API will return a token that could be used to return the entire catalog or an id
and the catalog module will store snapshots of the catalog.

Optimizer [Yannis]
+ Quickstep's optimizer is a viable choice and we should use it.


General
+ We need locks on views.
+ Parser should create a AST and only do grammar validation. 
+ CC module will verify the columns and tables and grab the necessary locks.
---
09/24/2018

Storage Manager [Aarati]
+ Looked into the storage manager and about global indices.
+ Will write up and share the storage manager's design.

Parser [Matt, Kevin]
+ Continuing work to isolate the parser by replacing calls to the code generator
  with calls to custom functions.
+ Using SQLite's parser might be not as easy as we hopped. Should we build our own and use SQLite's
SQLite's test suite to ensure with support the same surface?
+ Take a couple of days to decide if we should modify SQLite's parser. Explore the
contents of the parser's classes and evaluate how to add a new function to the parser.
+ Worked on defining the schema with Robert.

[Robert]
+ Did meetings to define the structure of the Catalog and the help with the Parser.
+ Will work on the first implementation and definition of the API of the Catalog.

Optimizer [Yannis]
+ Looked at Quickstep's optimizer as an alternative to starting from scratch.
+ Evaluating Quickstep's optimizer, will work on separating Quickstep's optimizer from the parser
 and catalog.

General
+ We should restrict the languages in order to not require heavy runtimes, we should use Rust, C, C++.
+ Follow Rust's model for error handling, define custom error codes and handle then explicitly.
