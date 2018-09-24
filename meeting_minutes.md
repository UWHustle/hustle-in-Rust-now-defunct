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
