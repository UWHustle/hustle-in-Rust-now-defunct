# Hustle    
[![Build Status](https://travis-ci.com/UWHustle/hustle.svg?branch=master)](https://travis-ci.com/UWHustle/hustle)

Hustle is a scalable data platform using a relational kernel and a microservices-based architecture.

Installation instructions:
==========================
To build Hustle, first run the script init.sh.

```bash
./init.sh
```

This generates a build folder and a Makefile for the project and each of the modules and installs cmake and cargo if they are not installed. Finally, build the project:

```bash
cd build
make -j<number of cores of your machine>
```

and then run the executable.
```bash
./hustle
```

To exit the shell, type the end of file character.

Run unit tests with CTest.
```bash
ctest --output-on-failure
```

Coding Guidelines
=================
- For Rust, we follow [these guidelines](https://github.com/rust-lang-nursery/fmt-rfcs).
- For Python, we follow [these guidelines](https://www.python.org/dev/peps/pep-0008). Please use a linter like [autopep8](https://pypi.python.org/pypi/autopep8) to validate compliance.

Naming
-------
TBD

Code Layout
-----------
* Use 4 spaces per indentation level.
* Limit all lines to a maximum of 79 characters.
* For long blocks of text (docstrings or comments), limit the line to 72 characters.
* Surround top-level function and class definitions with two blank lines. Method definitions inside a class are surrounded by a single blank line.

Testing
----------
* TBD


Working with GitHub project automation (without really having to think about it)
-------
GitHub has three key mechanisms to manage projects **Issues**, **Pull requests**, and **Projects**. We use these three mechanisms in the following way.

1. Always work on a known Issue.
- Make sure there is an Issue for what you are working on. If there isn't an Issue describing your work, then create an Issue before starting work.
- Keep **all** discussions, including design-related discussions, in the thread for that Issue. This way everyone can look/learn from the discussion. *We want all discussion to be open to everyone*.
- Be nice and polite even when you have a disagreement. Take time to read what you have written, and proof-read it before posting your discussion. This is especially important if the discussion has heated up :-)

2. Calling out your work.
- Before starting to work on an issue, call out by simply noting "I'm on it" on the discussion page for that Issue. Assign the Issue to yourself (in the "Assignees" box on the right), if it is not already assigned to you.
- Pick a "Project" tag in the box by the same name in the right panel of the Issue's page. **This action starts tracking the project in the Automation**, so it is crucial you do this. Now, the Projects page will show that the Issue has been picked up.

3. Raising a PR
- When you are done with your work, raise a PR.
- In the PR note `Fixes #123` (if you are fixing issue #123). There are [other keywords](https://help.github.com/articles/closing-issues-using-keywords/) that you can use to connect your work to a PR, **but it is important you make a note of the Issue you are addressing in your PR**. If you have a linking keyword  (like `Fixes`) in your PR, then closing the PR automatically closes the Issue. Closed Issues are automatically moved to the "Done" part in the Projects page!
- Check if your PR did close the Issue by visiting the corresponding Issue page. There should be a red `Closed` icon at the top if the Issue is closed. If you don't see that, then your PR did not close the Issue (e.g. you may not have used the right keyword to link the fixing of the PR to the corresponding Issue). If that happens, on the Issue page, you should note the PR that closes the issue, and manually close the Issue. Project tracking will now show that the issue is "Done".
