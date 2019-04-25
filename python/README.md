# Using the Python API

Before importing the Hustle Python API place `hustle.py` somewhere visible to
the Python interpreter (one option is to copy it to the working directory).
`hustle.py` uses the `build/execution/libexecution.dylib` shared library. The
`ctypes.util.find_libary` function is used to locate it. See the ctypes docs
for ways to make it visible from a nonstandard location. The easiest option is
again to copy the library to the working directory.