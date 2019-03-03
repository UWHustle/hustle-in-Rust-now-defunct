from ctypes import *

# If this is distributed as a Python package we'll want to find a way to
# include the dynamic lib within that package
lib = cdll.LoadLibrary("../../target/debug/libexecution.dylib")

print(lib.test_interop())