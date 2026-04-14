#!/usr/bin/env python3

"""Given a list of shared objects from ldd on stdin, write to stdout the
absolute location of the shared library if it has `build/monad-cxx-` in
its path; this is telling us the dependent shared libraries we need to
copy"""

import re
import sys

ldd_pattern = r'.*=> (.*?) \(0x'

def main():
  pattern = re.compile(ldd_pattern)
  for line in sys.stdin.readlines():
    if m := pattern.match(line):
      location = m.groups(0)[0]
      if 'build/monad-' in location:
        print(location)

if __name__ == '__main__':
  main()
