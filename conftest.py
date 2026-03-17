"""
Root conftest.py — ensures the meshconsole *package* (from src/) is imported
instead of the standalone meshconsole.py script in this directory.
"""

import sys
import os

# The standalone meshconsole.py in this directory shadows the installed package.
# Remove the current directory from sys.path if it would cause shadowing,
# and ensure the src/ directory is at the front.
_this_dir = os.path.dirname(os.path.abspath(__file__))
_src_dir = os.path.join(_this_dir, "src")

# Remove CWD / project root if present (to avoid meshconsole.py shadowing)
sys.path = [p for p in sys.path if os.path.abspath(p) != _this_dir]

# Ensure src/ is first
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)
