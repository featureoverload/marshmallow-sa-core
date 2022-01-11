#!/usr/bin/env python

import os
import sys

import pytest


os.environ["SQLALCHEMY_WARN_20"] = "true"

collect_ignore_glob = []

# minimum version for a py3k only test is at
# 3.6 because these are asyncio tests anyway
if sys.version_info[0:2] < (3, 6):
    collect_ignore_glob.append("*_py3k.py")

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")


# use bootstrapping so that test plugins are loaded
# without touching the main library before coverage starts
from sqlalchemy.testing import plugin
bootstrap_file = os.path.join(
    os.path.dirname(os.path.abspath(plugin.__file__)),
    "bootstrap.py",
)

with open(bootstrap_file) as f:
    code = compile(f.read(), "bootstrap.py", "exec")
    to_bootstrap = "pytest"
    exec(code, globals(), locals())
    from sqla_pytestplugin import *  # noqa
