# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@Time    : 2023-12-06 16:14:11
@Author  : Rey
@Contact : reyxbo@163.com
@Explain : DLL file methods.

Modules
-------
rdll_inject_core : DLL file inject method code.
rdll_inject : DLL file inject method.
"""


import ctypes


# Windows.
if hasattr(ctypes, "windll"):

    from .rdll_inject import *

    __all__ = (
        "inject_dll",
    )

# Non Windwos.
else:
    __all__ = ()