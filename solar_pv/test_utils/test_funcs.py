# This file is part of the solar wizard PV suitability model, copyright © Centre for Sustainable Energy, 2020-2023
# Licensed under the Reciprocal Public License v1.5. See LICENSE for licensing details.
import random
import unittest
from typing import List


class ParameterisedTestCase(unittest.TestCase):
    def parameterised_test(self, mapping: List[tuple], fn):
        for tup in mapping:
            inputs = tup[:-1]
            expected = tup[-1]
            try:
                actual = fn(*inputs)
            except Exception as e:
                print(e)
                actual = e
            test_name = str(inputs)[:100] if len(inputs) > 1 else str(inputs[0])[:100]
            with self.subTest(test_name):
                assert expected == actual, f"\nExpected: {expected}\nActual  : {actual}\nInputs : {inputs}"
