import unittest

from dbt.adapters.spark_livy.livysession import LivyCursor
import datetime as dt


class TestLivySession(unittest.TestCase):
    def test_coerce_some_column_types_from_response_rows(self):
        schema = [
            {'name': 'created_at', 'type': 'timestamp', 'nullable': True, 'metadata': {}},
            {'name': 'literal_str', 'type': 'string', 'nullable': False, 'metadata': {}},
            {'name': 'number', 'type': 'decimal', 'nullable': False, 'metadata': {}}
        ]
        res_rows = [['2023-02-06T17:03:30Z', 'literal string', '12'], ['2023-02-16T09:20:24Z', 'literal a', '20']]
        rows = LivyCursor._coerce_some_primitive_types(schema, res_rows)
        expected_rows = [[dt.datetime(2023, 2, 6, 17, 3, 30), 'literal string', '12'],
                         [dt.datetime(2023, 2, 16, 9, 20, 24), 'literal a', '20']]
        self.assertEqual(rows, expected_rows)
