class MockBigQueryClient:
    def __init__(self, expected_query_results=None):
        self.expected_query_results = expected_query_results or [[]]
        self._next_query_index = 0
        self.rows = []

    def get_table(self, name):
        """Do nothing.

        :param str name:
        :return None:
        """
        pass

    def insert_rows(self, table, rows):
        """Store the given rows in the `self.rows` attribute.

        :param str table:
        :param list(dict) rows:
        :return None:
        """
        self.rows.append(rows)

    def query(self, query, *args, **kwargs):
        """Return the `self.expected_query_result` attribute in a `MockQueryResult` instance.

        :param str query:
        :return MockQueryResult:
        """
        self._query = query
        try:
            result = MockQueryResult(result=self.expected_query_results[self._next_query_index])
        except IndexError:
            raise ValueError(
                "More mock queries have been run than mock query results given - make sure you've given enough."
            )
        self._next_query_index += 1
        return result


class MockQueryResult:
    def __init__(self, result):
        self._result = result

    def result(self):
        """Return the `self._result` attribute.

        :return any:
        """
        return self._result
