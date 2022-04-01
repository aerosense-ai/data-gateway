class MockBigQueryClient:
    def __init__(self, expected_query_result=None):
        self.expected_query_result = expected_query_result or []
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

    def query(self, query):
        """Return the `self.expected_query_result` attribute in a `MockQueryResult` instance.

        :param str query:
        :return MockQueryResult:
        """
        self._query = query
        return MockQueryResult(result=self.expected_query_result)


class MockQueryResult:
    def __init__(self, result):
        self._result = result

    def result(self):
        """Return the `self._result` attribute.

        :return any:
        """
        return self._result
