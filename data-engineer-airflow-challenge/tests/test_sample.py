from dags import challenge as c
import sys
import pytest
from datetime import datetime
sys.path.append('../')


class TestSample:

    @pytest.fixture(scope='class')
    def airflow_context(self) -> dict:
        """https://airflow.apache.org/code.html#default-variables"""
        return {
            'ds': datetime.now().isoformat().split('T')[0],
            'params': {
                'name': 'World',
            },
        }

    def test_hello_world(self, airflow_context):
        # Act
        hello = c.HelloWorld()(**airflow_context)
        # Assert
        assert hello == 'Hello, World!'

    def test_hello_world(self, airflow_context):
        # Arrange
        now = datetime.now().isoformat().split('T')[0]
        expected = 'Date: ' + now
        # Act
        execution_date = c.PrintExecutionDate.callable(**airflow_context)
        # Assert
        assert execution_date == expected
