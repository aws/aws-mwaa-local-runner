from airflow.models.dag import DagBag
import unittest

class TestHelloWorldDAG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=False, store_serialized_dags=False)

    def test_dag_loaded(self):
        dag = self.dagbag.get_dag(dag_id='testing_hello_world_plugin')
        self.assertDictEqual(self.dagbag.import_errors, {})
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 2)
