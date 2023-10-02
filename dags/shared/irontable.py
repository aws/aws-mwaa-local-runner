from shared.irondata import Irondata

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class Irontable:
    def __init__(self,
                 schema=None,
                 table=None,
                 is_dependency=False,
                 dag_dependencies=[],
                 other_dependencies=[],
                 op_strategy=None,
                 primary_key=None,
                 dist_key=None,
                 sort_key=None,
                 column_names=[]):
        self._schema = schema
        self._table = table
        self._is_dependency = is_dependency
        self._dag_dependencies = dag_dependencies
        self._other_dependencies = other_dependencies
        self._op_strategy = op_strategy
        self._primary_key = primary_key
        self._dist_key = dist_key
        self._sort_key = sort_key
        self._column_names = column_names

    @property
    def op(self):
        if self._op_strategy is None:
            name = self._table
        elif self._op_strategy == "regenerate":
            name = f"regenerate_table__{self._table}"
        elif self._op_strategy == "reset":
            name = f"reset_table__{self._table}"
        else:
            raise BaseException(f"unknown operator strategy {self._op_strategy}")

        return PostgresOperator(
            task_id=name,
            postgres_conn_id="redshift",
            sql=f"{name}.sql",
            params=self.to_dict(),
            autocommit=True)

    @property
    def op_strategy(self):
        return self._op_strategy

    @property
    def schema(self):
        return self._schema

    @property
    def name(self):
        return self._table

    @property
    def table(self):
        return self._table

    @property
    def is_dependency(self):
        return self._is_dependency

    @property
    def dag_dependencies(self):
        return self._dag_dependencies

    @property
    def other_dependencies(self):
        return self._other_dependencies

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def sort_key(self):
        return self._sort_key

    @property
    def dist_key(self):
        return self._dist_key

    @property
    def column_names(self):
        return self._column_names

    @property
    def schema_in_env(self):
        return Irondata.schema(self.schema)

    @property
    def table_in_env(self):
        return Irondata.table(self.schema, self.table)

    @property
    def full_name(self):
        return Irondata.full_table_name(self.schema, self.table)

    @property
    def full_name_as_dependency(self):
        return f"{Irondata.schema(self.schema, is_dependency=True)}." \
            f"{Irondata.table(self.schema, self.table, is_dependency=True)}"

    def to_dict(self, **kwargs):
        return {
            "schema": self.schema_in_env,
            "table": self.table_in_env,
            "prod_schema": self.schema,
            "prod_table": self.table,
            "table_refs": self._build_table_refs(),
            "dag_dependencies": self._build_dag_dependencies(),
            **kwargs
        }

    def _build_table_refs(self):
        table_refs_dict = {}
        for dep in self.dag_dependencies:
            table_refs_dict[dep.table] = dep.full_name_as_dependency

        for other_dep in self.other_dependencies:
            if isinstance(other_dep, dict):
                dep_schema = other_dep["schema"]
                dep_table = other_dep["table"]
                table_refs_dict[dep_table] = f"{Irondata.schema(dep_schema, is_dependency=True)}." \
                    f"{Irondata.table(dep_schema, dep_table, is_dependency=True)}"
            else:
                table_refs_dict[other_dep] = other_dep
        return table_refs_dict

    def _build_dag_dependencies(self):
        return list(map(lambda dep: dep.table, self.dag_dependencies))

    def query_column_names(self):
        hook = PostgresHook()
        query = f"""
        select column_name 
        from information_schema.columns 
        where table_schema = '{self.schema_in_env}' 
        and table_name = '{self.table_in_env}' 
        order by ordinal_position
        """
        records = hook.get_records(query)
        return [x[0] for x in records]
