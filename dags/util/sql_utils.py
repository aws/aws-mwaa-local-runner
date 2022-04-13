# class SqlTemplates():

#     # template_fields = ["name"]

#     def __init__(self, name: str, **kwargs) -> None:
#         super().__init__(**kwargs)

# @staticmethod
def upsert(target_schema, target_table_name, select_clause, where_clause = None) -> str:
    stage_table_name = f'stage_{target_table_name}'
    target_schema_table = f'{target_schema}.{target_table_name}'

    delete_clause = f'DELETE FROM {target_schema_table}'

    where_clause = f'\nUSING {stage_table_name}\nWHERE {where_clause}' if where_clause else f''

    sql_statment = f'''
CREATE temp TABLE {stage_table_name} AS (
    {select_clause}
);

BEGIN TRANSACTION;

{delete_clause}{where_clause};

INSERT INTO {target_schema_table}
SELECT * FROM {stage_table_name};

END TRANSACTION;
    '''
    print(sql_statment)
    return sql_statment