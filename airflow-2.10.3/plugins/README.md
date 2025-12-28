Please see https://airflow.apache.org/docs/apache-airflow/2.10.3/authoring-and-scheduling/plugins.html for information about creating Airflow plugin

Note that, per the above documentation, importing operators, sensors, hooks added in plugins via airflow.{operators,sensors,hooks}.<plugin_name> is no longer supported, and these extensions should just be imported as regular python modules.
