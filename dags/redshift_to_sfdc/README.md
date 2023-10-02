# redshift_to_sfdc

## Overview

The goal of this DAG is to use data in Redshift to update an object in Salesforce. This process is set to run every hour.

It does this in three steps:
1. Run a query in Redshift that generates an _m_ x 2 dataset (i.e. _m_ rows, 2 columns). The dataset should be unique on column _m_, which represents the ID of an object in Salesforce. The other column should be the value to update said Salesforce object.
2. Unload dataset created in step 1 to S3.
3. Read S3 object and bulk update the Salesforce object using the `simple-salesforce` Python module.

## Updating this DAG

To update the redshift_to_sfdc DAG, first, confirm the object and field with a Salesforce admin (e.g. Daris Paulino or Kevin Ortiz). They will need to update the field-level access for the data+engineering@flatironschool.com account.

Then, a data analyst will need to write a query to find records in Salesforce with data that needs to be updated. This query should be incremental, meaning, only records that need to be updated should be returned. 

Finally, the jobs dictionary needs to be updated in `./dag.py`. The key corresponds to the name of the query in `./sql/` (ignoring the .sql extension) and the value corresponds to the Salesforce object and field name, in the style of `object_name.field_name`.