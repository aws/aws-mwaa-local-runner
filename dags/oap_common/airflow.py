def raise_error(context):
    print(f"Task has failed, task_instance_key_str: {context['task_instance_key_str']}")


def construct_storage_filename(context, file_name):
    dag_id = context["dag"].dag_id
    ds = context["ds"]
    run_id = context["run_id"]
    filename = f"{dag_id}/{ds}/{run_id}/{file_name}".replace(":", "_")
    return filename
