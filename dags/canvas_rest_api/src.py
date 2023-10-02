from shared.irondata import Irondata
from shared.canvas.base import Canvas
from shared.canvas.api import CanvasAPI
from shared import s3

Canvas.root = "https://my.learn.co"

def canvas_extract(ds, bucket, **kwargs):
    accounts_table = kwargs["enterprise_accounts"]
    courses_table = kwargs["enterprise_courses"]
    enrollments_table = kwargs["enterprise_enrollments"]

    api = CanvasAPI(token=Irondata.get_config("CANVAS_ACCESS_TOKEN_ENTERPRISE"))

    accounts = api.paginate("/api/v1/accounts/1/sub_accounts?recursive=true")
    account_list = [_process_accounts(account) for account in accounts]

    courses = api.paginate("/api/v1/accounts/1/courses?per_page=50&with_enrollments=true")
    course_list = [_process_course(course) for course in courses]

    enrollments = [api.paginate(f"/api/v1/courses/{ course.get('id') }/enrollments?include=email&per_page=50") for course in courses]
    enrollment_list = []
    for course in enrollments:
        for enrollment in course:
            enrollment_list.append(_process_enrollment(enrollment))

    if account_list and course_list and enrollment_list:
        s3.upload_as_csv(
            bucket,
            f"{accounts_table.schema_in_env}/{ds}/{accounts_table.table_in_env}",
            account_list)
        
        s3.upload_as_csv(
            bucket,
            f"{courses_table.schema_in_env}/{ds}/{courses_table.table_in_env}",
            course_list)
        
        s3.upload_as_csv(
            bucket,
            f"{enrollments_table.schema_in_env}/{ds}/{enrollments_table.table_in_env}",
            enrollment_list)
        
        print("Accounts, courses, and enrollments written to S3")
        return True
    else:
        print("Found no accounts, courses, or enrollments via Canvas API")
        return False

def _process_accounts(account):
    return [
        account.get("id")
        , account.get("name")
        , account.get("workflow_state")
        , account.get("parent_account_id")
        , account.get("root_account_id")
    ]

def _process_course(course):
    return [
        course.get("id")
        , course.get("account_id")
        , course.get("name")
        , course.get("created_at")
        , course.get("workflow_state")
    ]

def _process_enrollment(enrollment):
    return [
        enrollment.get("id")
        , enrollment.get("course_id")
        , enrollment.get("course_section_id")
        , enrollment.get("user_id")
        , enrollment.get("type")
        , enrollment.get("created_at")
        , enrollment.get("updated_at")
        , enrollment.get("last_activity_at")
        , enrollment.get("enrollment_state")
        , enrollment.get("user").get("name")
        , enrollment.get("user").get("email")
        , enrollment.get("user").get("short_name")
        , enrollment.get("user").get("sis_user_id")
        , enrollment.get("user").get("login_id")
    ]