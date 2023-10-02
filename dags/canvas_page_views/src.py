import re
import numpy as np
from pandas import DataFrame
from typing import List
from shared.irondata import Irondata
from shared.canvas.base import CanvasResponseError
from shared.canvas.endpoints import CanvasUser, CanvasCourse
from shared.canvas.api import CanvasAPI
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook

TEST_DATA_LIMIT = 48
IS_PROD = Irondata.is_production()

def collect_ids(
    sql: str,
    splits: int = 3,
    ):
    hook = PostgresHook(postgres_conn_id='redshift')
    df = hook.get_pandas_df(sql).dropna()
    print('Base dataset total rows:', df.shape[0])
    chunks = np.array_split(df, splits)
    return chunks

def get_page_views(
    student_ids: DataFrame, 
    execution_date: datetime, 
    token: str,
    window: int=12, 
    **kwargs
    ) -> List[list]:

    archive = {"courses": {}, "assignments": {}}
    start = execution_date - timedelta(hours=window)
    end = execution_date
    page_views = []
    iterator = (
        student_ids.iterrows() if IS_PROD 
        else  student_ids.iloc[:TEST_DATA_LIMIT].iterrows() 
        )
    for idx, row in iterator:

        user = CanvasUser(token=token, id=int(row.canvas_id))

        try:
            student_page_views = user.get_page_views(start_time=start, end_time=end)
        except CanvasResponseError:
            continue


        controller_types = ('assignments', 'wiki_pages', 'quizzes/quizzes')
        assignments = [x for x in student_page_views if x.get('controller') in controller_types]
        for assignment in assignments:
            assignment['student_id'] = row.canvas_id
            assignment['student_uuid'] = row.learn_uuid
        student_page_views, archive = parse_page_views(assignments, token, archive)
        page_views += student_page_views if student_page_views else []

    return [DataFrame(page_views).drop_duplicates(subset=['student_id', 'created_at', 'url'])]

def parse_page_views(page_views, token, archive):

    def get_course_name(assignment, archive):
        course_id = assignment.get('links', {}).get('context', '')
        if str(course_id) in archive.get('courses', {}):
            return archive.get('courses').get(str(course_id))
        # Send a get request to courses/<course_id>
        # if the course_id is not stored in the archive
        try:
            name = CanvasAPI(token=token).get_course(int(course_id)).name
            # Add the course id:name to the archive
            archive['courses'][str(course_id)] = name
            return name
        except:
            return None

    def get_module_item_id(assignment):
        pattern = 'module_item_id=(\d+)'
        r = re.compile(pattern)
        id = r.findall(assignment.get('url', ''))
        if id:
            return id[0]

    def get_assignment_title(assignment, archive={}):
        controller = assignment.get('controller')
        data = get_assignment_data(assignment, archive=archive)
        if data:
            keys = {
                "wiki_pages": "title",
                "assignments": "name",
                "quizzes/quizzes": "title",
                }
            return data.get(keys[controller])

    def get_assignment_data(assignment, archive={}):
        
        if assignment.get('url') in archive.get('assignments', {}):
            return archive['assignments'][assignment.get('url')]

        regex_patterns = {
            "wiki_pages": r'\/pages\/([\w+-]+)?',
            "assignments": r'\/assignments\/([\w+-]+)?',
            "quizzes/quizzes": r'\/quizzes\/(\d+)?',
            }
        controller = assignment.get('controller')
        pattern = regex_patterns[controller]
        r = re.compile(pattern)
        assignment_id = r.findall(assignment.get('url', ''))
        if isinstance(assignment_id, list) and assignment_id:
            assignment_id = assignment_id[0]

        course_id = assignment.get('links', {}).get('context')
        course = CanvasCourse(token=token, id=course_id)
        if assignment_id:       
            try:
                data = course.get_assignment(assignment_id, controller=controller)
            except CanvasResponseError:
                return {}
            if not 'assignments' in archive:
                archive['assignments'] = {}
            archive['assignments'][assignment.get('url')] = data
            return data

    def get_timestamp(assignment):
        timestamp_str = assignment.get('created_at')
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')

    def get_module_item_id(assignment):
        r = re.compile('module_item_id=(\d+)')
        matches = r.findall(assignment.get('url', ''))
        if matches:
            return matches[0]
        else:
            return None

    parsed_page_views = []
    for assignment in page_views:

        data = {
            "student_id": assignment.get('student_id'),
            "student_uuid": assignment.get('student_uuid'),
            "course_name": get_course_name(assignment, archive),
            "course_id": assignment.get('links', {}).get('context'),
            "module_item_id": get_module_item_id(assignment),
            "assignment_title": get_assignment_title(assignment, archive),
            "url": assignment.get('url'),
            "kind": assignment.get('controller'),
            "action": assignment.get('action'),
            "interaction_seconds": assignment.get('interaction_seconds'),
            "created_at": get_timestamp(assignment),
            "participated": assignment.get('participated')
            }
        parsed_page_views.append(data)
    print('Page Views Collected:', len(parsed_page_views))
    return parsed_page_views, archive