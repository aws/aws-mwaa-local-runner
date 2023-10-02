from pandas import DataFrame, to_datetime
from typing import List, Dict
from shared.irondata import Irondata
from shared.canvas.endpoints import CanvasCourse
from shared.canvas.base import CanvasResponseError

STRFTIME = '%Y-%m-%d %H:%M:%S'


def get_todo_lists(ids: DataFrame, **kwargs) -> List[Dict[str, List[Dict]]]:
    data = []
    errors = []
    production = Irondata.is_production()
    for idx, row in (ids.iterrows() if production else ids.head(1).iterrows()):
        api = CanvasCourse(id=row.course_id)
        try:
            user_course_todos = api.get_user_course_todos(row.user_id)
            """
            There are a handful of cyber canvas courses
            that do not have any active teacher enrollments.

            At the time of writing this 09/28/22,
            these non teacher enrollment courses have the
            following ids:

            -
            -
            -

            CanvasResponseError captures non 200 response codes
            from the canvas api.
            """
        except CanvasResponseError:
            errors.append(
                (
                    row.course_id,
                    row.user_id,
                    )
                )
            continue
        # Collect submissions for each assignment todo item
        assignment_submissions = []
        for todo in user_course_todos:

            assignment_id = todo.get('assignment', {}).get('id')
            submissions = api.get_assignment_submissions(assignment_id)
            # Isolate assignment submissions that need to be reviewed by an instructor
            ungraded_submissions = [submission
                                    for submission
                                    in submissions
                                    if submission.get('workflow_state')
                                    in ('submitted', 'pending_review')
                                    ]
            assignment_submissions.append(ungraded_submissions)

        todo_list = generate_todo_list(user_course_todos, assignment_submissions)
        data += todo_list

    if errors:
        print('\n\nErrors:\n')
        for error in errors:
            print(f'Course id: {error[0]}', f'User id: {error[1]}')

    return DataFrame(data)


def generate_todo_list(user_course_todos, assignment_submissions):

    assert len(user_course_todos) == len(assignment_submissions), (
                        "user_course_todos and assignment_submissions "
                        "must be the same length."
                        )

    todo_list = []
    for course_todo, submissions in zip(user_course_todos, assignment_submissions):

        todo_data = [
            course_todo.get('context_name'),
            course_todo.get('assignment', {}).get('points_possible'),
            course_todo.get('assignment', {}).get('grading_type'),
            course_todo.get('assignment', {}).get('name'),
        ]
        for submission in submissions:

            submission_data = [
                submission.get('id'),  # submission id
                submission.get('workflow_state'),
                to_datetime(submission.get('submitted_at')).strftime(STRFTIME),
                submission.get('user_id'),
                submission.get('submission_type'),
                submission.get('attempt'),
                submission.get('late'),
                (f"https://learning.flatironschool.com/courses/{course_todo.get('course_id')}"
                 f"/gradebook/speed_grader?assignment_id={course_todo.get('assignment', {}).get('id')}"
                 f"&student_id={submission.get('user_id')}")  # speed grader
                 ]

            todo_item = todo_data + submission_data
            todo_list.append(todo_item)

    return todo_list
