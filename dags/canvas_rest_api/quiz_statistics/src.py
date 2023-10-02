from io import StringIO
from pandas import read_csv, DataFrame, concat
from shared.canvas.api import CanvasAPI
from airflow.providers.postgres.hooks.postgres import PostgresHook

def query_course_ids(ti, **kwargs):

    query = """
    
    SELECT blueprint_name
         , blueprint_canvas_id
    FROM (
        SELECT *
             , ROW_NUMBER() 
            OVER(PARTITION BY blueprint_canvas_id) AS rnk   
        FROM fis.milestone_canvas_mappings AS mcm 
        WHERE blueprint_name ~* 'prep'
        )
    WHERE rnk = 1
    
    """

    ids = PostgresHook().get_pandas_df(query).to_csv()
    ti.xcom_push(key="ids", value=ids)


def request_quiz_statistics(ti, **kwargs):

    print(kwargs)

    # ==================================================
    # Helper functions
    def quiz_row(entry):
        
        variables = [
            'id',
            'course_id',
            'quiz_id',
            'quiz_title',
            'html_url',
            'multiple_attempts_exist',
            'generated_at',
            'includes_all_versions',
            'points_possible',
            'anonymous_survey',
            'speed_grader_url',
            'quiz_submissions_zip_url',  
        ]
        
        return [entry.get(variable) for variable in variables]

    def question_row(entry):
        
        dataset = []

        for question in entry.get('question_statistics', []):

            variables = [
                'question_type',
                'question_text',
                'position',
                'responses',
                'top_student_count',
                'middle_student_count',
                'bottom_student_count',
                'answered_student_count',
                'correct_student_count',
                'incorrect_student_count',
                'correct_student_ratio',
                'incorrect_student_ratio',
                'correct_top_student_count',
                'correct_middle_student_count',
                'correct_bottom_student_count',
                'variance',
                'stdev',
                'difficulty_index',
                'alpha',  
            ]

            data = [
                question.get('id'),       
                entry.get('id'),         
                entry.get('quiz_id'),     
                entry.get('course_id'),   
                ] + (
                [question.get(var) for var in variables] 
                + [
                    (lambda x: 0.0 if not x.get('point_biserial') else x['point_biserial'])(y) 
                    for y in question.get('point_biserials', [{}]) if y.get('correct')
                    ])
            
            dataset.append(data)
        
        return dataset

    def answer_row(entry):
        user_answers = []
        for question in entry['question_statistics']:
                for answer in question['answers']:
                    user_answer = [[
                        question['id'],
                        entry['quiz_id'],
                        entry['course_id'],
                        user_id,
                        user_name,
                        answer['text'],
                        answer['correct']
                    ] for user_id, user_name in zip(answer['user_ids'], answer['user_names'])]

                    user_answers += user_answer
        return user_answers
    # ==================================================
    # Task Code
    def run_collection(blueprint):
        quiz_statistics = []
        question_statistics = []
        user_answers = []
        for course in blueprint.get_blueprint_associations():
            for quiz in course.get_quizzes():
                id_ = quiz["id"]
                statistics = course.get_quiz_statistics(id_)['quiz_statistics'][0]
                statistics.update(
                    {
                        "quiz_id": id_,
                        "quiz_title": quiz["title"],
                        "course_id": course.id
                    })
                quiz_statistics.append(quiz_row(statistics))
                question = question_row(statistics)
                if question:
                    question_statistics += question
                answers = answer_row(statistics)
                if answers:
                    user_answers += answers
        return quiz_statistics, question_statistics, user_answers

    course_ids = ti.xcom_pull(task_ids=['query_ids'], key='ids')[0]
    print(course_ids)
    course_ids = read_csv(StringIO(course_ids))
    print(course_ids)
    api = CanvasAPI()
    datasets = [DataFrame()] * 3
    for blueprint_id in course_ids.blueprint_canvas_id:
        blueprint = api.get_course(blueprint_id)
        quiz_data = run_collection(blueprint)
        for idx in range(len(datasets)):
            datasets[idx] = concat([datasets[idx], DataFrame(quiz_data[idx])])
    return datasets
