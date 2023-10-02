from sys import path
from pathlib import Path
from dotenv import load_dotenv

root = Path(__file__).resolve().parents[3]
dags = root / 'dags'
shared = dags / 'shared'
env = root / 'docker' / 'config' / '.env.localrunner'

load_dotenv(env.as_posix())

path.append(root.as_posix())
path.append(dags.as_posix())
path.append(shared.as_posix())

from dags.shared.canvas.api import CanvasAPI
from dags.canvas_page_views.src import *

api = CanvasAPI()
user = api.get_user(19111)
end = datetime.now()
start = end - timedelta(hours=96)
page_views = user.get_page_views(start_time=start, end_time=end)
controller_types = ('assignments', 'wiki_pages', 'quizzes/quizzes')
assignments = [x for x in page_views if x.get('controller') in controller_types]
archive = {"courses": {}, "assignments": {}}
