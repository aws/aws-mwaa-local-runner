# Canvas Rest API

This module contains code for interacting with the Canvas Rest API ([Docs](https://canvas.instructure.com/doc/api/))

## Basic Usage

### Get a specific course via the `courses/` endpoint
```python
from dags.shared.canvas.api import CanvasAPI

api = CanvasAPI()
course_id = 4674
course = api.get_course(course_id)
```

### Get a specific user via the `users/` endpoint
```python
from dags.shared.canvas.api import CanvasAPI

api = CanvasAPI()
user_id = 9179
user = api.get_user(user_id)
```

### Get the root and non-root accounts via the `accounts/` endpoint
```python
api = CanvasAPI()
# Root account (id=1 is the default)
root_account = api.get_account()
# Non root
account = api.get_account(id=2)
```

### Get the students for a specific course
There are two recommended ways for doing this. 

**1)** If you want access the course data by first sending a get request to the `courses/` endpoint, in addition to the the list of students:
```python
from dags.shared.canvas.api import CanvasAPI

api = CanvasAPI()
course_id = 4674
course = api.get_course(course_id)
students = course.get_students()
```

**2)** If you do not wish to make a request to the `courses/` endpoint, and just want to the list of students for a course
```python
from dags.shared.canvas.endpoints import CanvasCourse

course_id = 4674
course = CanvasCourse(id=course_id)
students = course.get_students()
```

## Filtering api calls
To add additional filters to an api call, you can pass the filter as a keyword argument.

### Example
The `CanvasAccount` object has a method `.get_courses()` that returns all courses attached to an account.

Below, I add additional keyword arguments (all options are listed in the [Docs](https://canvas.instructure.com/doc/api/accounts.html#method.accounts.courses_api)) to only collect published courses with at least one student enrollment.
```python
account.get_courses(published=True, enrollment_type='student')
```

## Running custom requests
If this tool does not have a method for the request you wish to run, you can use the `base.Canvas` object, which contains methods for running get, post, and paginated requests.
> The `base.Canvas` object is the parent class for every single object in this project, so you can access these methods from any object you've imported. ie, `CanvasAPI.get`

### get
> get the [number of unread conversions](https://canvas.instructure.com/doc/api/conversations.html#method.conversations.unread_count) in canvas for the current user
```python
api = Canvas()
endpoint = '/api/v1/conversations/unread_count'
response = api.get(endpoint)
unread_convos = response.json()
```

## paginate
> get [all conversations](https://canvas.instructure.com/doc/api/conversations.html#method.conversations.index) for the current user
```python
api = Canvas()
endpoint = '/api/v1/conversations'
convos = api.paginate(endpoint)
```


## Development

### Code organization
Because there are so many endpoint variations in the canvas rest api, the code for this module uses an enforced organization.

#### CanvasAPI
The general idea for this class is that it generates an `endpoints` class object by running a request to the object's base endpoint and returning the response json in the form of the endpoint class. For example, `api.get_course(<course_id>)` sends a get request to the `course/` endpoint, then returns `CanvasCourse(**response.json())`
- **Where is the json data added to the class object**?
    - This happens inside `dags.shared.canvas.base` within the `__init__`  method.

While complex api calls should typically be stored within `dags.shared.canvas.endpoints`, if there is an api operation that does not have a clear base endpoint, (ie, uses several different endpoints in a unique way), the code for this operation is probably best stored inside the `CanvasAPI` object.

#### `dags.shared.canvas.endpoints`
While there are many endpoint variations, the canvas rest api has only a handful of base endpoints. These include:
1. `courses/`
1. `users/`
1. `accounts/`

##### Endpoint objects 
For each base endpoint, there is a corresponding class object. (`CanvasCourse`, `CanvasUser`), where the base endpoint is set by the class attribute `<class>.endpoint`. 

Any additions to the endpoint's url request is set by the individual class method. 

**For example**, `CanvasCourse.get_quizzes()` returns `f'/{self.id}/quizzes'` which is then appended to the end of the base endpoint and sent as a get request.






