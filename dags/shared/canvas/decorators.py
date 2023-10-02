def get(func):
    """
    Decorator function for class methods
    that return an endpoint string and require a single
    get request
    """
    def get_wrapper(*args, **kwargs):
        endpoint = func(*args, **kwargs)
        if isinstance(endpoint, str):
            endpoint = {"endpoint": endpoint}
        data = args[0].get(**endpoint)
        return data.json()
    return get_wrapper

def post(func):
    def wrapper(*args, **kwargs):
        payload = func(*args, **kwargs)
        data = args[0].post(**payload)
        return data
    return wrapper

def paginate(func):
    """
    Decorator function for class methods
    that return an endpoint string and require
    pagination
    """
    def paginate_wrapper(*args, **kwargs):
        endpoint = func(*args, **kwargs)
        if isinstance(endpoint, str):
            endpoint = {"endpoint": endpoint}
        data = args[0].paginate(**endpoint)
        return data
    return paginate_wrapper

def cache(key, replace=False):
    """
    A caching function that adds the output of a class method
    to the class instance as an attribute and append the key
    to instance._keys so the data is included in `.to_dict()`
    """
    def decorator(function):
        def function_foo(*args, **kw):
            obj = args[0]
            result = function(*args, **kw)

            # If the key doesn't exist as an
            # attribute, add it
            if not hasattr(obj, key) or replace:
                setattr(obj, key, result)
            
            # If the key already exists
            # The data should be appended
            elif isinstance(getattr(obj, key), list):  # List
                if isinstance(result, list):
                    setattr(obj, key, getattr(obj, key) + result)
                else:
                    getattr(obj, key).append(result)
            elif isinstance(getattr(obj, key), dict):  # Dict
                getattr(obj, key)[key] = result

            # Append the key to _keys
            # so the data is returned with .to_dict
            args[0]._keys.append(key)
            return result
        return function_foo
    return decorator

def return_factory(type_):
    """
    Generates return type decorators.

    Example: 
    
    If an endpoint returns a list of courses, the api returns
    the courses as dictionaries. It is helpful to return
    those dictionaries as a CanvasCourse object instead.

    To do so, a CanvasCourse decorator is made by:
    ```
    course = return_factory('course')
    ```

    Then the decorator can be imported into a module
    ```
    from ..decorators import course
    ```

    And placed above an instance method
    ```
    @course
    def method(self):
    ```
    """
    def decorator(function):
        def function_wrapper(*args, **kw):
            return_type = type_factory(type_)
            result = function(*args, **kw)
            self = args[0]
            if isinstance(result, dict):
                return return_type(token=self.token, **result)
            elif isinstance(result, list):
                return [return_type(token=self.token, **data) for data in result if result]

        return function_wrapper
    return decorator

def type_factory(type_):
    """
    There are some circular imports happening between the different endpoint modules.
    (CanvasCourse.get_users returns CanvasUser objects
     & CanvasUser.get_courses returns CanvasCourse objects)

    The imports are placed inside this function so the module objects
    can be full constructed, and the imports are only activated
    when needed/after the module objects are constructed.
    
    """
    if type_ == 'user':
        from .endpoints.user import CanvasUser as type_
    elif type_ == 'course':
        from .endpoints.course import CanvasCourse as type_
    elif type_ == 'account':
        from .endpoints.account import CanvasAccount as type_
    elif type_ == 'quiz_submission':
        from .endpoints.quiz_submission import QuizSubmission as type_

    return type_


user = return_factory('user')
course = return_factory('course')
account = return_factory('account')
quiz_submission = return_factory('quiz_submission')
