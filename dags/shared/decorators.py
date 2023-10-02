from requests import Response

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
