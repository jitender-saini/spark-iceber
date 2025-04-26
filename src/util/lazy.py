def lazy(func):
    def wrapper(*args, **kwargs):
        return lambda: func(*args, **kwargs)

    return wrapper
