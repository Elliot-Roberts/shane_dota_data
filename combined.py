import json
from functools import wraps


def json_cached(filename):
    """
    Decorator that adapts a function to first read json from a file, passing that as input, and then saving the output
    back to the file.
    :param filename: The name of the file to load from and save to.
    :return: The decorated function
    """
    # python decorators are weird. `json_cached` is actually a decorator generator, lol
    def middle(f):
        @wraps(f)
        def cached_f(*args, **kwargs):
            try:
                with open(filename, "r") as fh:
                    out = f(*args, cached=json.load(fh), **kwargs)
            except FileNotFoundError:
                out = f(*args, **kwargs)
            with open(filename, "w") as fh:
                json.dump(out, fh)
            return out
        
        return cached_f
    
    return middle


@json_cached("wew.json")
def test(cached=None):
    cached = cached or []
    cached.append(int(input("enter a number: ")))
    return cached


if __name__ == '__main__':
    while True:
        print(test())
