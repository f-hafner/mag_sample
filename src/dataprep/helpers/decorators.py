def decorator_with_args(decorator_to_enhance):
    """ 
    This function is to be used as a decorator.
    It must decorate an other function, that is intended to be used as a decorator.
    It will allow any decorator to accept an arbitrary number of arguments.
    Source: https://stackoverflow.com/questions/739654/how-to-make-function-decorators-and-chain-them-together
    """
    
    def decorator_maker(*args, **kwargs):
       
        # We create on the fly a decorator that accepts only a function
        # but keeps the passed arguments from the maker.
        def decorator_wrapper(func):
       
            # Return the result of the original decorator, which is just a function.
            # The decorator must have this specific signature or it won't work:
            return decorator_to_enhance(func, *args, **kwargs)
        
        return decorator_wrapper
    
    return decorator_maker