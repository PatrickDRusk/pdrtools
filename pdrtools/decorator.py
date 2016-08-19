def logger(spaz=None):
    def wrap(f):
        def wrapped_f(*args, **kwargs):
            print "Arguments were: %s, %s" % (args, kwargs)
            if spaz:
                print "And spaz was %s" % spaz
            return f(*args, **kwargs)
        return wrapped_f
    return wrap

@logger(spaz="Ethan")
def fooble(x, y):
    print x + y

@logger()
def famble(x, y):
    print x - y

fooble(1, 2)    # returns 3, and prints the spaz line
famble(1, 2)    # returns -1, with no spaz line
