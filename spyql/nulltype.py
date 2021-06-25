# Defines the NULL datatype that mimics SQL's NULL
# Unlike None, which throughs exceptions, operations whith NULLs return NULL
# Allows getting items so that `a.get('b', NULL).get('c', NULL)`` returns NULL
#  when `b` does not exist, and `NULL[x]` returns NULL


#TODO: json dumps with diff default
#TODO: json/csv read None
#  json.loads('{"a":null, "b":1}', object_hook=lambda x: DictNull(x))
#  replace function calls by null_safe calls
#  check performance impact!
#   tests (all bultin funcs!)
#   coalesce/ifnull/ifnotnull(a, 2*a), NULL if col1 is NULL else xxx, 


class NullType:
    def __repr__(self):
        return "NULL"
    def __str__(self):
        return "NULL"
    def __hash__(self):
        return hash("NULL")
    def __bool__(self):
        return False
    def __lt__(self, other):
        return self
    def __le__(self, other):
        return self
    def __eq__(self, other):
        return self
    def __ne__(self, other):
        return self
    def __ge__(self, other):
        return self
    def __gt__(self, other):
        return self
    def __abs__(self):
        return self 
    def __add__(self, other):
        return self 
    def __and__(self, other):
        return self 
    def __floordiv__(self, other):
        return self 
    def __invert__(self):
        return self 
    def __lshift__(self, other):
        return self 
    def __mod__(self, other):
        return self 
    def __mul__(self, other):
        return self 
    def __matmul__(self, other):
        return self 
    def __neg__(self):
        return self 
    def __or__(self, other):
        return self 
    def __pos__(self):
        return self 
    def __pow__(self, other):
        return self 
    def __rshift__(self, other):
        return self 
    def __sub__(self, other):
        return self 
    def __truediv__(self, other):
        return self 
    def __xor__(self, other):
        return self 
    def __contains__(self, other):
        return False 
    def __delitem__(self, other):
        pass
    def __getitem__(self, other):
        return self 
    def __setitem__(self, other, val):
        pass
    def __radd__(self, other):
        return self 
    def __rand__(self, other):
        return self 
    def __rfloordiv__(self, other):
        return self 
    def __rlshift__(self, other):
        return self 
    def __rmod__(self, other):
        return self 
    def __rmul__(self, other):
        return self 
    def __rmatmul__(self, other):
        return self 
    def __ror__(self, other):
        return self 
    def __rpow__(self, other):
        return self 
    def __rrshift__(self, other):
        return self 
    def __rsub__(self, other):
        return self 
    def __rtruediv__(self, other):
        return self 
    def __rxor__(self, other):
        return self 
    def __iadd__(self, other):
        return self 
    def __iand__(self, other):
        return self 
    def __ifloordiv__(self, other):
        return self 
    def __ilshift__(self, other):
        return self 
    def __imod__(self, other):
        return self 
    def __imul__(self, other):
        return self 
    __array_priority__ = 10000
    def __imatmul__(self, other):
        return self 
    def __ior__(self, other):
        return self 
    def __ipow__(self, other):
        return self 
    def __irshift__(self, other):
        return self 
    def __isub__(self, other):
        return self 
    def __itruediv__(self, other):
        return self 
    def __ixor__(self, other):
        return self 
    def __len__(self):
        return 0
    def __iter__(self):
        return [].__iter__()
    def __round__(self, ndigits=0):
        return self
    def __trunc__(self):
        return self
    def __floor__(self):
        return self
    def __ceil__(self):
        return self
    def get(self, *args, **kwargs):
        return self



#singleton
NULL = NullType()

#functions that support NULLs
NULL_SAFE_FUNCS = [
    ('int',     'int_'),
    ('float',   'float_') ,
    ('str',     'str_')]

class NullSafeDict(dict):
    def __missing__(self, key):
        return NULL



def null_safe_call(fun, *args, **kwargs):    
    if NULL in args or NULL in kwargs.values():
        return NULL    
    return fun(*args, **kwargs)

def float_(*args, **kwargs):
    try:
        return null_safe_call(float, *args, **kwargs)
    except ValueError:
        #warning?
        return NULL

def int_(*args, **kwargs):
    try:
        return null_safe_call(int, *args, **kwargs)
    except ValueError:
        #warning?
        return NULL

def str_(*args, **kwargs):
    return null_safe_call(str, *args, **kwargs)
