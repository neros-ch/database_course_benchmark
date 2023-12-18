from time import perf_counter as time
from statistics import median
from config import LAUNCH_NUMBER, DEFAULT_BACKEND, PANDAS_BACKEND, SQLALCHEMY_BACKEND


def measure(queries, cursor=None, backend=DEFAULT_BACKEND):
    '''
    backend options:
    0 - default backend with cursor
    1 - pandas
    2 - sqlalchemy
    '''
    print("Start of measurements")
    for i in range(len(queries)):
        arr = []
        for j in range(LAUNCH_NUMBER):
            start = time()
            if backend == DEFAULT_BACKEND:
                cursor.execute(queries[i])
            elif backend == PANDAS_BACKEND:
                queries[i]()
            elif backend == SQLALCHEMY_BACKEND:
                queries[i].all()
            else:
                raise KeyError
            end = time()
            arr.append(end - start)
        print("Query", i + 1, "time", median(arr))
    print("End of measurements")