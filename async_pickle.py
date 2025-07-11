from multiprocess import Pool
from textwrap import dedent
from typing import Callable

if __name__ == "__main__":
    double: Callable = lambda: None
    func = dedent(
        """
        def double(num):
            return num * 2
        """   
    )
    exec(func)
    print(double(2))

    pool = Pool(2)
    try:
        print(pool.apply_async(double, (2,)).get())
        print(pool.apply_async(double, (2,)).get())
    except Exception as e:
        pool.close()
        print(e)