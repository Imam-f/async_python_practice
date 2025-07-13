import multiprocessor

if  __name__ == "__main__":
    # multiprocessor.start_remote_server()
    pool = multiprocessor.Pool(8)
    result1 = pool.apply_async(lambda x: print(x, sum([5 for i in range(50000000)])), 5)
    print("<====================>")
    result2 = pool.apply_async(lambda x: print(x), 5)
    result2 = pool.apply_async(lambda x: print(x), 5)
    result2 = pool.apply_async(lambda x: print(x), 5)
    result2 = pool.apply_async(lambda x: print(x), 5)
    result2 = pool.apply_async(lambda x: print(x), 5)
    result2 = pool.apply_async(lambda x: print(x), 5)
    result2 = pool.apply_async(lambda x: print(x), 5)
    pool.close()

    with multiprocessor.Pool(2) as pool:
        result1 = pool.apply_async(lambda x: print(x, sum([5 for i in range(50000000)])), 5)
        print("<====================>")
        result2 = pool.apply_async(lambda x: print(x), 5)
