import asyncio

async def task1():
    for i in range(10):
        await asyncio.sleep(0)
        print("Task 1",i)
    return 'Task 1 result'

async def task2():
    for i in range(10):
        await asyncio.sleep(0)
        print("Task 2",i)
    return 'Task 2 result'

async def main():
    result1, result2 = await asyncio.gather(task1(), task2())
    print(result1)
    print(result2)


# Python 3.7+
asyncio.run(main())
