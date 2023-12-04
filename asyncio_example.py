import asyncio
import time

init_time = time.time()

async def task1():
    print(f"[{time.time() - init_time}] Task 1 started")
    handle = printing()
    await asyncio.sleep(1)
    print(f"[{time.time() - init_time}] Task 1 continues")
    handle = await handle
    print(f"[{time.time() - init_time}] Task 1 completed")

async def task2():
    print(f"[{time.time() - init_time}] Task 2 started")
    await asyncio.sleep(1)
    print(f"[{time.time() - init_time}] Task 2 completed")

async def printing():
    print("Before tasks")

async def main():
    await asyncio.gather(task1(), task2())

asyncio.run(main())