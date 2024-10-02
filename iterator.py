import asyncio


class OddCounter:

    def __init__(self, end_range):
        self.end = end_range
        self.start = -1

    def __aiter__(self):
        return self

    async def __anext__(self):
        print("prenext")
        await asyncio.sleep(1)
        print("postnext")
        if self.start < self.end-1:
            self.start += 2
            return self.start
        else:
            raise StopAsyncIteration


async def main():
    async for c in OddCounter(10):
        print(c)

asyncio.run(main())
