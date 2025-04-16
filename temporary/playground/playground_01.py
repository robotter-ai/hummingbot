import asyncio
import time

from scripts.utility.utils import CacheManager, cached


class ExampleService:
    def __init__(self):
        self.counter = 0

    @cached(ttl=5)  # Cache for 5 seconds
    def get_data(self, param1: str, param2: int):
        self.counter += 1
        return {"param1": param1, "param2": param2, "counter": self.counter, "timestamp": time.time()}

    @cached(ttl=5)
    async def get_data_async(self, param1: str, param2: int):
        self.counter += 1
        await asyncio.sleep(1)  # Simulate async operation
        return {"param1": param1, "param2": param2, "counter": self.counter, "timestamp": time.time()}


async def main():
    service = ExampleService()

    print("=== Synchronous Example ===")
    # First call - will execute the method
    result1 = service.get_data("test", 123)
    print(f"First call result: {result1}")

    # Second call within 5 seconds - will return cached result
    result2 = service.get_data("test", 123)
    print(f"Second call result (cached): {result2}")

    # Force refresh - will execute the method again
    result3 = service.get_data("test", 123, force_refresh=True)
    print(f"Third call result (forced refresh): {result3}")

    print("\n=== Asynchronous Example ===")
    # Async call
    result4 = await service.get_data_async("test", 123)
    print(f"First async call result: {result4}")

    # Second async call within 5 seconds - will return cached result
    result5 = await service.get_data_async("test", 123)
    print(f"Second async call result (cached): {result5}")

    # Force refresh async call
    result6 = await service.get_data_async("test", 123, force_refresh=True)
    print(f"Third async call result (forced refresh): {result6}")

    print("\n=== Cache Clear Example ===")
    # Clear the cache
    CacheManager.clear_cache()

    # Call after cache clear
    result7 = service.get_data("test", 123)
    print(f"Call after cache clear: {result7}")


if __name__ == "__main__":
    asyncio.run(main())
