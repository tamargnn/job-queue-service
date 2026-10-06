import asyncio


async def main() -> None:
    print("Worker started (placeholder)")
    while True:
        await asyncio.sleep(5)
        print("Worker alive")


if __name__ == "__main__":
    asyncio.run(main())