import asyncio
import traceback

async def long_running():
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        traceback.print_exc()
        raise

async def main():
    await asyncio.wait_for(long_running(), timeout=5)

asyncio.run(main())