import asyncio, sys
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def test():
    import psycopg
    dsn = "postgresql://profiler:SpaldinG%2624252425@localhost:5432/profiler"
    print(f"Connecting to: {dsn[:40]}...")
    try:
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            cur = await conn.execute("SELECT 1")
            print(f"PostgreSQL OK: {await cur.fetchone()}")
    except Exception as e:
        print(f"PostgreSQL ERROR: {e}")

asyncio.run(test())
