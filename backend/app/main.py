from fastapi import FastAPI
import asyncio
from contextlib import asynccontextmanager
from services import worker, update_processor_health

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(update_processor_health())

    for _ in range(2):
        asyncio.create_task(worker())
    yield

app = FastAPI(title="Rinha", lifespan=lifespan)


from routes import router as payment_router


app.include_router(payment_router)