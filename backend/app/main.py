from fastapi import FastAPI


app = FastAPI(title="Rinha")


from routes import router as payment_router

app.include_router(payment_router)
