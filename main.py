from fastapi import FastAPI
from routers import routers

app = FastAPI()

# Incluir o roteamento do módulo de routers
app.include_router(routers.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
