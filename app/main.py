from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.version_routes import router as version_router


# 1️⃣ First create app
app = FastAPI()

# 2️⃣ Then add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 3️⃣ Then include routers
from app.api.routes import router

app.include_router(router)
app.include_router(version_router)