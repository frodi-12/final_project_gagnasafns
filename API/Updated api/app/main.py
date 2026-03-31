from fastapi import FastAPI
from app.routes.routes import router as OrkuFlaediIsland_routes

# Instantiate FastAPI application
app = FastAPI()

# Attach API routes under /UpdatedOrkuFlaediIsland prefix
app.include_router(OrkuFlaediIsland_routes)

