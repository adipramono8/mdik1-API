import aiosqlite
import math
from fastapi import FastAPI, HTTPException
from typing import Optional
from fastapi.responses import ORJSONResponse
from fastapi import Query, Request
import httpx
import os
import orjson
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

# database configuration
app = FastAPI(default_response_class=ORJSONResponse)
DATABASE_FILE = 'tripdata.db'
DATABASE_URL_FROM_CLOUD = os.getenv("DATABASE_URL")
TABLE_NAME = 'trips'

if not DATABASE_URL_FROM_CLOUD:
    raise ValueError("DATABASE_URL environment variable is not set.")

# rate limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# prevent sql injection - part 1
VALID_SORT_COLUMNS = ["pickup_datetime", "trip_miles", "trip_duration_minutes", "PULocationID", "DOLocationID"]

# 'download database' function
def download_database_if_not_exists():
    if not os.path.exists(DATABASE_FILE):
        print(f"Database file '{DATABASE_FILE}' not found, downloading now...")
        try:
            with httpx.stream("GET", DATABASE_URL_FROM_CLOUD,timeout=60.0) as response:
                response.raise_for_status()
                with open(DATABASE_FILE, "wb") as f:
                    for chunk in response.iter_bytes():
                        f.write(chunk)
            print("Database downloaded successfully.")
        except httpx.HTTPError as e:
            print(f"Failed to download database: {e}")
            raise

# running 'download database' function at startup
@app.on_event("startup")
def startup_event():
    download_database_if_not_exists()

# GET /trips
@app.get("/trips")
@limiter.limit("100/minute")
async def get_trips(
    # validator
    request: Request,
    page: int = Query(1, ge=1, description="Page number to retrieve (must be >= 1)"),
    limit: int = Query(1000, ge=1, le=10000, description="Number of records per page (must be between 1 and 10000)"),
    PULocationID: Optional[int] = Query(None, ge=1, description="Filter by Pickup Location ID (must be positive)"),
    sort_by: Optional[str] = Query(None, description=f"Column to sort by. Valid options: {VALID_SORT_COLUMNS}"),
    order: str = Query("asc", pattern="^(asc|desc)$", description="Sort order: 'asc' or 'desc'")
    ):

    offset = (page - 1) * limit

    # parameted query
    base_query = f"SELECT * FROM {TABLE_NAME}"
    conditions = []
    values = []

    # filter WHERE
    if PULocationID is not None:
        conditions.append("PULocationID = ?")
        values.append(PULocationID)

    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    # order by + prevent sql injection - part 2
    if sort_by is not None:
        if sort_by not in VALID_SORT_COLUMNS:
            raise HTTPException(status_code=400, detail="Invalid sort_by column. Valid options are: {VALID_SORT_COLUMNS}")
        if order.lower() not in ["asc", "desc"]:
            raise HTTPException(status_code=400, detail="Invalid order, must be 'asc' or 'desc'")
        base_query += f" ORDER BY {sort_by} {order.upper()}"
    
    # pagination
    pagination_query="LIMIT ? OFFSET ?"
    async with aiosqlite.connect(DATABASE_FILE) as db:
        db.row_factory = aiosqlite.Row

        # total count dari filter
        count_query = f"SELECT COUNT(*) as total FROM {TABLE_NAME}"
        if conditions:
            count_query += " WHERE " + " AND ".join(conditions)
        
        total_cursor = await db.execute(count_query, values)
        total_records = (await total_cursor.fetchone())[0]
        total_pages = math.ceil(total_records/limit)

        # actual page of data
        final_query = f"{base_query} {pagination_query}"
        final_values = values + [limit, offset]
        data_cursor = await db.execute(final_query, final_values)
        data = await data_cursor.fetchall()

        return{
            "metadata":{
                "total_records": total_records,
                "total_pages": total_pages,
                "current_page": page,
                "limit": limit
            },
            "data": data
        }

'''
def get_trips(limit: int=1000000):
    df_slice = df.head(limit)
    response = StreamingResponse(orjson_streaming_generator(df_slice), media_type="application/json")
    return response'''

'''# read function
@app.get("/")
def read_root():
    return {"message": "みたか？"}'''