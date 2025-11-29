from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
import pandas as pd
import os
import json

app = FastAPI(title="AI Analyst Gateway - Superstore Analytics")

# PostgreSQL Connection
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://postgres:password123@db:5432/ai_analyst_demo")
engine = create_engine(POSTGRES_URL)

@app.get("/")
async def root():
    return {"status": "AI Analyst Gateway Active!", "database": "Superstore Analytics"}

@app.get("/dashboard")
async def dashboard():
    query = """
    SELECT 
        COUNT(*)::text as total_records,
        COUNT(DISTINCT "Order ID")::text as orders,
        COUNT(DISTINCT "Customer ID")::text as customers,
        CONCAT('$', ROUND(SUM("Sales")::numeric, 0)) as total_sales
    FROM kaggle_data
    """
    df = pd.read_sql(query, engine)
    return df.to_dict('records')[0]

@app.get("/top-customers")
async def top_customers(limit: int = Query(10)):
    query = text("""
    SELECT 
        "Customer Name",
        COUNT(DISTINCT "Order ID") as orders,
        ROUND(SUM("Sales")::numeric, 0) as spent
    FROM kaggle_data 
    GROUP BY "Customer Name" 
    ORDER BY spent DESC 
    LIMIT :limit
    """)
    df = pd.read_sql(query, engine, params={"limit": limit})
    return df.to_dict('records')

@app.get("/top-regions")
async def top_regions():
    query = text("""
    SELECT "Region", 
           COUNT(DISTINCT "Order ID") as orders,
           ROUND(SUM("Sales")::numeric, 0) as sales
    FROM kaggle_data 
    GROUP BY "Region" 
    ORDER BY sales DESC
    """)
    df = pd.read_sql(query, engine)
    return df.to_dict('records')

@app.get("/sql")
async def execute_sql(query: str = Query(...)):
    try:
        df = pd.read_sql(query, engine)
        return {"success": True, "data": df.to_dict('records')}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/sean-miller")
async def sean_miller():
    query = text("""
    SELECT "Order ID", "Order Date", "Product Name", "Sales"
    FROM kaggle_data 
    WHERE "Customer Name" = 'Sean Miller'
    ORDER BY "Sales" DESC
    """)
    df = pd.read_sql(query, engine)
    return df.to_dict('records')
