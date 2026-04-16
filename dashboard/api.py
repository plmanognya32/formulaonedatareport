import duckdb
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="F1 Data API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = "/workspaces/formulaonedatareport/f1_transforms/f1.duckdb"

def query(sql: str):
    con = duckdb.connect(DB_PATH, read_only=True)
    result = con.execute(sql).df()
    con.close()
    return result.to_dict(orient="records")

@app.get("/fastest-laps")
def fastest_laps(session_key: int = None):
    where = f"where session_key = {session_key}" if session_key else ""
    return query(f"""
        select * from main_gold.gold_fastest_laps
        {where}
        order by fastest_lap_seconds
    """)

@app.get("/race-summary")
def race_summary(session_key: int = None):
    where = f"where session_key = {session_key}" if session_key else ""
    return query(f"""
        select * from main_gold.gold_driver_race_summary
        {where}
        order by avg_lap_seconds
    """)

@app.get("/sessions")
def sessions():
    return query("""
        select distinct session_key
        from main_gold.gold_driver_race_summary
        order by session_key
    """)
