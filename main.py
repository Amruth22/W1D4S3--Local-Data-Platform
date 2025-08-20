from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
import sqlite3
import threading
import time
from collections import OrderedDict
import asyncio
import uvicorn
from contextlib import contextmanager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Weather Station Data Logger",
    description="Local Data Platform for Weather Monitoring with LRU Cache and Connection Pooling",
    version="1.0.0"
)

# Pydantic models
class TemperatureReading(BaseModel):
    temperature: float
    sensor_id: str
    timestamp: Optional[datetime] = None

class TemperatureResponse(BaseModel):
    timestamp: datetime
    temperature: float
    sensor_id: str

class AverageTemperatureResponse(BaseModel):
    average_temperature: float
    period_start: datetime
    period_end: datetime
    readings_count: int
    data_source: str  # "cache" or "database"

# LRU Cache Implementation
class LRUCache:
    """Thread-safe LRU Cache for temperature readings"""
    
    def __init__(self, capacity: int = 100):
        self.capacity = capacity
        self.cache = OrderedDict()
        self.lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get item from cache and move to end (most recently used)"""
        with self.lock:
            if key in self.cache:
                # Move to end (most recently used)
                value = self.cache.pop(key)
                self.cache[key] = value
                return value
            return None
    
    def put(self, key: str, value: Dict[str, Any]) -> None:
        """Add item to cache, remove oldest if at capacity"""
        with self.lock:
            if key in self.cache:
                # Update existing item and move to end
                self.cache.pop(key)
            elif len(self.cache) >= self.capacity:
                # Remove oldest item (first item)
                self.cache.popitem(last=False)
            
            self.cache[key] = value
    
    def get_recent_readings(self, limit: int = None) -> List[Dict[str, Any]]:
        """Get recent readings from cache (most recent first)"""
        with self.lock:
            readings = list(self.cache.values())
            readings.reverse()  # Most recent first
            return readings[:limit] if limit else readings
    
    def size(self) -> int:
        """Get current cache size"""
        with self.lock:
            return len(self.cache)
    
    def clear(self) -> None:
        """Clear all cache entries"""
        with self.lock:
            self.cache.clear()

# Connection Pool Implementation
class SQLiteConnectionPool:
    """Thread-safe SQLite connection pool"""
    
    def __init__(self, database_path: str, min_connections: int = 2, max_connections: int = 5):
        self.database_path = database_path
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.pool = []
        self.pool_lock = threading.RLock()
        self.active_connections = 0
        
        # Initialize minimum connections
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool with minimum connections"""
        for _ in range(self.min_connections):
            conn = self._create_connection()
            if conn:
                self.pool.append(conn)
                self.active_connections += 1
    
    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create a new SQLite connection"""
        try:
            conn = sqlite3.connect(
                self.database_path,
                check_same_thread=False,
                timeout=30.0
            )
            conn.row_factory = sqlite3.Row  # Enable column access by name
            return conn
        except sqlite3.Error as e:
            logger.error(f"Error creating database connection: {e}")
            return None
    
    @contextmanager
    def get_connection(self):
        """Get a connection from the pool (context manager)"""
        conn = None
        try:
            with self.pool_lock:
                if self.pool:
                    # Get connection from pool
                    conn = self.pool.pop()
                elif self.active_connections < self.max_connections:
                    # Create new connection if under max limit
                    conn = self._create_connection()
                    if conn:
                        self.active_connections += 1
                else:
                    # Wait for a connection to become available
                    pass
            
            if not conn:
                # If no connection available, create temporary one
                conn = self._create_connection()
                if not conn:
                    raise Exception("Unable to create database connection")
            
            yield conn
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                try:
                    # Return connection to pool
                    with self.pool_lock:
                        if len(self.pool) < self.max_connections:
                            self.pool.append(conn)
                        else:
                            conn.close()
                            self.active_connections -= 1
                except sqlite3.Error as e:
                    logger.error(f"Error returning connection to pool: {e}")
    
    def close_all(self):
        """Close all connections in the pool"""
        with self.pool_lock:
            for conn in self.pool:
                try:
                    conn.close()
                except sqlite3.Error:
                    pass
            self.pool.clear()
            self.active_connections = 0

# Global instances
lru_cache = LRUCache(capacity=100)
db_pool = SQLiteConnectionPool("weather_data.db", min_connections=2, max_connections=5)

# Database initialization
def init_database():
    """Initialize the SQLite database with weather readings table"""
    with db_pool.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS temperature_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                temperature REAL NOT NULL,
                sensor_id TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create index for faster time-based queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp 
            ON temperature_readings(timestamp)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sensor_timestamp 
            ON temperature_readings(sensor_id, timestamp)
        """)
        
        conn.commit()
        logger.info("Database initialized successfully")

# Helper functions
def store_temperature_reading(reading: TemperatureReading) -> bool:
    """Store temperature reading in database and cache"""
    try:
        # Set timestamp if not provided
        if not reading.timestamp:
            reading.timestamp = datetime.now()
        
        # Store in database
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO temperature_readings (timestamp, temperature, sensor_id)
                VALUES (?, ?, ?)
            """, (reading.timestamp, reading.temperature, reading.sensor_id))
            conn.commit()
            reading_id = cursor.lastrowid
        
        # Store in cache
        cache_key = f"{reading.sensor_id}_{reading.timestamp.isoformat()}"
        cache_value = {
            "id": reading_id,
            "timestamp": reading.timestamp,
            "temperature": reading.temperature,
            "sensor_id": reading.sensor_id
        }
        lru_cache.put(cache_key, cache_value)
        
        logger.info(f"Stored reading: {reading.sensor_id} - {reading.temperature}°C at {reading.timestamp}")
        return True
        
    except Exception as e:
        logger.error(f"Error storing temperature reading: {e}")
        return False

def get_average_temperature_last_hour() -> Optional[AverageTemperatureResponse]:
    """Get average temperature for the last hour, checking cache first"""
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1)
    
    # First, try to get data from cache
    cache_readings = lru_cache.get_recent_readings()
    cache_data = []
    
    for reading in cache_readings:
        reading_time = reading["timestamp"]
        if isinstance(reading_time, str):
            reading_time = datetime.fromisoformat(reading_time)
        
        if start_time <= reading_time <= end_time:
            cache_data.append(reading)
    
    # If we have enough recent data in cache (let's say at least 30 readings)
    if len(cache_data) >= 30:
        temperatures = [r["temperature"] for r in cache_data]
        avg_temp = sum(temperatures) / len(temperatures)
        
        return AverageTemperatureResponse(
            average_temperature=round(avg_temp, 2),
            period_start=start_time,
            period_end=end_time,
            readings_count=len(cache_data),
            data_source="cache"
        )
    
    # Fallback to database
    try:
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT AVG(temperature) as avg_temp, COUNT(*) as count
                FROM temperature_readings
                WHERE timestamp BETWEEN ? AND ?
            """, (start_time, end_time))
            
            result = cursor.fetchone()
            
            if result and result["avg_temp"] is not None:
                return AverageTemperatureResponse(
                    average_temperature=round(result["avg_temp"], 2),
                    period_start=start_time,
                    period_end=end_time,
                    readings_count=result["count"],
                    data_source="database"
                )
            else:
                return None
                
    except Exception as e:
        logger.error(f"Error getting average temperature: {e}")
        return None

def get_recent_readings(limit: int = 10) -> List[TemperatureResponse]:
    """Get recent temperature readings"""
    try:
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT timestamp, temperature, sensor_id
                FROM temperature_readings
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,))
            
            readings = []
            for row in cursor.fetchall():
                readings.append(TemperatureResponse(
                    timestamp=datetime.fromisoformat(row["timestamp"]),
                    temperature=row["temperature"],
                    sensor_id=row["sensor_id"]
                ))
            
            return readings
            
    except Exception as e:
        logger.error(f"Error getting recent readings: {e}")
        return []

# API Endpoints

@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    init_database()
    logger.info("Weather Station Data Logger started")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown"""
    db_pool.close_all()
    lru_cache.clear()
    logger.info("Weather Station Data Logger stopped")

@app.get("/")
async def root():
    """Root endpoint with system information"""
    return {
        "message": "Weather Station Data Logger API",
        "version": "1.0.0",
        "cache_size": lru_cache.size(),
        "endpoints": {
            "POST /readings": "Submit temperature reading",
            "GET /readings/recent": "Get recent readings",
            "GET /analytics/average-hour": "Get average temperature for last hour",
            "GET /status": "System status"
        }
    }

@app.post("/readings", response_model=dict)
async def submit_reading(reading: TemperatureReading, background_tasks: BackgroundTasks):
    """Submit a temperature reading from a sensor"""
    
    # Validate temperature range (reasonable weather temperatures)
    if not -50 <= reading.temperature <= 60:
        raise HTTPException(
            status_code=400,
            detail="Temperature must be between -50°C and 60°C"
        )
    
    # Store reading in background to avoid blocking the API
    background_tasks.add_task(store_temperature_reading, reading)
    
    return {
        "message": "Temperature reading submitted successfully",
        "sensor_id": reading.sensor_id,
        "temperature": reading.temperature,
        "timestamp": reading.timestamp or datetime.now()
    }

@app.get("/readings/recent", response_model=List[TemperatureResponse])
async def get_recent_temperature_readings(limit: int = 10):
    """Get recent temperature readings"""
    
    if limit > 100:
        raise HTTPException(
            status_code=400,
            detail="Limit cannot exceed 100"
        )
    
    readings = get_recent_readings(limit)
    return readings

@app.get("/analytics/average-hour", response_model=AverageTemperatureResponse)
async def get_hourly_average():
    """Get average temperature for the last hour (checks cache first)"""
    
    result = get_average_temperature_last_hour()
    
    if not result:
        raise HTTPException(
            status_code=404,
            detail="No temperature data available for the last hour"
        )
    
    return result

@app.get("/status")
async def get_system_status():
    """Get system status including cache and database information"""
    
    try:
        # Test database connection
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as total FROM temperature_readings")
            total_readings = cursor.fetchone()["total"]
            
            cursor.execute("""
                SELECT COUNT(*) as recent_count 
                FROM temperature_readings 
                WHERE timestamp > datetime('now', '-1 hour')
            """)
            recent_readings = cursor.fetchone()["recent_count"]
        
        return {
            "status": "healthy",
            "database": {
                "total_readings": total_readings,
                "recent_readings_last_hour": recent_readings,
                "connection_pool_active": db_pool.active_connections
            },
            "cache": {
                "size": lru_cache.size(),
                "capacity": lru_cache.capacity
            },
            "timestamp": datetime.now()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=503,
            detail="System health check failed"
        )

@app.delete("/readings/clear")
async def clear_all_data():
    """Clear all temperature readings (for testing purposes)"""
    
    try:
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM temperature_readings")
            conn.commit()
            deleted_count = cursor.rowcount
        
        # Clear cache
        lru_cache.clear()
        
        return {
            "message": "All temperature readings cleared",
            "deleted_count": deleted_count
        }
        
    except Exception as e:
        logger.error(f"Error clearing data: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to clear data"
        )

# Simulate sensor data (for testing)
@app.post("/simulate/sensor-data")
async def simulate_sensor_data(
    background_tasks: BackgroundTasks,
    sensor_count: int = 3,
    readings_per_sensor: int = 60
):
    """Simulate sensor data for testing (creates readings for the last hour)"""
    
    if sensor_count > 10 or readings_per_sensor > 120:
        raise HTTPException(
            status_code=400,
            detail="Too many sensors or readings requested"
        )
    
    import random
    
    def generate_test_data():
        base_time = datetime.now() - timedelta(hours=1)
        
        for sensor_id in range(1, sensor_count + 1):
            for i in range(readings_per_sensor):
                # Generate realistic temperature data
                base_temp = 20 + random.uniform(-5, 5)  # Base temperature around 20°C
                temperature = base_temp + random.uniform(-2, 2)  # Small variations
                
                timestamp = base_time + timedelta(minutes=i)
                
                reading = TemperatureReading(
                    temperature=round(temperature, 1),
                    sensor_id=f"sensor_{sensor_id:02d}",
                    timestamp=timestamp
                )
                
                store_temperature_reading(reading)
                time.sleep(0.01)  # Small delay to avoid overwhelming the system
    
    background_tasks.add_task(generate_test_data)
    
    return {
        "message": "Simulating sensor data generation",
        "sensors": sensor_count,
        "readings_per_sensor": readings_per_sensor,
        "total_readings": sensor_count * readings_per_sensor
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)