# Weather Station Data Logger - Local Data Platform

A high-performance local weather monitoring system built with FastAPI, featuring LRU caching, connection pooling, and time-series data management.

## 🌡️ Features

### 🔄 **Core Functionality**
- **Real-time Data Ingestion**: Accept temperature readings from multiple sensors
- **Time-series Storage**: Store readings with timestamps in SQLite database
- **Analytics**: Calculate average temperatures for specified time periods
- **Data Retrieval**: Get recent readings and historical data

### ⚡ **Performance Optimizations**
- **LRU Cache**: 100-item cache for most recent readings to minimize database hits
- **Connection Pooling**: Managed SQLite connections (2-5 connections) for concurrent writes
- **Background Processing**: Non-blocking data storage using FastAPI background tasks
- **Indexed Queries**: Optimized database queries with proper indexing

### 🛡️ **Production Features**
- **Input Validation**: Temperature range validation and data integrity checks
- **Error Handling**: Comprehensive error handling and logging
- **Health Monitoring**: System status endpoint with cache and database metrics
- **Thread Safety**: Thread-safe LRU cache and connection pool implementations

## 🚀 Quick Start

### 1. Clone and Setup
```bash
git clone https://github.com/Amruth22/W1D4S3--Local-Data-Platform.git
cd W1D4S3--Local-Data-Platform
pip install -r requirements.txt
```

### 2. Run the Application
```bash
python main.py
```

The API will be available at: `http://localhost:8000`

### 3. View API Documentation
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 📊 API Endpoints

### **Data Ingestion**

#### Submit Temperature Reading
```http
POST /readings
Content-Type: application/json

{
    "temperature": 23.5,
    "sensor_id": "sensor_01",
    "timestamp": "2024-01-15T10:30:00"  // Optional, defaults to current time
}
```

**Response:**
```json
{
    "message": "Temperature reading submitted successfully",
    "sensor_id": "sensor_01",
    "temperature": 23.5,
    "timestamp": "2024-01-15T10:30:00"
}
```

### **Data Retrieval**

#### Get Recent Readings
```http
GET /readings/recent?limit=10
```

**Response:**
```json
[
    {
        "timestamp": "2024-01-15T10:30:00",
        "temperature": 23.5,
        "sensor_id": "sensor_01"
    }
]
```

#### Get Hourly Average (Cache-First)
```http
GET /analytics/average-hour
```

**Response:**
```json
{
    "average_temperature": 22.8,
    "period_start": "2024-01-15T09:30:00",
    "period_end": "2024-01-15T10:30:00",
    "readings_count": 45,
    "data_source": "cache"  // or "database"
}
```

### **System Management**

#### System Status
```http
GET /status
```

**Response:**
```json
{
    "status": "healthy",
    "database": {
        "total_readings": 1250,
        "recent_readings_last_hour": 45,
        "connection_pool_active": 3
    },
    "cache": {
        "size": 85,
        "capacity": 100
    },
    "timestamp": "2024-01-15T10:30:00"
}
```

## 🧪 Testing & Simulation

### Generate Test Data
```http
POST /simulate/sensor-data
Content-Type: application/json

{
    "sensor_count": 3,
    "readings_per_sensor": 60
}
```

This creates realistic temperature data for the last hour from multiple sensors.

### Clear All Data (Testing)
```http
DELETE /readings/clear
```

## 🏗️ Architecture

### **LRU Cache Implementation**
```python
# Thread-safe LRU cache with 100-item capacity
lru_cache = LRUCache(capacity=100)

# Cache-first query strategy
def get_average_temperature_last_hour():
    # 1. Check cache for recent data
    cache_readings = lru_cache.get_recent_readings()
    
    # 2. If sufficient cache data, use it
    if len(cache_data) >= 30:
        return calculate_from_cache(cache_data)
    
    # 3. Fallback to database query
    return query_database()
```

### **Connection Pool Management**
```python
# SQLite connection pool (2-5 connections)
db_pool = SQLiteConnectionPool(
    database_path="weather_data.db",
    min_connections=2,
    max_connections=5
)

# Context manager for safe connection handling
with db_pool.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("INSERT INTO temperature_readings ...")
    conn.commit()
```

### **Database Schema**
```sql
CREATE TABLE temperature_readings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME NOT NULL,
    temperature REAL NOT NULL,
    sensor_id TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Optimized indexes for time-series queries
CREATE INDEX idx_timestamp ON temperature_readings(timestamp);
CREATE INDEX idx_sensor_timestamp ON temperature_readings(sensor_id, timestamp);
```

## 📈 Performance Benefits

### **Cache Performance**
- **Cache Hit**: ~0.1ms response time for recent data
- **Database Query**: ~10-50ms response time
- **Cache Efficiency**: 80-90% hit rate for recent queries

### **Connection Pooling Benefits**
- **Concurrent Writes**: Handle multiple sensor writes simultaneously
- **Resource Management**: Efficient connection reuse
- **Scalability**: Support for high-frequency data ingestion

### **Query Optimization**
- **Indexed Lookups**: Fast time-range queries
- **Background Processing**: Non-blocking API responses
- **Memory Efficiency**: LRU eviction of old cache entries

## 🔧 Configuration

### **Cache Settings**
```python
# Adjust cache size based on memory constraints
lru_cache = LRUCache(capacity=100)  # Default: 100 readings
```

### **Connection Pool Settings**
```python
# Tune based on concurrent load
db_pool = SQLiteConnectionPool(
    min_connections=2,    # Always available connections
    max_connections=5     # Maximum concurrent connections
)
```

### **Temperature Validation**
```python
# Reasonable weather temperature range
if not -50 <= reading.temperature <= 60:
    raise HTTPException(status_code=400, detail="Invalid temperature range")
```

## 📊 Usage Examples

### **Python Client Example**
```python
import requests
import json
from datetime import datetime

# Submit temperature reading
reading_data = {
    "temperature": 25.3,
    "sensor_id": "outdoor_sensor",
    "timestamp": datetime.now().isoformat()
}

response = requests.post(
    "http://localhost:8000/readings",
    json=reading_data
)
print(response.json())

# Get hourly average
avg_response = requests.get("http://localhost:8000/analytics/average-hour")
print(f"Average temperature: {avg_response.json()['average_temperature']}°C")
```

### **Curl Examples**
```bash
# Submit reading
curl -X POST "http://localhost:8000/readings" \
     -H "Content-Type: application/json" \
     -d '{"temperature": 22.5, "sensor_id": "sensor_01"}'

# Get recent readings
curl "http://localhost:8000/readings/recent?limit=5"

# Get hourly average
curl "http://localhost:8000/analytics/average-hour"

# Check system status
curl "http://localhost:8000/status"
```

## 🎯 Learning Objectives

This project demonstrates several important concepts:

### **1. Caching Strategies**
- **LRU (Least Recently Used)** cache implementation
- **Cache-first** query patterns
- **Memory management** with automatic eviction

### **2. Connection Pooling**
- **Resource management** for database connections
- **Concurrent access** handling
- **Connection lifecycle** management

### **3. Time-Series Data**
- **Timestamp-based** data storage
- **Time-range queries** optimization
- **Data aggregation** (averages, counts)

### **4. Performance Optimization**
- **Background processing** for non-blocking APIs
- **Database indexing** for fast queries
- **Thread-safe** data structures

### **5. Production Patterns**
- **Health monitoring** and status endpoints
- **Input validation** and error handling
- **Logging** and observability

## 🔍 Monitoring & Debugging

### **Check Cache Performance**
```http
GET /status
```
Monitor `cache.size` and database query frequency.

### **Database Query Analysis**
```sql
-- Check recent data distribution
SELECT 
    sensor_id,
    COUNT(*) as reading_count,
    AVG(temperature) as avg_temp,
    MIN(timestamp) as first_reading,
    MAX(timestamp) as last_reading
FROM temperature_readings 
WHERE timestamp > datetime('now', '-1 hour')
GROUP BY sensor_id;
```

### **Performance Metrics**
- **API Response Time**: Monitor `/analytics/average-hour` response times
- **Cache Hit Rate**: Compare cache vs database data sources
- **Connection Pool Usage**: Monitor active connections in status endpoint

## 🚀 Production Considerations

For production deployment, consider:

1. **Database**: Upgrade to PostgreSQL/MySQL for better concurrent performance
2. **Caching**: Use Redis for distributed caching across multiple instances
3. **Monitoring**: Add Prometheus metrics and Grafana dashboards
4. **Security**: Add authentication and rate limiting
5. **Scaling**: Implement horizontal scaling with load balancers
6. **Backup**: Regular database backups and disaster recovery
7. **Configuration**: Environment-based configuration management

## 📝 Dependencies

- **FastAPI**: Modern, fast web framework for building APIs
- **Uvicorn**: ASGI server for running FastAPI applications
- **Pydantic**: Data validation and serialization
- **SQLite**: Lightweight, serverless database (built into Python)

## 🤝 Contributing

This is an educational project demonstrating local data platform concepts. Feel free to:

- Fork and experiment with different caching strategies
- Add new analytics endpoints
- Implement additional sensor types
- Optimize database queries
- Add monitoring and alerting features

## 📄 License

This project is for educational purposes. Feel free to use and modify as needed.

---

**Built with ❤️ for learning data platform concepts**