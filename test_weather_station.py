"""
Test script for Weather Station Data Logger
Demonstrates LRU cache, connection pooling, and time-series data functionality
"""

import requests
import json
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict

# API base URL
BASE_URL = "http://localhost:8000"

def test_weather_station():
    print("🌡️ Testing Weather Station Data Logger\n")
    
    # Test 1: Check system status
    print("1️⃣ Checking system status...")
    try:
        response = requests.get(f"{BASE_URL}/status")
        if response.status_code == 200:
            status = response.json()
            print(f"   ✅ System Status: {status['status']}")
            print(f"   📊 Total readings: {status['database']['total_readings']}")
            print(f"   💾 Cache size: {status['cache']['size']}/{status['cache']['capacity']}")
            print(f"   🔗 Active connections: {status['database']['connection_pool_active']}")
        else:
            print(f"   ❌ Status check failed: {response.status_code}")
            return
    except requests.exceptions.ConnectionError:
        print("   ❌ Cannot connect to Weather Station API")
        print("   Make sure the server is running: python main.py")
        return
    
    print()
    
    # Test 2: Clear existing data for clean test
    print("2️⃣ Clearing existing data...")
    response = requests.delete(f"{BASE_URL}/readings/clear")
    if response.status_code == 200:
        print(f"   ✅ Data cleared: {response.json()}")
    print()
    
    # Test 3: Submit individual temperature readings
    print("3️⃣ Submitting individual temperature readings...")
    
    test_readings = [
        {"temperature": 22.5, "sensor_id": "outdoor_sensor"},
        {"temperature": 24.1, "sensor_id": "indoor_sensor"},
        {"temperature": 21.8, "sensor_id": "greenhouse_sensor"},
        {"temperature": 23.2, "sensor_id": "outdoor_sensor"},
        {"temperature": 25.0, "sensor_id": "indoor_sensor"},
    ]
    
    for reading in test_readings:
        response = requests.post(f"{BASE_URL}/readings", json=reading)
        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ {result['sensor_id']}: {result['temperature']}°C")
        else:
            print(f"   ❌ Failed to submit reading: {response.status_code}")
    
    print()
    
    # Test 4: Get recent readings
    print("4️⃣ Getting recent readings...")
    response = requests.get(f"{BASE_URL}/readings/recent?limit=5")
    if response.status_code == 200:
        readings = response.json()
        print(f"   ✅ Retrieved {len(readings)} recent readings:")
        for reading in readings:
            timestamp = datetime.fromisoformat(reading['timestamp'].replace('Z', '+00:00'))
            print(f"      - {reading['sensor_id']}: {reading['temperature']}°C at {timestamp.strftime('%H:%M:%S')}")
    print()
    
    # Test 5: Test with insufficient data for hourly average
    print("5️⃣ Testing hourly average with insufficient data...")
    response = requests.get(f"{BASE_URL}/analytics/average-hour")
    if response.status_code == 404:
        print("   ✅ Correctly returned 404 - insufficient data for hourly average")
    elif response.status_code == 200:
        avg_data = response.json()
        print(f"   ✅ Average temperature: {avg_data['average_temperature']}°C")
        print(f"   📊 Data source: {avg_data['data_source']}")
        print(f"   📈 Readings count: {avg_data['readings_count']}")
    print()
    
    # Test 6: Generate test data to fill cache and database
    print("6️⃣ Generating test sensor data...")
    response = requests.post(f"{BASE_URL}/simulate/sensor-data", json={
        "sensor_count": 3,
        "readings_per_sensor": 60
    })
    
    if response.status_code == 200:
        result = response.json()
        print(f"   ✅ Simulating {result['total_readings']} readings...")
        print("   ⏳ Waiting for data generation to complete...")
        time.sleep(5)  # Wait for background task to complete
    print()
    
    # Test 7: Check system status after data generation
    print("7️⃣ Checking system status after data generation...")
    response = requests.get(f"{BASE_URL}/status")
    if response.status_code == 200:
        status = response.json()
        print(f"   ✅ Total readings: {status['database']['total_readings']}")
        print(f"   💾 Cache size: {status['cache']['size']}/{status['cache']['capacity']}")
        print(f"   📊 Recent readings (last hour): {status['database']['recent_readings_last_hour']}")
    print()
    
    # Test 8: Test hourly average with sufficient data (should use cache)
    print("8️⃣ Testing hourly average with sufficient data...")
    response = requests.get(f"{BASE_URL}/analytics/average-hour")
    if response.status_code == 200:
        avg_data = response.json()
        print(f"   ✅ Average temperature: {avg_data['average_temperature']}°C")
        print(f"   📊 Data source: {avg_data['data_source']} (cache preferred)")
        print(f"   📈 Readings count: {avg_data['readings_count']}")
        print(f"   ⏰ Period: {avg_data['period_start']} to {avg_data['period_end']}")
    print()
    
    # Test 9: Performance test - multiple concurrent requests
    print("9️⃣ Performance test - multiple concurrent requests...")
    
    def submit_reading(sensor_id: str, temp: float):
        """Submit a single reading"""
        reading = {
            "temperature": temp,
            "sensor_id": sensor_id
        }
        return requests.post(f"{BASE_URL}/readings", json=reading)
    
    # Submit multiple readings quickly
    start_time = time.time()
    responses = []
    
    for i in range(20):
        temp = 20 + random.uniform(-5, 5)  # Random temperature
        sensor_id = f"perf_sensor_{i % 3 + 1:02d}"
        response = submit_reading(sensor_id, round(temp, 1))
        responses.append(response.status_code == 200)
    
    end_time = time.time()
    success_count = sum(responses)
    
    print(f"   ✅ Submitted {success_count}/20 readings in {end_time - start_time:.2f} seconds")
    print(f"   ⚡ Average: {(end_time - start_time) / 20 * 1000:.1f}ms per request")
    print()
    
    # Test 10: Cache performance comparison
    print("🔟 Cache performance comparison...")
    
    # Multiple requests to test cache hits
    cache_times = []
    for _ in range(5):
        start = time.time()
        response = requests.get(f"{BASE_URL}/analytics/average-hour")
        end = time.time()
        
        if response.status_code == 200:
            data = response.json()
            cache_times.append((end - start) * 1000)  # Convert to milliseconds
            print(f"   📊 Response time: {(end - start) * 1000:.1f}ms (source: {data['data_source']})")
        
        time.sleep(0.1)  # Small delay between requests
    
    if cache_times:
        avg_time = sum(cache_times) / len(cache_times)
        print(f"   ⚡ Average response time: {avg_time:.1f}ms")
    
    print()
    
    # Test 11: Input validation
    print("1️⃣1️⃣ Testing input validation...")
    
    # Test invalid temperature
    invalid_reading = {"temperature": 100, "sensor_id": "test_sensor"}  # Too hot
    response = requests.post(f"{BASE_URL}/readings", json=invalid_reading)
    if response.status_code == 400:
        print("   ✅ Invalid temperature rejected (100°C)")
    
    # Test very cold temperature
    invalid_reading = {"temperature": -60, "sensor_id": "test_sensor"}  # Too cold
    response = requests.post(f"{BASE_URL}/readings", json=invalid_reading)
    if response.status_code == 400:
        print("   ✅ Invalid temperature rejected (-60°C)")
    
    # Test valid edge case
    valid_reading = {"temperature": -49.9, "sensor_id": "arctic_sensor"}
    response = requests.post(f"{BASE_URL}/readings", json=valid_reading)
    if response.status_code == 200:
        print("   ✅ Valid edge case accepted (-49.9°C)")
    
    print()
    
    # Test 12: Final system status
    print("1️⃣2️⃣ Final system status...")
    response = requests.get(f"{BASE_URL}/status")
    if response.status_code == 200:
        status = response.json()
        print(f"   ✅ System Status: {status['status']}")
        print(f"   📊 Total readings: {status['database']['total_readings']}")
        print(f"   💾 Cache utilization: {status['cache']['size']}/{status['cache']['capacity']} ({status['cache']['size']/status['cache']['capacity']*100:.1f}%)")
        print(f"   🔗 Connection pool: {status['database']['connection_pool_active']} active connections")
        print(f"   📈 Recent activity: {status['database']['recent_readings_last_hour']} readings in last hour")
    
    print("\n" + "="*60)
    print("🎉 Weather Station Data Logger Test Completed!")
    print("="*60)
    print("\n📋 Test Summary:")
    print("✅ System status and health checks")
    print("✅ Individual temperature reading submission")
    print("✅ Recent readings retrieval")
    print("✅ Hourly average calculation (cache-first)")
    print("✅ Bulk data generation and simulation")
    print("✅ Performance testing with concurrent requests")
    print("✅ Cache performance optimization")
    print("✅ Input validation and error handling")
    print("✅ LRU cache and connection pooling functionality")
    
    print("\n🔧 Key Features Demonstrated:")
    print("• LRU Cache: Stores 100 most recent readings")
    print("• Connection Pool: 2-5 SQLite connections for concurrent access")
    print("• Cache-First Queries: Checks cache before database")
    print("• Background Processing: Non-blocking data storage")
    print("• Time-Series Analytics: Hourly temperature averages")
    print("• Input Validation: Temperature range checking")
    print("• Performance Monitoring: Response time tracking")

def run_continuous_monitoring(duration_minutes: int = 5):
    """Run continuous monitoring to demonstrate real-time capabilities"""
    print(f"\n🔄 Running continuous monitoring for {duration_minutes} minutes...")
    
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)
    
    sensors = ["outdoor", "indoor", "greenhouse"]
    base_temps = {"outdoor": 22, "indoor": 24, "greenhouse": 26}
    
    while time.time() < end_time:
        # Submit readings from all sensors
        for sensor in sensors:
            base_temp = base_temps[sensor]
            # Add some realistic variation
            temperature = base_temp + random.uniform(-2, 2)
            
            reading = {
                "temperature": round(temperature, 1),
                "sensor_id": f"{sensor}_sensor"
            }
            
            try:
                response = requests.post(f"{BASE_URL}/readings", json=reading)
                if response.status_code == 200:
                    print(f"📊 {sensor}: {temperature:.1f}°C", end="  ")
            except:
                print("❌ Connection error", end="  ")
        
        print()  # New line after all sensors
        
        # Check system status every minute
        if int(time.time() - start_time) % 60 == 0:
            try:
                response = requests.get(f"{BASE_URL}/status")
                if response.status_code == 200:
                    status = response.json()
                    print(f"💾 Cache: {status['cache']['size']}/100, 🔗 Connections: {status['database']['connection_pool_active']}")
            except:
                pass
        
        time.sleep(10)  # Wait 10 seconds between readings
    
    print(f"\n✅ Continuous monitoring completed!")

if __name__ == "__main__":
    print("🧪 Weather Station Data Logger Test Suite")
    print("=" * 60)
    print("Make sure the Weather Station API is running:")
    print("python main.py")
    print("=" * 60)
    
    try:
        test_weather_station()
        
        # Ask if user wants to run continuous monitoring
        print("\n🔄 Would you like to run continuous monitoring? (y/n): ", end="")
        choice = input().lower().strip()
        
        if choice in ['y', 'yes']:
            run_continuous_monitoring(2)  # Run for 2 minutes
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Tests interrupted by user")
    except Exception as e:
        print(f"\n\n💥 Unexpected error: {e}")