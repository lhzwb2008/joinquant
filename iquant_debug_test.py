import sys
import os
from datetime import datetime

def init(ContextInfo):
    print("=" * 60)
    print("iQuant Debug Test - Detailed Environment Check")
    print("=" * 60)
    
    # Basic info
    print(f"Python Version: {sys.version}")
    print(f"Python Executable: {sys.executable}")
    print(f"Current Directory: {os.getcwd()}")
    
    # Check critical modules
    print("\n" + "=" * 40)
    print("Module Availability Check:")
    print("=" * 40)
    
    modules_to_check = [
        'pymysql', 'pandas', 'numpy', 'json', 'time', 
        'datetime', 'requests', 'urllib', 'sqlite3',
        'csv', 'logging', 'threading', 'multiprocessing'
    ]
    
    available_modules = []
    missing_modules = []
    
    for module in modules_to_check:
        try:
            __import__(module)
            available_modules.append(module)
            print(f"✓ {module} - Available")
        except ImportError as e:
            missing_modules.append(module)
            print(f"✗ {module} - Missing: {e}")
    
    # Check file permissions
    print("\n" + "=" * 40)
    print("File System Check:")
    print("=" * 40)
    
    try:
        test_file = "temp_test.txt"
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        print("✓ File write/delete permissions - OK")
    except Exception as e:
        print(f"✗ File permissions issue: {e}")
    
    # Check network connectivity (basic)
    print("\n" + "=" * 40)
    print("Network & Database Check:")
    print("=" * 40)
    
    try:
        import socket
        socket.create_connection(("8.8.8.8", 53), timeout=3)
        print("✓ Basic network connectivity - OK")
    except Exception as e:
        print(f"✗ Network connectivity issue: {e}")
    
    # Try database connection
    try:
        import pymysql
        print("✓ PyMySQL module available")
        
        # Test connection (using your existing config)
        conn = pymysql.connect(
            host="sh-cdb-kgv8etuq.sql.tencentcdb.com",
            port=23333,
            user="root",
            password="Hello2025",
            database='order',
            charset='utf8',
            connect_timeout=5
        )
        conn.close()
        print("✓ Database connection - OK")
    except ImportError:
        print("✗ PyMySQL not available")
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
    
    # Memory and performance info
    print("\n" + "=" * 40)
    print("System Resources:")
    print("=" * 40)
    
    try:
        import psutil
        memory = psutil.virtual_memory()
        print(f"✓ Memory: {memory.percent}% used")
        print(f"✓ Available memory: {memory.available / (1024**3):.2f} GB")
    except ImportError:
        print("✗ psutil not available - cannot check memory")
    
    # Summary
    print("\n" + "=" * 40)
    print("Summary:")
    print("=" * 40)
    print(f"Available modules: {len(available_modules)}/{len(modules_to_check)}")
    print(f"Missing modules: {missing_modules}")
    
    if len(missing_modules) > 0:
        print("\nRecommendations:")
        print("- Consider installing missing modules if needed")
        print("- Check if alternative modules are available")
    
    print("=" * 60)
    print("Debug Test Complete")
    print("=" * 60)

def handlebar(ContextInfo):
    pass

if __name__ == "__main__":
    class MockContextInfo:
        pass
    
    init(MockContextInfo()) 