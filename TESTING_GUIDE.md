# Testing Guide for Refactored Algorithm Package

## Overview

After refactoring the code to use proper relative imports and package structure, here are the different ways to test your code:

## ✅ **Working Test Methods**

### 1. **Run All Tests**
```bash
python run_tests.py
```

### 2. **Run Specific Test Module**
```bash
python run_tests.py test_tracers
python run_tests.py test_database
```

### 3. **Run Tests as Modules**
```bash
python -m tests.test_tracers
python -m tests.test_database
```

### 4. **Run Individual Test Classes**
```bash
python -m tests.test_tracers TestLogTracer
python -m tests.test_database TestDatabase
```

### 5. **Run Individual Test Methods**
```bash
python -m tests.test_tracers TestLogTracer.test_log_trace_id_generation
```

## ❌ **What Doesn't Work Anymore**

### Direct File Execution (Before Refactoring)
```bash
# ❌ These will FAIL with "ImportError: attempted relative import with no known parent package"
cd algorithm/utils && python tracers.py
python algorithm/framework_agent.py
python algorithm/utils/database.py
```

### Why This Happened
- **Before**: Used absolute imports like `from algorithm.utils.tracers import LogTracer`
- **After**: Uses relative imports like `from .utils.tracers import LogTracer`
- Relative imports only work when the code is run as part of a package

## ✅ **Alternative Ways to Run Code**

### 1. **Run as Modules (Recommended)**
```bash
python -m algorithm.framework_agent
python -m algorithm.utils.tracers
python -m algorithm.utils.database
```

### 2. **Use Entry Points**
```bash
python run_framework.py
```

### 3. **Import from Package**
```python
from algorithm import AgentFramework, main
from algorithm.utils.tracers import LogTracer
from algorithm.utils.database import write_lineage_log
```

## 📁 **Test Structure**

```
lineagentic/
├── tests/
│   ├── __init__.py
│   ├── test_tracers.py      # Tests for tracers module
│   └── test_database.py     # Tests for database module
├── run_tests.py             # Test runner script
└── algorithm/
    ├── __init__.py          # Package exports
    ├── framework_agent.py   # Main framework
    └── utils/
        ├── tracers.py       # Tracing functionality
        └── database.py      # Database operations
```

## 🧪 **Writing New Tests**

### Template for New Test File
```python
#!/usr/bin/env python3
"""
Tests for algorithm.module_name module.
Run with: python -m tests.test_module_name
"""

import unittest
import sys
import os
from unittest.mock import Mock, patch

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from algorithm.module_name import function_name


class TestModuleName(unittest.TestCase):
    """Test cases for module_name module"""
    
    def setUp(self):
        """Set up test fixtures"""
        pass
    
    def test_function_name(self):
        """Test function_name function"""
        # Your test code here
        pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
```

### Running Your New Tests
```bash
# Run all tests
python run_tests.py

# Run specific test module
python run_tests.py test_your_module

# Run as module
python -m tests.test_your_module
```

## 🔧 **Test Best Practices**

### 1. **Use Mocks for External Dependencies**
```python
@patch('algorithm.utils.tracers.write_lineage_log')
def test_function(self, mock_write_log):
    # Test code here
    mock_write_log.assert_called_once()
```

### 2. **Test Function Signatures**
```python
def test_function_exists(self):
    """Test that functions exist and are callable"""
    self.assertTrue(callable(function_name))
    
    import inspect
    sig = inspect.signature(function_name)
    self.assertEqual(len(sig.parameters), expected_count)
```

### 3. **Test Error Handling**
```python
def test_function_with_exception(self):
    """Test function behavior when exceptions occur"""
    try:
        function_name()
    except Exception as e:
        # Test that the exception is handled properly
        pass
```

## 🚀 **Quick Start for Testing**

1. **Run all tests:**
   ```bash
   python run_tests.py
   ```

2. **Run specific module tests:**
   ```bash
   python run_tests.py test_tracers
   ```

3. **Add new tests:**
   - Create `tests/test_your_module.py`
   - Follow the template above
   - Run with `python run_tests.py`

4. **Debug tests:**
   ```bash
   python -m tests.test_tracers -v
   ```

