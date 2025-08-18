#!/usr/bin/env python3
"""
Tests for lf_algorithm.utils.tracers module.
Run with: python -m tests.test_tracers
"""

import unittest
import sys
import os
from unittest.mock import Mock, patch

# Add the project root to the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lf_algorithm.utils.tracers import LogTracer, log_trace_id


class TestLogTracer(unittest.TestCase):
    """Test cases for LogTracer class"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.tracer = LogTracer()
    
    def test_log_trace_id_generation(self):
        """Test that log_trace_id generates proper format"""
        trace_id = log_trace_id("test")
        
        # Should start with "trace_"
        self.assertTrue(trace_id.startswith("trace_"))
        
        # Should be 32 chars after "trace_"
        self.assertEqual(len(trace_id), len("trace_") + 32)
        
        # Should contain the tag
        self.assertIn("test", trace_id)
    
    def test_get_name_with_valid_trace_id(self):
        """Test get_name method with valid trace ID"""
        # Create a mock trace with a valid trace_id
        mock_trace = Mock()
        mock_trace.trace_id = "trace_test01234567890123456789012345"
        
        name = self.tracer.get_name(mock_trace)
        self.assertEqual(name, "test")
    
    def test_get_name_with_invalid_trace_id(self):
        """Test get_name method with invalid trace ID"""
        # Create a mock trace with an invalid trace_id
        mock_trace = Mock()
        mock_trace.trace_id = "invalid_trace_id"
        
        name = self.tracer.get_name(mock_trace)
        self.assertIsNone(name)
    
    def test_get_name_with_no_zero_in_tag(self):
        """Test get_name method when tag doesn't contain '0'"""
        # Create a mock trace with a tag that doesn't contain '0'
        mock_trace = Mock()
        mock_trace.trace_id = "trace_abc01234567890123456789012345"
        
        name = self.tracer.get_name(mock_trace)
        self.assertEqual(name, "abc")
    
    @patch('lf_algorithm.utils.tracers.write_lineage_log')
    def test_on_trace_start(self, mock_write_log):
        """Test on_trace_start method"""
        mock_trace = Mock()
        mock_trace.trace_id = "trace_test01234567890123456789012345"
        mock_trace.name = "test_trace"
        
        self.tracer.on_trace_start(mock_trace)
        
        # Verify write_lineage_log was called
        mock_write_log.assert_called_once_with("test", "trace", "Started: test_trace")
    
    @patch('lf_algorithm.utils.tracers.write_lineage_log')
    def test_on_trace_end(self, mock_write_log):
        """Test on_trace_end method"""
        mock_trace = Mock()
        mock_trace.trace_id = "trace_test01234567890123456789012345"
        mock_trace.name = "test_trace"
        
        self.tracer.on_trace_end(mock_trace)
        
        # Verify write_lineage_log was called
        mock_write_log.assert_called_once_with("test", "trace", "Ended: test_trace")
    
    @patch('lf_algorithm.utils.tracers.write_lineage_log')
    def test_on_span_start(self, mock_write_log):
        """Test on_span_start method"""
        mock_span = Mock()
        mock_span.trace_id = "trace_test01234567890123456789012345"
        mock_span.span_data = Mock()
        mock_span.span_data.type = "function"
        mock_span.span_data.name = None
        mock_span.span_data.server = None
        mock_span.error = None
        
        self.tracer.on_span_start(mock_span)
        
        # Verify write_lineage_log was called
        mock_write_log.assert_called_once_with("test", "function", "Started function")
    
    @patch('lf_algorithm.utils.tracers.write_lineage_log')
    def test_on_span_end(self, mock_write_log):
        """Test on_span_end method"""
        mock_span = Mock()
        mock_span.trace_id = "trace_test01234567890123456789012345"
        mock_span.span_data = Mock()
        mock_span.span_data.type = "function"
        mock_span.span_data.name = None
        mock_span.span_data.server = None
        mock_span.error = None
        
        self.tracer.on_span_end(mock_span)
        
        # Verify write_lineage_log was called
        mock_write_log.assert_called_once_with("test", "function", "Ended function")


class TestLogTraceId(unittest.TestCase):
    """Test cases for log_trace_id function"""
    
    def test_log_trace_id_length(self):
        """Test that log_trace_id generates correct length"""
        trace_id = log_trace_id("short")
        expected_length = len("trace_") + 32
        self.assertEqual(len(trace_id), expected_length)
    
    def test_log_trace_id_format(self):
        """Test that log_trace_id follows correct format"""
        trace_id = log_trace_id("test")
        
        # Should start with "trace_"
        self.assertTrue(trace_id.startswith("trace_"))
        
        # Should contain the tag
        self.assertIn("test", trace_id)
        
        # Should contain random alphanumeric characters
        import string
        alphanum = string.ascii_lowercase + string.digits
        suffix = trace_id[len("trace_test"):]
        self.assertTrue(all(c in alphanum for c in suffix))
    
    def test_log_trace_id_uniqueness(self):
        """Test that log_trace_id generates unique IDs"""
        ids = set()
        for _ in range(100):
            trace_id = log_trace_id("test")
            self.assertNotIn(trace_id, ids)
            ids.add(trace_id)


if __name__ == "__main__":
    # Create tests directory if it doesn't exist
    os.makedirs("tests", exist_ok=True)
    
    # Run the tests
    unittest.main(verbosity=2) 