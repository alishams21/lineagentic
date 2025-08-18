#!/usr/bin/env python3
"""
Production-ready example of using the lf_algorithm library.
"""

import asyncio
import logging
import logging.config
import json
import os
from datetime import datetime
from lf_algorithm import FrameworkAgent

def setup_production_logging():
    """Setup production-ready logging configuration."""
    
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Production logging configuration
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            },
            "json": {
                "format": '{"timestamp": "%(asctime)s", "logger": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}'
            },
            "simple": {
                "format": "%(levelname)s: %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "simple",
                "level": "INFO"
            },
            "file": {
                "class": "logging.FileHandler",
                "filename": "logs/app.log",
                "formatter": "detailed",
                "level": "DEBUG"
            },
            "lineage_file": {
                "class": "logging.FileHandler",
                "filename": "logs/lineage.log",
                "formatter": "json",
                "level": "INFO"
            },
            "error_file": {
                "class": "logging.FileHandler",
                "filename": "logs/errors.log",
                "formatter": "detailed",
                "level": "ERROR"
            }
        },
        "loggers": {
            # Application logger
            "myapp": {
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": False
            },
            # Library loggers
            "lf_algorithm": {
                "handlers": ["console", "file"],
                "level": "INFO",
                "propagate": False
            },
            "lf_algorithm.lineage": {
                "handlers": ["lineage_file"],
                "level": "INFO",
                "propagate": False
            }
        },
        "root": {
            "handlers": ["error_file"],
            "level": "ERROR"
        }
    }
    
    logging.config.dictConfig(LOGGING_CONFIG)

class LineageAnalyzer:
    """Production-ready lineage analyzer."""
    
    def __init__(self):
        self.logger = logging.getLogger("myapp")
        self.logger.info("LineageAnalyzer initialized")
    
    async def analyze_sql(self, query: str, agent_name: str = "sql-lineage-agent"):
        """Analyze SQL query with proper error handling."""
        try:
            self.logger.info(f"Starting SQL analysis for agent: {agent_name}")
            
            agent = FrameworkAgent(
                agent_name=agent_name,
                source_code=query
            )
            
            result = await agent.run_agent()
            
            # Log success
            if hasattr(result, 'to_dict'):
                result_dict = result.to_dict()
                self.logger.info(f"SQL analysis completed. Inputs: {len(result_dict.get('inputs', []))}, Outputs: {len(result_dict.get('outputs', []))}")
            else:
                self.logger.info("SQL analysis completed with raw result")
            
            return result
            
        except Exception as e:
            self.logger.error(f"SQL analysis failed: {e}")
            raise
    
    async def analyze_python(self, code: str):
        """Analyze Python code with proper error handling."""
        try:
            self.logger.info("Starting Python analysis")
            
            agent = FrameworkAgent(
                agent_name="python-lineage-agent",
                source_code=code
            )
            
            result = await agent.run_agent()
            
            if hasattr(result, 'to_dict'):
                result_dict = result.to_dict()
                self.logger.info(f"Python analysis completed. Inputs: {len(result_dict.get('inputs', []))}, Outputs: {len(result_dict.get('outputs', []))}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Python analysis failed: {e}")
            raise
    
    def save_results(self, results, filename: str = None):
        """Save results to file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"results_{timestamp}.json"
        
        try:
            if hasattr(results, 'to_dict'):
                data = results.to_dict()
            else:
                data = results
            
            with open(f"logs/{filename}", 'w') as f:
                json.dump(data, f, indent=2)
            
            self.logger.info(f"Results saved to logs/{filename}")
            
        except Exception as e:
            self.logger.error(f"Failed to save results: {e}")
            raise

async def main():
    """Main production function."""
    
    # Setup production logging
    setup_production_logging()
    
    # Create analyzer
    analyzer = LineageAnalyzer()
    
    # Example SQL analysis
    sql_query = """
    SELECT 
        u.user_id,
        u.name,
        COUNT(o.order_id) as order_count,
        SUM(o.amount) as total_amount
    FROM users u
    LEFT JOIN orders o ON u.user_id = o.user_id
    WHERE u.active = true
    GROUP BY u.user_id, u.name
    HAVING COUNT(o.order_id) > 0
    ORDER BY total_amount DESC
    """
    
    try:
        # Run SQL analysis
        sql_result = await analyzer.analyze_sql(sql_query)
        
        # Save results
        analyzer.save_results(sql_result, "sql_analysis.json")
        
        # Example Python analysis
        python_code = """
        def process_user_data(users, orders):
            # Filter active users
            active_users = [u for u in users if u['active']]
            
            # Join with orders
            user_orders = {}
            for order in orders:
                user_id = order['user_id']
                if user_id not in user_orders:
                    user_orders[user_id] = []
                user_orders[user_id].append(order)
            
            # Calculate statistics
            result = []
            for user in active_users:
                user_id = user['id']
                orders = user_orders.get(user_id, [])
                
                result.append({
                    'user_id': user_id,
                    'name': user['name'],
                    'order_count': len(orders),
                    'total_amount': sum(o['amount'] for o in orders)
                })
            
            return result
        """
        
        python_result = await analyzer.analyze_python(python_code)
        analyzer.save_results(python_result, "python_analysis.json")
        
        print("All analyses completed successfully!")
        print("Check the 'logs' directory for detailed logs and results.")
        
    except Exception as e:
        print(f"Analysis failed: {e}")
        print("Check logs/errors.log for details.")

if __name__ == "__main__":
    asyncio.run(main())
