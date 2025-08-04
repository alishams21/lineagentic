import requests
import json
import asyncio
from typing import Dict, Any

class SparkLineageAPIClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        
    def health_check(self) -> Dict[str, Any]:
        """Check if the API is running"""
        response = requests.get(f"{self.base_url}/health")
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response text: {response.text}")
            response.raise_for_status()
        return response.json()
    
    def analyze_query(self, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "spark-lineage-agent") -> Dict[str, Any]:
        """
        Analyze a single Spark query using the spark_lineage_agent plugin
        
        Args:
            query: Spark query to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            
        Returns:
            Analysis results
        """
        payload = {
            "query": query,
            "model_name": model_name,
            "agent_name": agent_name
        }
        
        response = requests.post(f"{self.base_url}/analyze", json=payload)
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response text: {response.text}")
            response.raise_for_status()
        return response.json()
    
    def analyze_queries_batch(self, queries: list[str], model_name: str = "gpt-4o-mini", agent_name: str = "spark-lineage-agent") -> Dict[str, Any]:
        """
        Analyze multiple Spark queries in batch using the spark_lineage_agent plugin
        
        Args:
            queries: List of Spark queries to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            
        Returns:
            Batch analysis results
        """
        payload = {
            "queries": queries,
            "model_name": model_name,
            "agent_name": agent_name
        }
        
        response = requests.post(f"{self.base_url}/analyze/batch", json=payload)
        return response.json()
    
   
    def run_operation(self, operation_name: str, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "spark-lineage-agent") -> Dict[str, Any]:
        """
        Run a specific operation using the appropriate plugin
        
        Args:
            operation_name: The operation to perform (e.g., "spark_lineage_analysis")
            query: Spark query to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            
        Returns:
            Operation results
        """
        payload = {
            "query": query,
            "model_name": model_name,
            "agent_name": agent_name
        }
        
        response = requests.post(f"{self.base_url}/operation/{operation_name}", json=payload)
        return response.json()

def main():
    """Example usage of the API client"""
    
    # Initialize client
    client = SparkLineageAPIClient()
    
    # Check if API is running
    print("Checking API health...")
    health = client.health_check()
    print(f"Health status: {health}")
    print()
    
    # Example Spark query
    sample_query = """
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, when, year, current_date, datediff, lit
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

        # Initialize Spark session
        spark = SparkSession.builder \\
            .appName("CustomerDataProcessing") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .getOrCreate()

        # Step 1: Read from customer_3 table
        df = spark.table('customer_3')

        # Step 2: Clean whitespace from names
        df = df.withColumn('first_name', col('first_name').cast('string').trim()) \\
               .withColumn('last_name', col('last_name').cast('string').trim())

        # Step 3: Create full name
        df = df.withColumn('full_name', col('first_name') + ' ' + col('last_name'))

        # Step 4: Convert birthdate to date and calculate age
        df = df.withColumn('birthdate', col('birthdate').cast('date')) \\
               .withColumn('age', year(current_date()) - year(col('birthdate')))

        # Step 5: Categorize by age group
        df = df.withColumn('age_group', 
            when(col('age') >= 60, 'Senior') \\
            .when(col('age') >= 30, 'Adult') \\
            .otherwise('Young'))

        # Step 6: Filter out rows with missing email
        df = df.filter(col('email').isNotNull())

        # Step 7: Write result to customer_4 table
        df.write.mode('overwrite').saveAsTable('customer_4')
    """

    # Run Spark lineage agent directly
    print("Running Spark lineage agent directly...")
    lineage_result = client.analyze_query(sample_query)
    print(f"Spark lineage agent result: {json.dumps(lineage_result, indent=8)}")
    print()

  
if __name__ == "__main__":
    main() 