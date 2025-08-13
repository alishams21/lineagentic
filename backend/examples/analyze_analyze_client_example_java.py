import requests
import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime
import uuid

class JavaLineageAPIClient:
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
    
    def analyze_query(self, query: str, model_name: str = "gpt-4o-mini", 
                     agent_name: str = "java-lineage-agent", save_to_db: bool = True,
                     save_to_neo4j: bool = True, lineage_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze a single SQL query using the sql_lineage_agent plugin
        
        Args:
            query: SQL query to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            save_to_db: Whether to save results to database
            save_to_neo4j: Whether to save lineage data to Neo4j
            lineage_config: Optional lineage configuration with required fields
            
        Returns:
            Analysis results
        """
        payload = {
            "query": query,
            "model_name": model_name,
            "agent_name": agent_name,
            "save_to_db": save_to_db,
            "save_to_neo4j": save_to_neo4j
        }
        
        # Add lineage config if provided
        if lineage_config:
            payload["lineage_config"] = lineage_config
        
        response = requests.post(f"{self.base_url}/analyze", json=payload)
        if response.status_code != 200:
            print(f"Error: HTTP {response.status_code}")
            print(f"Response text: {response.text}")
            response.raise_for_status()
        return response.json()
    
    def analyze_queries_batch(self, queries: list[str], model_name: str = "gpt-4o-mini", agent_name: str = "java-lineage-agent") -> Dict[str, Any]:
        """
        Analyze multiple Java queries in batch using the java_lineage_agent plugin
        
        Args:
            queries: List of Java queries to analyze
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
    
   
    def run_operation(self, operation_name: str, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "java-lineage-agent") -> Dict[str, Any]:
        """
        Run a specific operation using the appropriate plugin
        
        Args:
            operation_name: The operation to perform (e.g., "java_lineage_analysis")
            query: Java query to analyze
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
    client = JavaLineageAPIClient()
    
    # Check if API is running
    print("Checking API health...")
    health = client.health_check()
    print(f"Health status: {health}")
    print()
    
    # Example Java query
    sample_query = """
        import java.io.*;
        import java.util.*;
        import java.time.*;
        import java.time.format.*;
        import java.sql.*;
        import java.util.stream.*;

        public class CustomerDataProcessor {
            public static void main(String[] args) throws IOException {
                // Database connection parameters
                String url = "jdbc:mysql://localhost:3306/customer_db";
                String username = "root";
                String password = "password";
                
                try (Connection connection = DriverManager.getConnection(url, username, password)) {
                    // Step 1: Read from customer_1 table
                    List<Customer> customers = new ArrayList<>();
                    String selectQuery = "SELECT first_name, last_name, email, birthdate FROM customer_1";
                    
                    try (PreparedStatement stmt = connection.prepareStatement(selectQuery);
                         ResultSet rs = stmt.executeQuery()) {
                        
                        while (rs.next()) {
                            Customer customer = new Customer();
                            customer.setFirstName(rs.getString("first_name"));
                            customer.setLastName(rs.getString("last_name"));
                            customer.setEmail(rs.getString("email"));
                            customer.setBirthdate(rs.getString("birthdate"));
                            customers.add(customer);
                        }
                    }

                    // Step 2: Clean whitespace from names
                    customers.forEach(customer -> {
                        customer.setFirstName(customer.getFirstName().trim());
                        customer.setLastName(customer.getLastName().trim());
                    });

                    // Step 3: Create full name
                    customers.forEach(customer -> {
                        String fullName = customer.getFirstName() + " " + customer.getLastName();
                        customer.setFullName(fullName);
                    });

                    // Step 4: Convert birthdate and calculate age
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                    LocalDate today = LocalDate.now();
                    
                    customers.forEach(customer -> {
                        try {
                            LocalDate birthdate = LocalDate.parse(customer.getBirthdate(), formatter);
                            int age = Period.between(birthdate, today).getYears();
                            customer.setAge(age);
                        } catch (Exception e) {
                            customer.setAge(0);
                        }
                    });

                    // Step 5: Categorize by age group
                    customers.forEach(customer -> {
                        String ageGroup;
                        if (customer.getAge() >= 60) {
                            ageGroup = "Senior";
                        } else if (customer.getAge() >= 30) {
                            ageGroup = "Adult";
                        } else {
                            ageGroup = "Young";
                        }
                        customer.setAgeGroup(ageGroup);
                    });

                    // Step 6: Filter out rows with missing email
                    List<Customer> filteredCustomers = customers.stream()
                        .filter(customer -> customer.getEmail() != null && !customer.getEmail().isEmpty())
                        .collect(Collectors.toList());

                    // Step 7: Write result to customer_2 table
                    String insertQuery = "INSERT INTO customer_2 (first_name, last_name, email, birthdate, full_name, age, age_group) VALUES (?, ?, ?, ?, ?, ?, ?)";
                    
                    try (PreparedStatement insertStmt = connection.prepareStatement(insertQuery)) {
                        for (Customer customer : filteredCustomers) {
                            insertStmt.setString(1, customer.getFirstName());
                            insertStmt.setString(2, customer.getLastName());
                            insertStmt.setString(3, customer.getEmail());
                            insertStmt.setString(4, customer.getBirthdate());
                            insertStmt.setString(5, customer.getFullName());
                            insertStmt.setInt(6, customer.getAge());
                            insertStmt.setString(7, customer.getAgeGroup());
                            insertStmt.executeUpdate();
                        }
                    }
                    
                    System.out.println("Successfully processed " + filteredCustomers.size() + " customers from customer_1 to customer_2");
                    
                } catch (SQLException e) {
                    System.err.println("Database error: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        class Customer {
            private String firstName;
            private String lastName;
            private String email;
            private String birthdate;
            private String fullName;
            private int age;
            private String ageGroup;

            // Getters and setters
            public String getFirstName() { return firstName; }
            public void setFirstName(String firstName) { this.firstName = firstName; }
            public String getLastName() { return lastName; }
            public void setLastName(String lastName) { this.lastName = lastName; }
            public String getEmail() { return email; }
            public void setEmail(String email) { this.email = email; }
            public String getBirthdate() { return birthdate; }
            public void setBirthdate(String birthdate) { this.birthdate = birthdate; }
            public String getFullName() { return fullName; }
            public void setFullName(String fullName) { this.fullName = fullName; }
            public int getAge() { return age; }
            public void setAge(int age) { this.age = age; }
            public String getAgeGroup() { return ageGroup; }
            public void setAgeGroup(String ageGroup) { this.ageGroup = ageGroup; }
        }
    """

    # Example 3: Run with minimal required lineage config
    print("Running SQL lineage agent with minimal lineage configuration...")
    minimal_config = {
        "event_type": "START",
        "event_time": datetime.utcnow().isoformat() + "Z",
        "run_id": str(uuid.uuid4()),
        "job_namespace": "minimal-test",
        "job_name": "minimal-job"
    }
    
    lineage_result_minimal = client.analyze_query(
        query=sample_query,
        lineage_config=minimal_config
    )
    print(f"SQL lineage agent result with minimal config: {json.dumps(lineage_result_minimal, indent=8)}")
    print()

  

if __name__ == "__main__":
    main() 