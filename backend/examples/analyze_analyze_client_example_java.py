import requests
import json
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timezone
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
                     save_to_neo4j: bool = True, event_ingestion_request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Analyze a single Java query using the java_lineage_agent plugin
        
        Args:
            query: Java query to analyze
            model_name: Model to use for analysis
            agent_name: Name of the agent
            save_to_db: Whether to save results to database
            save_to_neo4j: Whether to save lineage data to Neo4j
            event_ingestion_request: Optional EventIngestionRequest configuration
            
        Returns:
            Analysis results
        """
        # If no event_ingestion_request provided, create a basic one with the query
        if event_ingestion_request is None:
            event_ingestion_request = {
                "event_type": "START",
                "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "run": {
                    "run_id": str(uuid.uuid4()),
                    "facets": {}
                },
                "job": {
                    "namespace": "default",
                    "name": "java-query-analysis",
                    "facets": {
                        "source_code": {
                            "language": "java",
                            "source_code": query
                        }
                    }
                },
                "inputs": [],
                "outputs": []
            }
        else:
            # Ensure the query is set in the event_ingestion_request
            if "job" not in event_ingestion_request:
                event_ingestion_request["job"] = {}
            if "facets" not in event_ingestion_request["job"]:
                event_ingestion_request["job"]["facets"] = {}
            if "source_code" not in event_ingestion_request["job"]["facets"]:
                event_ingestion_request["job"]["facets"]["source_code"] = {}
            
            # Set the query in the source_code
            event_ingestion_request["job"]["facets"]["source_code"]["source_code"] = query
            event_ingestion_request["job"]["facets"]["source_code"]["language"] = "java"
        
        payload = {
            "model_name": model_name,
            "agent_name": agent_name,
            "save_to_db": save_to_db,
            "save_to_neo4j": save_to_neo4j,
            "event_ingestion_request": event_ingestion_request
        }
        
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

    # Create the event ingestion request with proper structure
    event_ingestion_request = {
        "event_type": "START",
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "run": {
            "run_id": str(uuid.uuid4()),
            "facets": {
                "parent": {
                    "job": {
                        "namespace": "test-namespace",
                        "name": "test-java-job",
                        "facets": {
                            "source_code_location": {
                                "type": "file",
                                "url": "https://github.com/your-repo/your-java-job/blob/main/CustomerDataProcessor.java",
                                "repo_url": "https://github.com/your-repo/your-java-job",
                                "path": "/path/to/CustomerDataProcessor.java",
                                "version": "1.0.0",
                                "branch": "main"
                            },
                            "source_code": {
                                "language": "java",
                                "source_code": sample_query
                            },
                            "job_type": {
                                "processing_type": "BATCH",
                                "integration": "JAVA",
                                "job_type": "APPLICATION"
                            },
                            "documentation": {
                                "description": "This is a test Java application",
                                "content_type": "text/plain"
                            },
                            "ownership": {
                                "owners": [
                                    {"name": "John Doe", "type": "INDIVIDUAL"}
                                ]
                            },
                            "environment_variables": [
                                {"name": "JAVA_HOME", "value": "/usr/lib/jvm/java-11"},
                                {"name": "CLASSPATH", "value": "/app/lib/*"},
                                {"name": "DB_URL", "value": "jdbc:mysql://localhost:3306/customer_db"}
                            ]
                        }
                    }
                }
            }
        },
        "job": {
            "namespace": "test-namespace",
            "name": "test-java-job",
            "facets": {
                "source_code_location": {
                    "type": "file",
                    "url": "https://github.com/your-repo/your-java-job/blob/main/CustomerDataProcessor.java",
                    "repo_url": "https://github.com/your-repo/your-java-job",
                    "path": "/path/to/CustomerDataProcessor.java",
                    "version": "1.0.0",
                    "branch": "main"
                },
                "source_code": {
                    "language": "java",
                    "source_code": sample_query
                },
                "job_type": {
                    "processing_type": "BATCH",
                    "integration": "JAVA",
                    "job_type": "APPLICATION"
                },
                "documentation": {
                    "description": "This is a test Java application",
                    "content_type": "text/plain"
                },
                "ownership": {
                    "owners": [
                        {"name": "John Doe", "type": "INDIVIDUAL"}
                    ]
                },
                "environment_variables": [
                    {"name": "JAVA_HOME", "value": "/usr/lib/jvm/java-11"},
                    {"name": "CLASSPATH", "value": "/app/lib/*"},
                    {"name": "DB_URL", "value": "jdbc:mysql://localhost:3306/customer_db"}
                ]
            }
        },
        "inputs": [
            {
                "namespace": "customer_db",
                "name": "customer_1",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "first_name", "type": "VARCHAR", "description": "First name", "version_id": "1.0"},
                            {"name": "last_name", "type": "VARCHAR", "description": "Last name", "version_id": "1.0"},
                            {"name": "email", "type": "VARCHAR", "description": "Customer email", "version_id": "1.0"},
                            {"name": "birthdate", "type": "DATE", "description": "Birth date", "version_id": "1.0"}
                        ]
                    },
                    "tags": [
                        {"key": "input", "value": "test", "source": "manual"}
                    ],
                    "ownership": {
                        "owners": [
                            {"name": "John Doe", "type": "INDIVIDUAL"}
                        ]
                    },
                    "input_statistics": {
                        "row_count": 1000,
                        "file_count": 1,
                        "size": 50000
                    }
                }
            }
        ],
        "outputs": [
            {
                "namespace": "customer_db",
                "name": "customer_2",
                "facets": {
                    "schema": {
                        "fields": [
                            {"name": "first_name", "type": "VARCHAR", "description": "First name", "version_id": "1.0"},
                            {"name": "last_name", "type": "VARCHAR", "description": "Last name", "version_id": "1.0"},
                            {"name": "email", "type": "VARCHAR", "description": "Customer email", "version_id": "1.0"},
                            {"name": "birthdate", "type": "DATE", "description": "Birth date", "version_id": "1.0"},
                            {"name": "full_name", "type": "VARCHAR", "description": "Full name", "version_id": "1.0"},
                            {"name": "age", "type": "INTEGER", "description": "Calculated age", "version_id": "1.0"},
                            {"name": "age_group", "type": "VARCHAR", "description": "Age group category", "version_id": "1.0"}
                        ]
                    },
                    "tags": [
                        {"key": "output", "value": "test", "source": "manual"}
                    ],
                    "ownership": {
                        "owners": [
                            {"name": "John Doe", "type": "INDIVIDUAL"}
                        ]
                    },
                    "output_statistics": {
                        "row_count": 950,
                        "file_count": 1,
                        "size": 45000
                    }
                }
            }
        ]
    }
    
    lineage_result_proper = client.analyze_query(
        query=sample_query,
        event_ingestion_request=event_ingestion_request
    )
    print(f"Java lineage agent result with proper EventIngestionRequest: {json.dumps(lineage_result_proper, indent=2)}")
    print()

if __name__ == "__main__":
    main() 