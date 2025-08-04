import requests
import json
import asyncio
from typing import Dict, Any

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
    
    def analyze_query(self, query: str, model_name: str = "gpt-4o-mini", agent_name: str = "java-lineage-agent") -> Dict[str, Any]:
        """
        Analyze a single Java query using the java_lineage_agent plugin
        
        Args:
            query: Java query to analyze
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
        import java.nio.file.*;
        import java.util.stream.*;

        public class CustomerDataProcessor {
            public static void main(String[] args) throws IOException {
                // Step 1: Load input CSV
                List<String> lines = Files.readAllLines(Paths.get("/data/input/customers.csv"));
                List<Customer> customers = new ArrayList<>();
                
                // Skip header
                for (int i = 1; i < lines.size(); i++) {
                    String[] parts = lines.get(i).split(",");
                    if (parts.length >= 4) {
                        Customer customer = new Customer();
                        customer.setFirstName(parts[0].trim());
                        customer.setLastName(parts[1].trim());
                        customer.setEmail(parts[2].trim());
                        customer.setBirthdate(parts[3].trim());
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

                // Step 7: Write result to new CSV
                try (PrintWriter writer = new PrintWriter(new FileWriter("/data/output/cleaned_customers.csv"))) {
                    writer.println("first_name,last_name,email,birthdate,full_name,age,age_group");
                    for (Customer customer : filteredCustomers) {
                        writer.printf("%s,%s,%s,%s,%s,%d,%s%n",
                            customer.getFirstName(),
                            customer.getLastName(),
                            customer.getEmail(),
                            customer.getBirthdate(),
                            customer.getFullName(),
                            customer.getAge(),
                            customer.getAgeGroup());
                    }
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

    # Run Java lineage agent directly
    print("Running Java lineage agent directly...")
    lineage_result = client.analyze_query(sample_query)
    print(f"Java lineage agent result: {json.dumps(lineage_result, indent=8)}")
    print()

  

if __name__ == "__main__":
    main() 