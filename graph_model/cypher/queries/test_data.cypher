// Simple test query to see what data exists
MATCH (n) 
RETURN labels(n) AS labels, count(*) AS count 
ORDER BY count DESC; 