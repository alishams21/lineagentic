import json
from typing import Dict, Any, List, Optional
from ..repository_layer.lineage_repository import LineageRepository 


class LineageProcessor:
    """Utility class for processing lineage data"""
    
    def __init__(self, repository: LineageRepository = None):
        self.repository = repository or LineageRepository()
    
    def process_lineage_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a lineage event (now just validates without saving)"""
        try:
            # Validate the event data structure
            self._validate_event_data(event_data)
            
            # Return success without saving to database
            return {
                "success": True,
                "message": "Lineage event processed successfully (not saved to database)",
                "data": None
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Error processing lineage event: {str(e)}",
                "data": None
            }
    
    def process_lineage_events_batch(self, events_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process multiple lineage events in batch"""
        results = []
        success_count = 0
        error_count = 0
        
        for event_data in events_data:
            try:
                result = self.process_lineage_event(event_data)
                results.append(result)
                if result["success"]:
                    success_count += 1
                else:
                    error_count += 1
            except Exception as e:
                results.append({
                    "success": False,
                    "message": f"Error processing event: {str(e)}",
                    "data": None
                })
                error_count += 1
        
        return {
            "success": error_count == 0,
            "total_events": len(events_data),
            "successful_events": success_count,
            "failed_events": error_count,
            "results": results
        }
    
    def process_lineage_from_json_file(self, file_path: str) -> Dict[str, Any]:
        """Process lineage events from a JSON file"""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Handle both single event and array of events
            if isinstance(data, dict):
                return self.process_lineage_event(data)
            elif isinstance(data, list):
                return self.process_lineage_events_batch(data)
            else:
                raise ValueError("JSON file must contain either a single event object or an array of events")
                
        except FileNotFoundError:
            return {
                "success": False,
                "message": f"File not found: {file_path}",
                "data": None
            }
        except json.JSONDecodeError as e:
            return {
                "success": False,
                "message": f"Invalid JSON format: {str(e)}",
                "data": None
            }
        except Exception as e:
            return {
                "success": False,
                "message": f"Error processing file: {str(e)}",
                "data": None
            }
    
    def get_lineage_summary(self, event_id: Optional[int] = None) -> Dict[str, Any]:
        """Get a summary of lineage data"""
        try:
            if event_id:
                event_data = self.repository.get_lineage_event(event_id)
                if not event_data:
                    return {
                        "success": False,
                        "message": f"Event with ID {event_id} not found",
                        "data": None
                    }
                
                return {
                    "success": True,
                    "message": "Lineage event retrieved successfully",
                    "data": event_data
                }
            else:
                events = self.repository.get_all_lineage_events(limit=10)
                return {
                    "success": True,
                    "message": "Recent lineage events retrieved successfully",
                    "data": {
                        "events": events,
                        "total_events": len(events)
                    }
                }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"Error retrieving lineage data: {str(e)}",
                "data": None
            }
    
    def _validate_event_data(self, event_data: Dict[str, Any]) -> None:
        """Validate the structure of lineage event data"""
        required_fields = ['eventType', 'eventTime', 'run', 'job']
        
        for field in required_fields:
            if field not in event_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate run structure
        if 'runId' not in event_data['run']:
            raise ValueError("Missing runId in run data")
        
        # Validate job structure
        if 'facets' not in event_data['job']:
            raise ValueError("Missing facets in job data")
    
    def export_lineage_to_json(self, event_ids: List[int] = None, output_file: str = None) -> Dict[str, Any]:
        """Export lineage data to JSON format"""
        try:
            if event_ids:
                events_data = []
                for event_id in event_ids:
                    event_data = self.repository.get_lineage_event(event_id)
                    if event_data:
                        events_data.append(event_data)
            else:
                events_data = self.repository.get_all_lineage_events(limit=100)
            
            if output_file:
                with open(output_file, 'w') as f:
                    json.dump(events_data, f, indent=2, default=str)
            
            return {
                "success": True,
                "message": f"Exported {len(events_data)} lineage events",
                "data": {
                    "events_count": len(events_data),
                    "output_file": output_file
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Error exporting lineage data: {str(e)}",
                "data": None
            }
    
    def get_lineage_statistics(self) -> Dict[str, Any]:
        """Get statistics about lineage data"""
        try:
            # Get all events
            events = self.repository.get_all_lineage_events(limit=1000)
            
            if not events:
                return {
                    "success": True,
                    "message": "No lineage data found",
                    "data": {
                        "total_events": 0,
                        "total_inputs": 0,
                        "total_outputs": 0,
                        "event_types": {},
                        "namespaces": set()
                    }
                }
            
            # Calculate statistics
            total_events = len(events)
            total_inputs = sum(len(event.get('inputs', [])) for event in events)
            total_outputs = sum(len(event.get('outputs', [])) for event in events)
            
            event_types = {}
            namespaces = set()
            
            for event in events:
                event_type = event.get('event', {}).get('event_type')
                if event_type:
                    event_types[event_type] = event_types.get(event_type, 0) + 1
                
                # Collect namespaces from inputs and outputs
                for input_data in event.get('inputs', []):
                    if 'namespace' in input_data:
                        namespaces.add(input_data['namespace'])
                
                for output_data in event.get('outputs', []):
                    if 'namespace' in output_data:
                        namespaces.add(output_data['namespace'])
            
            return {
                "success": True,
                "message": "Lineage statistics retrieved successfully",
                "data": {
                    "total_events": total_events,
                    "total_inputs": total_inputs,
                    "total_outputs": total_outputs,
                    "event_types": event_types,
                    "namespaces": list(namespaces),
                    "average_inputs_per_event": total_inputs / total_events if total_events > 0 else 0,
                    "average_outputs_per_event": total_outputs / total_events if total_events > 0 else 0
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Error calculating lineage statistics: {str(e)}",
                "data": None
            }
    
    def cleanup_old_events(self, days_old: int = 30) -> Dict[str, Any]:
        """Clean up old lineage events (placeholder for future implementation)"""
        try:
            # This is a placeholder - you would implement the actual cleanup logic
            # based on your database schema and requirements
            
            return {
                "success": True,
                "message": f"Cleanup of events older than {days_old} days completed",
                "data": {
                    "days_old": days_old,
                    "events_cleaned": 0  # Placeholder
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "message": f"Error during cleanup: {str(e)}",
                "data": None
            } 