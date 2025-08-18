import importlib.metadata
from typing import Dict, Any, Optional, Type, Callable
import logging

logger = logging.getLogger(__name__)


class AgentManager:
    """Manages plugin discovery and loading for the FrameworkAgent"""
    
    def __init__(self):
        self.agents: Dict[str, Dict[str, Any]] = {}
        self.agent_factories: Dict[str, Callable] = {}
        self._load_plugins()
    
    def _load_plugins(self):
        """Load all available agents plugins using entry points"""
        try:
            # Load plugins from the 'lineagentic.lf_algorithm.plugins' entry point group
            for entry_point in importlib.metadata.entry_points(group='lineagentic.lf_algorithm.plugins'):
                try:
                    agent_info = entry_point.load()
                    if callable(agent_info):
                        # If it's a function, assume it returns plugin info
                        agent_data = agent_info()
                    else:
                        # If it's already a dict/object
                        agent_data = agent_info
                    
                    agent_name = agent_data.get('name', entry_point.name)
                    self.agents[agent_name] = agent_data
                    
                    # Store the factory function if available
                    if 'factory_function' in agent_data:
                        self.agent_factories[agent_name] = agent_data['factory_function']
                    
                    logger.info(f"Loaded plugin: {agent_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to load plugin {entry_point.name}: {e}")
                    
        except Exception as e:
            logger.error(f"Error loading plugins: {e}")
    
    def get_agent(self, agent_name: str) -> Optional[Dict[str, Any]]:
        """Get agent information by name"""
        return self.agents.get(agent_name)
    
    def list_agents(self) -> Dict[str, Dict[str, Any]]:
        """List all available agents"""
        return self.agents.copy()
    
    def create_agent(self, agent_name: str, **kwargs) -> Any:
        """Create an agent instance using the agent's factory function"""
        if agent_name not in self.agent_factories:
            raise ValueError(f"Agent '{agent_name}' not found or has no factory function")
        
        factory = self.agent_factories[agent_name]
        return factory(agent_name=agent_name, **kwargs)
    
    def get_supported_operations(self) -> Dict[str, list]:
        """Get all supported operations from all agents"""
        operations = {}
        for agent_name, agent_info in self.agents.items():
            supported_ops = agent_info.get('supported_operations', [])
            for op in supported_ops:
                if op not in operations:
                    operations[op] = []
                operations[op].append(agent_name)
        return operations
    
    def get_agents_for_operation(self, operation: str) -> list:
        """Get all agents that support a specific operation"""
        supported_ops = self.get_supported_operations()
        return supported_ops.get(operation, [])


# Global agent manager instance
agent_manager = AgentManager() 