import importlib.metadata
from typing import Dict, Any, Optional, Type, Callable
import logging

logger = logging.getLogger(__name__)


class PluginManager:
    """Manages plugin discovery and loading for the AgentFramework"""
    
    def __init__(self):
        self.plugins: Dict[str, Dict[str, Any]] = {}
        self.agent_factories: Dict[str, Callable] = {}
        self._load_plugins()
    
    def _load_plugins(self):
        """Load all available plugins using entry points"""
        try:
            # Load plugins from the 'lineagent.algorithm.plugins' entry point group
            for entry_point in importlib.metadata.entry_points(group='lineagent.algorithm.plugins'):
                try:
                    plugin_info = entry_point.load()
                    if callable(plugin_info):
                        # If it's a function, assume it returns plugin info
                        plugin_data = plugin_info()
                    else:
                        # If it's already a dict/object
                        plugin_data = plugin_info
                    
                    plugin_name = plugin_data.get('name', entry_point.name)
                    self.plugins[plugin_name] = plugin_data
                    
                    # Store the factory function if available
                    if 'factory_function' in plugin_data:
                        self.agent_factories[plugin_name] = plugin_data['factory_function']
                    
                    logger.info(f"Loaded plugin: {plugin_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to load plugin {entry_point.name}: {e}")
                    
        except Exception as e:
            logger.error(f"Error loading plugins: {e}")
    
    def get_plugin(self, plugin_name: str) -> Optional[Dict[str, Any]]:
        """Get plugin information by name"""
        return self.plugins.get(plugin_name)
    
    def list_plugins(self) -> Dict[str, Dict[str, Any]]:
        """List all available plugins"""
        return self.plugins.copy()
    
    def create_agent(self, plugin_name: str, **kwargs) -> Any:
        """Create an agent instance using the plugin's factory function"""
        if plugin_name not in self.agent_factories:
            raise ValueError(f"Plugin '{plugin_name}' not found or has no factory function")
        
        factory = self.agent_factories[plugin_name]
        return factory(**kwargs)
    
    def get_supported_operations(self) -> Dict[str, list]:
        """Get all supported operations from all plugins"""
        operations = {}
        for plugin_name, plugin_info in self.plugins.items():
            supported_ops = plugin_info.get('supported_operations', [])
            for op in supported_ops:
                if op not in operations:
                    operations[op] = []
                operations[op].append(plugin_name)
        return operations
    
    def get_plugins_for_operation(self, operation: str) -> list:
        """Get all plugins that support a specific operation"""
        supported_ops = self.get_supported_operations()
        return supported_ops.get(operation, [])


# Global plugin manager instance
plugin_manager = PluginManager() 