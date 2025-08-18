import os
import logging
from openai import AsyncOpenAI
from agents import OpenAIChatCompletionsModel
from dotenv import load_dotenv

# Get logger for this module
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(override=True)

# API Keys
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GROK_API_KEY = os.getenv("GROK_API_KEY")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# Base URLs
DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
GROK_BASE_URL = "https://api.x.ai/v1"
GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai/"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# Initialize API clients
openrouter_client = AsyncOpenAI(base_url=OPENROUTER_BASE_URL, api_key=OPENROUTER_API_KEY)
deepseek_client = AsyncOpenAI(base_url=DEEPSEEK_BASE_URL, api_key=DEEPSEEK_API_KEY)
grok_client = AsyncOpenAI(base_url=GROK_BASE_URL, api_key=GROK_API_KEY)
gemini_client = AsyncOpenAI(base_url=GEMINI_BASE_URL, api_key=GOOGLE_API_KEY)


def get_model(model_name: str):
    """
    Get the appropriate model based on the model name.
    
    Args:
        model_name (str): The name of the model to use
        
    Returns:
        OpenAIChatCompletionsModel or str: The model instance or model name
    """
    if "/" in model_name:
        return OpenAIChatCompletionsModel(model=model_name, openai_client=openrouter_client)
    elif "deepseek" in model_name:
        return OpenAIChatCompletionsModel(model=model_name, openai_client=deepseek_client)
    elif "grok" in model_name:
        return OpenAIChatCompletionsModel(model=model_name, openai_client=grok_client)
    elif "gemini" in model_name:
        return OpenAIChatCompletionsModel(model=model_name, openai_client=gemini_client)
    else:
        return model_name


def get_api_clients():
    """
    Get all API clients for external use if needed.
    
    Returns:
        dict: Dictionary containing all API clients
    """
    return {
        'openrouter': openrouter_client,
        'deepseek': deepseek_client,
        'grok': grok_client,
        'gemini': gemini_client
    }


def validate_api_keys():
    """
    Validate that required API keys are available.
    
    Returns:
        dict: Dictionary with validation results for each API
    """
    validation_results = {
        'openrouter': bool(OPENROUTER_API_KEY),
        'deepseek': bool(DEEPSEEK_API_KEY),
        'grok': bool(GROK_API_KEY),
        'gemini': bool(GOOGLE_API_KEY)
    }
    
    missing_keys = [key for key, available in validation_results.items() if not available]
    if missing_keys:
        logger.warning(f"Missing API keys for: {', '.join(missing_keys)}")
    
    return validation_results
