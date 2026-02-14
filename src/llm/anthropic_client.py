"""
Anthropic Claude client for EmendoAI
"""
from typing import Dict, Any, Optional, List
import anthropic
import logging

from config import settings

logger = logging.getLogger(__name__)


class ClaudeClient:
    """Client for interacting with Anthropic Claude API"""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.ANTHROPIC_API_KEY
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY is required")
        
        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = settings.ANTHROPIC_MODEL
        self.max_tokens = settings.MAX_TOKENS
    
    def generate(
        self, 
        prompt: str, 
        system_prompt: Optional[str] = None,
        temperature: float = 0.7
    ) -> str:
        """Generate a response from Claude"""
        try:
            messages = [{"role": "user", "content": prompt}]
            
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                messages=messages,
                system=system_prompt or "",
                temperature=temperature
            )
            
            # Extract text from response
            if response.content:
                return response.content[0].text
            
            return ""
            
        except anthropic.APIError as e:
            logger.error(f"Anthropic API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def generate_with_history(
        self,
        messages: List[Dict[str, str]],
        system_prompt: Optional[str] = None,
        temperature: float = 0.7
    ) -> str:
        """Generate a response with conversation history"""
        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                messages=messages,
                system=system_prompt or "",
                temperature=temperature
            )
            
            if response.content:
                return response.content[0].text
            
            return ""
            
        except anthropic.APIError as e:
            logger.error(f"Anthropic API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise


# Factory function
def get_claude_client() -> ClaudeClient:
    """Get or create a Claude client instance"""
    return ClaudeClient()
