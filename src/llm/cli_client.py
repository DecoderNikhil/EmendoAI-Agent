"""
Claude CLI client for EmendoAI
Uses Claude Code CLI to generate responses (for demo/temporary use)
"""
import subprocess
import logging
from typing import Optional, List, Dict
import shlex

from config import settings

logger = logging.getLogger(__name__)


class CLIClaudeClient:
    """Client for interacting with Claude via CLI"""
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None
    ):
        self.model = model or settings.ANTHROPIC_MODEL
        self.max_tokens = settings.MAX_TOKENS
        logger.info("CLI Claude client initialized")
    
    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None
    ) -> str:
        """
        Generate a response from Claude CLI
        """
        try:
            # Build the full prompt with system prompt
            full_prompt = prompt
            if system_prompt:
                full_prompt = f"{system_prompt}\n\n{prompt}"
            
            escaped_prompt = shlex.quote(full_prompt)
            # Use claude CLI with -p flag for prompt mode
            result = subprocess.run(
                ["wsl", "bash", "-lc", f"claude -p {escaped_prompt}"],
                capture_output=True,
                text=True,
                timeout=120  # 2 minute timeout
            )
            
            if result.returncode != 0:
                logger.error(f"Claude CLI error: {result.stderr}")
                raise Exception(f"Claude CLI failed: {result.stderr}")
            
            return result.stdout.strip()
            
        except subprocess.TimeoutExpired:
            logger.error("Claude CLI timed out")
            raise Exception("Claude CLI timed out after 2 minutes")
        
        except FileNotFoundError:
            logger.error("Claude CLI not found")
            raise Exception("Claude CLI not found. Please install Claude Code.")
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise
    
    def generate_with_history(
        self,
        messages: List[Dict[str, str]],
        system_prompt: Optional[str] = None,
        temperature: float = 0.7
    ) -> str:
        """
        Generate a response with conversation history
        """
        try:
            # Build a conversation prompt from history
            prompt_parts = []
            
            # Add system prompt if exists
            if system_prompt:
                prompt_parts.append(f"System: {system_prompt}")
            
            # Add conversation history
            for msg in messages:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                
                if isinstance(content, str):
                    prompt_parts.append(f"{role.capitalize()}: {content}")
                elif isinstance(content, list):
                    # Handle list content (e.g., from Bedrock format)
                    text_content = " ".join([
                        c.get("text", "") for c in content if isinstance(c, dict)
                    ])
                    prompt_parts.append(f"{role.capitalize()}: {text_content}")
            
            full_prompt = "\n\n".join(prompt_parts)
            
            escaped_prompt = shlex.quote(full_prompt)
            # Use claude CLI
            result = subprocess.run(
                ["wsl", "bash", "-lc", f"claude -p {escaped_prompt}"],
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.returncode != 0:
                logger.error(f"Claude CLI error: {result.stderr}")
                raise Exception(f"Claude CLI failed: {result.stderr}")
            
            return result.stdout.strip()
            
        except subprocess.TimeoutExpired:
            logger.error("Claude CLI timed out")
            raise Exception("Claude CLI timed out after 2 minutes")
        
        except FileNotFoundError:
            logger.error("Claude CLI not found")
            raise Exception("Claude CLI not found. Please install Claude Code.")
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise


def get_cli_client() -> CLIClaudeClient:
    """Get or create a CLI client instance"""
    return CLIClaudeClient()
