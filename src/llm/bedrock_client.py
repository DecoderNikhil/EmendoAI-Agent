"""
Amazon Bedrock client for EmendoAI
Uses boto3 to invoke Claude models via AWS Bedrock
"""
import os
import json
import logging
from typing import Optional, List, Dict
import boto3
from botocore.exceptions import ClientError, BotoCoreError

from config import settings

logger = logging.getLogger(__name__)


class BedrockClient:
    """Client for interacting with Claude via Amazon Bedrock"""
    
    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None
    ):
        self.aws_access_key_id = aws_access_key_id or settings.AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = aws_secret_access_key or settings.AWS_SECRET_ACCESS_KEY
        self.region_name = region_name or settings.AWS_REGION
        self.model_id = settings.ANTHROPIC_MODEL
        self.max_tokens = settings.MAX_TOKENS
        
        # Validate credentials
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise ValueError("AWS credentials are required (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)")
        
        # Create Bedrock Runtime client
        self.client = boto3.client(
            "bedrock-runtime",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )
        
        logger.info(f"Bedrock client initialized with model: {self.model_id}")
    
    def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: Optional[int] = None
    ) -> str:
        """
        Generate a response from Claude via Bedrock
        """
        try:
            # Build messages format for Bedrock Claude
            messages = [{"role": "user", "content": [{"text": prompt}]}]
            
            # Build request body
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens or self.max_tokens,
                "temperature": temperature,
                "messages": messages
            }
            
            # Add system prompt if provided
            if system_prompt:
                request_body["system"] = [{"text": system_prompt}]
            
            # Invoke model
            response = self.client.invoke_model(
                modelId=self.model_id,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(request_body)
            )
            
            # Parse response
            response_body = json.loads(response["body"].read().decode("utf-8"))
            
            # Extract text from response
            if response_body.get("content"):
                for content_block in response_body["content"]:
                    if content_block.get("type") == "text":
                        return content_block.get("text", "")
            
            return ""
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            
            # Don't retry on auth errors
            if error_code in ["AccessDeniedException", "UnauthorizedException", "InvalidSignatureException"]:
                logger.error(f"Bedrock auth error: {e}")
                raise Exception(f"AWS authentication failed: {error_code}")
            
            # Retry on throttling
            if error_code in ["ThrottlingException", "ProvisionedThroughputExceededException"]:
                logger.warning(f"Bedrock throttling error: {e}")
                raise Exception("Rate limited by AWS Bedrock, please try again later")
            
            logger.error(f"Bedrock client error: {e}")
            raise
        
        except BotoCoreError as e:
            logger.error(f"Bedrock connection error: {e}")
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
        """
        Generate a response with conversation history
        """
        try:
            # Convert messages to Bedrock format
            bedrock_messages = []
            for msg in messages:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                
                # Handle both string and dict content
                if isinstance(content, str):
                    bedrock_messages.append({
                        "role": role,
                        "content": [{"text": content}]
                    })
                elif isinstance(content, list):
                    bedrock_messages.append({
                        "role": role,
                        "content": content
                    })
            
            # Build request body
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": self.max_tokens,
                "temperature": temperature,
                "messages": bedrock_messages
            }
            
            # Add system prompt if provided
            if system_prompt:
                request_body["system"] = [{"text": system_prompt}]
            
            # Invoke model
            response = self.client.invoke_model(
                modelId=self.model_id,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(request_body)
            )
            
            # Parse response
            response_body = json.loads(response["body"].read().decode("utf-8"))
            
            # Extract text from response
            if response_body.get("content"):
                for content_block in response_body["content"]:
                    if content_block.get("type") == "text":
                        return content_block.get("text", "")
            
            return ""
            
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            
            if error_code in ["AccessDeniedException", "UnauthorizedException"]:
                logger.error(f"Bedrock auth error: {e}")
                raise Exception(f"AWS authentication failed: {error_code}")
            
            if error_code in ["ThrottlingException", "ProvisionedThroughputExceededException"]:
                logger.warning(f"Bedrock throttling error: {e}")
                raise Exception("Rate limited by AWS Bedrock")
            
            logger.error(f"Bedrock client error: {e}")
            raise
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise


def get_bedrock_client() -> BedrockClient:
    """Get or create a Bedrock client instance"""
    return BedrockClient()
