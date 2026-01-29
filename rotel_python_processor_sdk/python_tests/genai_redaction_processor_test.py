"""
Tests for GenAI-specific redaction processor.

These tests demonstrate how to configure and use the processor
for different redaction scenarios.
"""

import json
import pytest
from processors.genai_redaction_processor import (
    GenAIRedactionConfig,
    GenAIRedactionProcessor,
    RedactionStats,
    create_pii_redactor,
    create_minimal_redactor,
)


class TestGenAIRedactionConfig:
    """Tests for configuration validation."""
    
    def test_default_config(self):
        config = GenAIRedactionConfig()
        assert config.replacement == "[REDACTED]"
        assert config.hash_function is None
        assert config.summary == "info"
        assert config.redact_system_instructions is True
        assert config.redact_input_messages is True
        assert config.redact_output_messages is True
    
    def test_invalid_hash_function(self):
        with pytest.raises(ValueError, match="not available"):
            GenAIRedactionConfig(hash_function="not_a_hash")
    
    def test_invalid_summary(self):
        with pytest.raises(ValueError, match="must be"):
            GenAIRedactionConfig(summary="verbose")


class TestTextRedaction:
    """Tests for content pattern redaction."""
    
    def test_email_redaction(self):
        config = GenAIRedactionConfig(
            content_patterns=[
                r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            ]
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        result = processor._redact_text(
            "Contact me at john.doe@example.com for more info",
            stats
        )
        
        assert "john.doe@example.com" not in result
        assert "[REDACTED]" in result
        assert stats.patterns_matched == 1
    
    def test_ssn_redaction(self):
        config = GenAIRedactionConfig(
            content_patterns=[r'\b\d{3}-\d{2}-\d{4}\b']
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        result = processor._redact_text(
            "My SSN is 123-45-6789",
            stats
        )
        
        assert "123-45-6789" not in result
        assert "[REDACTED]" in result
    
    def test_multiple_patterns(self):
        config = GenAIRedactionConfig(
            content_patterns=[
                r'\b\d{3}-\d{2}-\d{4}\b',  # SSN
                r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',  # Email
            ]
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        result = processor._redact_text(
            "SSN: 123-45-6789, Email: test@example.com",
            stats
        )
        
        assert "123-45-6789" not in result
        assert "test@example.com" not in result
        assert stats.patterns_matched == 2
    
    def test_hash_redaction(self):
        config = GenAIRedactionConfig(
            content_patterns=[r'secret_\w+'],
            hash_function="sha256"
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        result = processor._redact_text(
            "The password is secret_abc123",
            stats
        )
        
        assert "secret_abc123" not in result
        assert "[REDACTED]" not in result  # Using hash instead
        assert len(result.split()[-1]) == 64  # SHA256 hex length


class TestToolFieldRedaction:
    """Tests for tool-specific field redaction."""
    
    def test_redact_password_field(self):
        config = GenAIRedactionConfig(
            redact_tool_fields=["password", "api_key"]
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        obj = {
            "username": "john",
            "password": "supersecret123",
            "email": "john@example.com"
        }
        
        result = processor._redact_dict_fields(obj, stats)
        
        assert result["username"] == "john"
        assert result["password"] == "[REDACTED]"
        assert result["email"] == "john@example.com"
        assert stats.fields_redacted == 1
    
    def test_redact_nested_fields(self):
        config = GenAIRedactionConfig(
            redact_tool_fields=["secret"]
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        obj = {
            "config": {
                "name": "test",
                "credentials": {
                    "secret": "my_secret_value"
                }
            }
        }
        
        result = processor._redact_dict_fields(obj, stats)
        
        assert result["config"]["name"] == "test"
        assert result["config"]["credentials"]["secret"] == "[REDACTED]"


class TestMessagePartRedaction:
    """Tests for message part redaction."""
    
    def test_text_part_redaction(self):
        config = GenAIRedactionConfig(
            content_patterns=[r'\b\d{3}-\d{2}-\d{4}\b']
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        part = {
            "type": "text",
            "content": "My SSN is 123-45-6789"
        }
        
        result = processor._redact_message_part(part, stats)
        
        assert "123-45-6789" not in result["content"]
    
    def test_tool_call_argument_redaction(self):
        config = GenAIRedactionConfig(
            redact_tool_fields=["password"]
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        part = {
            "type": "tool_call",
            "id": "call_123",
            "name": "login",
            "arguments": {
                "username": "john",
                "password": "secret123"
            }
        }
        
        result = processor._redact_message_part(part, stats)
        
        assert result["arguments"]["username"] == "john"
        assert result["arguments"]["password"] == "[REDACTED]"
    
    def test_sensitive_tool_full_redaction(self):
        config = GenAIRedactionConfig(
            redact_tool_arguments=["authenticate"]
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        part = {
            "type": "tool_call",
            "id": "call_123",
            "name": "authenticate",
            "arguments": {
                "username": "john",
                "password": "secret123",
                "mfa_token": "123456"
            }
        }
        
        result = processor._redact_message_part(part, stats)
        
        # Entire arguments should be redacted
        assert "[REDACTED]" in result["arguments"]
        assert "john" not in str(result["arguments"])
        assert stats.tool_args_redacted == 1
    
    def test_tool_response_redaction(self):
        config = GenAIRedactionConfig(
            content_patterns=[r'token_\w+']
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        part = {
            "type": "tool_call_response",
            "id": "call_123",
            "result": "Your token is token_abc123xyz"
        }
        
        result = processor._redact_message_part(part, stats)
        
        assert "token_abc123xyz" not in result["result"]


class TestFullMessageRedaction:
    """Tests for complete message redaction."""
    
    def test_input_message_redaction(self):
        config = GenAIRedactionConfig(
            content_patterns=[r'\b\d{3}-\d{2}-\d{4}\b'],
            redact_tool_fields=["password"]
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        message = {
            "role": "user",
            "parts": [
                {"type": "text", "content": "My SSN is 123-45-6789"},
                {
                    "type": "tool_call",
                    "id": "call_1",
                    "name": "login",
                    "arguments": {"user": "john", "password": "secret"}
                }
            ]
        }
        
        result = processor._redact_message(message, stats)
        
        assert result["role"] == "user"
        assert "123-45-6789" not in result["parts"][0]["content"]
        assert result["parts"][1]["arguments"]["password"] == "[REDACTED]"
        assert stats.messages_processed == 1


class TestJsonAttributeProcessing:
    """Tests for JSON attribute processing."""
    
    def test_process_json_messages(self):
        config = GenAIRedactionConfig(
            content_patterns=[r'secret_\w+']
        )
        processor = GenAIRedactionProcessor(config)
        stats = RedactionStats()
        
        json_str = json.dumps([
            {
                "role": "user",
                "parts": [
                    {"type": "text", "content": "The code is secret_abc123"}
                ]
            }
        ])
        
        result = processor._process_json_attribute(
            json_str,
            processor._redact_message,
            stats
        )
        
        parsed = json.loads(result)
        assert "secret_abc123" not in parsed[0]["parts"][0]["content"]


class TestConvenienceFactories:
    """Tests for factory functions."""
    
    def test_pii_redactor(self):
        processor = create_pii_redactor()
        stats = RedactionStats()
        
        # Test various PII patterns
        text = """
        Email: john@example.com
        Phone: (555) 123-4567
        SSN: 123-45-6789
        Credit Card: 4111-1111-1111-1111
        IP: 192.168.1.1
        """
        
        result = processor._redact_text(text, stats)
        
        assert "john@example.com" not in result
        assert "123-45-6789" not in result
        assert "4111-1111-1111-1111" not in result
        assert stats.patterns_matched >= 4
    
    def test_pii_redactor_with_hashing(self):
        processor = create_pii_redactor(use_hashing=True)
        stats = RedactionStats()
        
        result = processor._redact_text("test@example.com", stats)
        
        assert "[REDACTED]" not in result
        # Should contain a hash
        assert len(result) > 20
    
    def test_minimal_redactor(self):
        processor = create_minimal_redactor(
            tool_fields=["api_key", "secret"]
        )
        stats = RedactionStats()
        
        obj = {
            "api_key": "sk-12345",
            "name": "test",
            "secret": "my_secret"
        }
        
        result = processor._redact_dict_fields(obj, stats)
        
        assert result["api_key"] == "[REDACTED]"
        assert result["name"] == "test"
        assert result["secret"] == "[REDACTED]"


class TestIntegrationScenarios:
    """Integration tests showing real-world usage."""
    
    def test_llm_conversation_redaction(self):
        """Test redacting a full LLM conversation with PII."""
        processor = create_pii_redactor(
            patterns=[r'patient_id_\w+'],  # Custom pattern
            summary="debug"
        )
        
        # Simulate input messages attribute value
        input_messages = json.dumps([
            {
                "role": "user",
                "parts": [
                    {
                        "type": "text",
                        "content": "Look up patient patient_id_12345, email is john@hospital.com"
                    }
                ]
            },
            {
                "role": "assistant",
                "parts": [
                    {
                        "type": "tool_call",
                        "id": "call_1",
                        "name": "get_patient",
                        "arguments": {"patient_id": "patient_id_12345"}
                    }
                ]
            }
        ])
        
        stats = RedactionStats()
        result = processor._process_json_attribute(
            input_messages,
            processor._redact_message,
            stats
        )
        
        parsed = json.loads(result)
        
        # User message content should be redacted
        user_content = parsed[0]["parts"][0]["content"]
        assert "patient_id_12345" not in user_content
        assert "john@hospital.com" not in user_content
        
        # Tool call arguments should be redacted
        tool_args = parsed[1]["parts"][0]["arguments"]
        assert "patient_id_12345" not in str(tool_args)
        
        assert stats.messages_processed == 2
    
    def test_authentication_tool_redaction(self):
        """Test that authentication-related tools are fully redacted."""
        config = GenAIRedactionConfig(
            redact_tool_arguments=["login", "authenticate", "set_password"],
            summary="info"
        )
        processor = GenAIRedactionProcessor(config)
        
        input_messages = json.dumps([
            {
                "role": "assistant",
                "parts": [
                    {
                        "type": "tool_call",
                        "id": "call_auth",
                        "name": "login",
                        "arguments": {
                            "username": "admin",
                            "password": "super_secret_password",
                            "mfa_code": "123456"
                        }
                    }
                ]
            }
        ])
        
        stats = RedactionStats()
        result = processor._process_json_attribute(
            input_messages,
            processor._redact_message,
            stats
        )
        
        parsed = json.loads(result)
        tool_call = parsed[0]["parts"][0]
        
        # Entire arguments should be redacted
        assert "admin" not in str(tool_call["arguments"])
        assert "super_secret_password" not in str(tool_call["arguments"])
        assert stats.tool_args_redacted == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

