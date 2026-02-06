"""
GenAI-specific redaction processor for PII/sensitive content in LLM message structures.

This processor targets GenAI semantic convention attributes:
- gen_ai.input.messages  
- gen_ai.output.messages

It parses the message arrays and applies redaction rules to:
- Text content in messages
- Tool call arguments
- Tool response results

Example configuration:

    config = GenAIRedactionConfig(
        # Regex patterns to redact in message content
        content_patterns=[
            r'\\b\\d{3}-\\d{2}-\\d{4}\\b',  # SSN
            r'\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b',  # Email
        ],
        
        # Specific fields to always redact in tool calls
        redact_tool_fields=["password", "api_key", "secret", "token"],
        
        # Tool names whose arguments should be fully redacted
        redact_tool_arguments=["login", "authenticate", "set_password"],
        
        # Replacement text for redacted content
        replacement="[REDACTED]",
        
        # Or use hashing
        hash_function="sha256",
        
        # Add summary attributes: "silent", "info", or "debug"
        summary="info",
    )

    processor = GenAIRedactionProcessor(config)
    processor.process_spans(resource_spans)
"""

import hashlib
import json
import re
from typing import List, Optional, Set, Dict, Any, Tuple

from rotel_sdk.open_telemetry.common.v1 import KeyValue, AnyValue
from rotel_sdk.open_telemetry.trace.v1 import ResourceSpans


# GenAI semantic convention attribute keys
GENAI_INPUT_MESSAGES = "gen_ai.input.messages"
GENAI_OUTPUT_MESSAGES = "gen_ai.output.messages"


class GenAIRedactionConfig:
    """Configuration for GenAI-specific content redaction."""
    
    def __init__(
        self,
        # Patterns to redact in text content
        content_patterns: List[str] = None,
        
        # Field names in tool calls/responses to always redact
        redact_tool_fields: List[str] = None,
        
        # Tool names whose entire arguments should be redacted
        redact_tool_arguments: List[str] = None,
        
        # Replacement text (used if hash_function is None)
        replacement: str = "[REDACTED]",
        
        # Hash function for redaction (None = use replacement text)
        hash_function: Optional[str] = None,
        
        # Summary level: "silent", "info", "debug"
        summary: str = "info",
        
        # Whether to redact input messages
        redact_input_messages: bool = True,
        
        # Whether to redact output messages
        redact_output_messages: bool = True,
    ):
        self.content_patterns = [
            re.compile(p, re.IGNORECASE) for p in (content_patterns or [])
        ]
        self.redact_tool_fields = set(f.lower() for f in (redact_tool_fields or []))
        self.redact_tool_arguments = set(t.lower() for t in (redact_tool_arguments or []))
        self.replacement = replacement
        
        if hash_function and hash_function not in hashlib.algorithms_available:
            raise ValueError(
                f"Hash function '{hash_function}' not available. "
                f"Available: {hashlib.algorithms_available}"
            )
        self.hash_function = hash_function
        
        if summary not in ("silent", "info", "debug"):
            raise ValueError(f"Summary must be 'silent', 'info', or 'debug', got '{summary}'")
        self.summary = summary
        
        self.redact_input_messages = redact_input_messages
        self.redact_output_messages = redact_output_messages


class RedactionStats:
    """Tracks redaction statistics for summary attributes."""
    
    def __init__(self):
        self.patterns_matched: int = 0
        self.fields_redacted: int = 0
        self.tool_args_redacted: int = 0
        self.messages_processed: int = 0
    
    def to_dict(self) -> Dict[str, int]:
        return {
            "patterns_matched": self.patterns_matched,
            "fields_redacted": self.fields_redacted,
            "tool_args_redacted": self.tool_args_redacted,
            "messages_processed": self.messages_processed,
        }


class GenAIRedactionProcessor:
    """
    Processor that applies PII redaction specifically to GenAI message structures.
    
    This operates on the raw span attributes before they are transformed into
    the GenAI-optimized schema, allowing redaction at the OTEL level.
    """
    
    def __init__(self, config: GenAIRedactionConfig):
        self.config = config
    
    def _get_redacted_value(self, original: str) -> str:
        """Get the redacted replacement for a value."""
        if self.config.hash_function:
            hasher = hashlib.new(self.config.hash_function)
            hasher.update(original.encode('utf-8'))
            return hasher.hexdigest()
        return self.config.replacement
    
    def _redact_text(self, text: str, stats: RedactionStats) -> str:
        """Apply content pattern redaction to text."""
        result = text
        for pattern in self.config.content_patterns:
            matches = pattern.findall(result)
            if matches:
                stats.patterns_matched += len(matches)
                result = pattern.sub(
                    lambda m: self._get_redacted_value(m.group(0)),
                    result
                )
        return result
    
    def _redact_dict_fields(
        self, 
        obj: Dict[str, Any], 
        stats: RedactionStats,
        parent_key: str = ""
    ) -> Dict[str, Any]:
        """Recursively redact sensitive fields in a dictionary."""
        result = {}
        for key, value in obj.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            key_lower = key.lower()
            
            if key_lower in self.config.redact_tool_fields:
                # Redact this field entirely
                stats.fields_redacted += 1
                result[key] = self._get_redacted_value(str(value))
            elif isinstance(value, str):
                # Apply content patterns
                result[key] = self._redact_text(value, stats)
            elif isinstance(value, dict):
                result[key] = self._redact_dict_fields(value, stats, full_key)
            elif isinstance(value, list):
                result[key] = self._redact_list(value, stats)
            else:
                result[key] = value
        
        return result
    
    def _redact_list(self, items: List[Any], stats: RedactionStats) -> List[Any]:
        """Recursively redact items in a list."""
        result = []
        for item in items:
            if isinstance(item, str):
                result.append(self._redact_text(item, stats))
            elif isinstance(item, dict):
                result.append(self._redact_dict_fields(item, stats))
            elif isinstance(item, list):
                result.append(self._redact_list(item, stats))
            else:
                result.append(item)
        return result
    
    def _redact_message_part(
        self, 
        part: Dict[str, Any], 
        stats: RedactionStats
    ) -> Dict[str, Any]:
        """Redact a single message part based on its type."""
        part_type = part.get("type", "")
        result = dict(part)
        
        if part_type == "text":
            # Redact text content
            if "content" in result:
                result["content"] = self._redact_text(result["content"], stats)
        
        elif part_type == "tool_call":
            tool_name = result.get("name", "").lower()
            
            if tool_name in self.config.redact_tool_arguments:
                # Redact entire arguments for sensitive tools
                stats.tool_args_redacted += 1
                result["arguments"] = {"[REDACTED]": "Tool arguments redacted"}
            elif "arguments" in result and isinstance(result["arguments"], dict):
                # Redact specific fields in arguments
                result["arguments"] = self._redact_dict_fields(
                    result["arguments"], stats
                )
        
        elif part_type == "tool_call_response":
            # Redact tool response result
            if "result" in result:
                if isinstance(result["result"], str):
                    result["result"] = self._redact_text(result["result"], stats)
                elif isinstance(result["result"], dict):
                    result["result"] = self._redact_dict_fields(result["result"], stats)
        
        return result
    
    def _redact_message(
        self, 
        message: Dict[str, Any], 
        stats: RedactionStats
    ) -> Dict[str, Any]:
        """Redact a single message (input or output)."""
        result = dict(message)
        stats.messages_processed += 1
        
        if "parts" in result and isinstance(result["parts"], list):
            result["parts"] = [
                self._redact_message_part(part, stats)
                for part in result["parts"]
                if isinstance(part, dict)
            ]
        
        return result
    
    def _process_json_attribute(
        self,
        value: str,
        processor_fn,
        stats: RedactionStats
    ) -> str:
        """Parse JSON, apply processor, and re-serialize."""
        try:
            parsed = json.loads(value)
            
            if isinstance(parsed, list):
                processed = [processor_fn(item, stats) for item in parsed if isinstance(item, dict)]
            elif isinstance(parsed, dict):
                processed = processor_fn(parsed, stats)
            else:
                return value
            
            return json.dumps(processed, ensure_ascii=False)
        except json.JSONDecodeError:
            # If not valid JSON, apply text redaction directly
            return self._redact_text(value, stats)
    
    def _add_summary_attributes(
        self, 
        attributes: Dict[str, KeyValue], 
        stats: RedactionStats
    ):
        """Add summary attributes about redaction performed."""
        if self.config.summary == "silent":
            return
        
        prefix = "genai.redaction"
        
        if stats.patterns_matched > 0:
            attributes[f"{prefix}.patterns_matched"] = KeyValue.new_int_value(
                f"{prefix}.patterns_matched",
                stats.patterns_matched
            )
        
        if stats.fields_redacted > 0:
            attributes[f"{prefix}.fields_redacted"] = KeyValue.new_int_value(
                f"{prefix}.fields_redacted",
                stats.fields_redacted
            )
        
        if stats.tool_args_redacted > 0:
            attributes[f"{prefix}.tool_args_redacted"] = KeyValue.new_int_value(
                f"{prefix}.tool_args_redacted",
                stats.tool_args_redacted
            )
        
        if self.config.summary == "debug":
            attributes[f"{prefix}.messages_processed"] = KeyValue.new_int_value(
                f"{prefix}.messages_processed",
                stats.messages_processed
            )
    
    def process_spans(self, resource_spans: ResourceSpans):
        """
        Process spans and apply GenAI-specific redaction.
        
        This modifies the span attributes in place.
        """
        for scope_spans in resource_spans.scope_spans:
            for span in scope_spans.spans:
                self._process_span_attributes(span)
    
    def _process_messages_value(self, value: Any, stats: RedactionStats) -> Any:
        """
        Process a messages value which could be:
        - A JSON string containing an array of messages
        - A list of message dicts
        
        Returns the processed value in the same format.
        """
        # Handle JSON string
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                if isinstance(parsed, list):
                    processed = [self._redact_message(msg, stats) for msg in parsed if isinstance(msg, dict)]
                    return json.dumps(processed, ensure_ascii=False)
            except json.JSONDecodeError:
                # Not valid JSON, apply text redaction directly
                return self._redact_text(value, stats)
            return value
        
        # Handle list directly (array of message objects)
        if isinstance(value, list):
            return [self._redact_message(msg, stats) for msg in value if isinstance(msg, dict)]
        
        return value
    
    def _process_span_attributes(self, span):
        """Process a single span's attributes."""
        stats = RedactionStats()
        attr_map: Dict[str, KeyValue] = {kv.key: kv for kv in span.attributes}
        
        # Log all attribute keys for debugging
        if self.config.summary == "debug":
            print(f"[GenAI Redaction] Processing span with {len(attr_map)} attributes")
            print(f"[GenAI Redaction] Attribute keys: {list(attr_map.keys())}")
        
        # Process input messages (gen_ai.input.messages)
        if self.config.redact_input_messages and GENAI_INPUT_MESSAGES in attr_map:
            kv = attr_map[GENAI_INPUT_MESSAGES]
            value = kv.value.value
            if self.config.summary == "debug":
                print(f"[GenAI Redaction] Found {GENAI_INPUT_MESSAGES}, type: {type(value).__name__}")
            
            new_value = self._process_messages_value(value, stats)
            
            # Update the value - for strings use AnyValue, for lists we need to serialize to JSON
            if isinstance(new_value, str):
                kv.value = AnyValue(new_value)
            elif isinstance(new_value, list):
                # Serialize list back to JSON string for storage
                kv.value = AnyValue(json.dumps(new_value, ensure_ascii=False))
        
        # Process output messages (gen_ai.output.messages)
        if self.config.redact_output_messages and GENAI_OUTPUT_MESSAGES in attr_map:
            kv = attr_map[GENAI_OUTPUT_MESSAGES]
            value = kv.value.value
            if self.config.summary == "debug":
                print(f"[GenAI Redaction] Found {GENAI_OUTPUT_MESSAGES}, type: {type(value).__name__}")
            
            new_value = self._process_messages_value(value, stats)
            
            # Update the value
            if isinstance(new_value, str):
                kv.value = AnyValue(new_value)
            elif isinstance(new_value, list):
                kv.value = AnyValue(json.dumps(new_value, ensure_ascii=False))
        
        # Log redaction stats
        if self.config.summary == "debug":
            print(f"[GenAI Redaction] Stats: patterns={stats.patterns_matched}, fields={stats.fields_redacted}, tool_args={stats.tool_args_redacted}, messages={stats.messages_processed}")
        
        # Add summary attributes
        self._add_summary_attributes(attr_map, stats)
        
        # Update span attributes
        span.attributes = list(attr_map.values())


# ============================================================================
# Convenience factory functions
# ============================================================================

def create_pii_redactor(
    patterns: List[str] = None,
    use_hashing: bool = False,
    summary: str = "info"
) -> GenAIRedactionProcessor:
    """
    Create a processor with common PII patterns.
    
    Args:
        patterns: Additional custom patterns to redact
        use_hashing: If True, use SHA256 hashing instead of [REDACTED]
        summary: Summary level ("silent", "info", "debug")
    
    Returns:
        Configured GenAIRedactionProcessor
    """
    default_patterns = [
        # SSN (US)
        r'\b\d{3}-\d{2}-\d{4}\b',
        # Email addresses
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        # Phone numbers (various formats)
        r'\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b',
        # Credit card numbers (simple)
        r'\b(?:\d{4}[-\s]?){3}\d{4}\b',
        # IP addresses
        r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
    ]
    
    all_patterns = default_patterns + (patterns or [])
    
    config = GenAIRedactionConfig(
        content_patterns=all_patterns,
        redact_tool_fields=["password", "api_key", "secret", "token", "key", "credential"],
        redact_tool_arguments=["login", "authenticate", "set_password", "reset_password"],
        hash_function="sha256" if use_hashing else None,
        summary=summary,
    )
    
    return GenAIRedactionProcessor(config)


def create_minimal_redactor(
    tool_fields: List[str] = None,
    summary: str = "silent"
) -> GenAIRedactionProcessor:
    """
    Create a minimal processor that only redacts specific tool fields.
    
    Useful when you only want to redact known sensitive fields without
    pattern matching on all content.
    """
    config = GenAIRedactionConfig(
        content_patterns=[],
        redact_tool_fields=tool_fields or ["password", "api_key", "secret"],
        redact_tool_arguments=[],
        summary=summary,
    )
    
    return GenAIRedactionProcessor(config)

