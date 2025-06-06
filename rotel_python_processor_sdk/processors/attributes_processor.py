import hashlib
import re
from enum import Enum
from typing import Any, Dict, List, Optional

from rotel_sdk.open_telemetry.common.v1 import KeyValue, AnyValue


class ValueType(Enum):
    """Enum for possible attribute value types, mirroring RValue variants from Rust."""
    STRING = "string"
    INT = "int"
    DOUBLE = "double"
    BOOL = "bool"
    BYTES = "bytes"
    ARRAY = "array"  # Corresponds to RVArrayValue
    MAP = "map"  # Corresponds to KvListValue
    UNKNOWN = "unknown"  # For values that don't fit known types


# --- OpenTelemetry-like Configuration Classes ---

class Action(str, Enum):
    """Defines the types of actions that can be performed on attributes."""
    INSERT = "insert"
    UPDATE = "update"
    UPSERT = "upsert"
    DELETE = "delete"
    HASH = "hash"
    EXTRACT = "extract"
    CONVERT = "convert"


class ActionKeyValue:
    """
    Specifies an attribute action, similar to OpenTelemetry's `ActionKeyValue`.
    """

    def __init__(
            self,
            key: str,
            action: Action,
            value: Any = None,
            regex_pattern: Optional[str] = None,
            from_attribute: Optional[str] = None,
            # TODO: from_context not yet supported
            # from_context: Optional[str] = None,
            converted_type: Optional[str] = None,
    ):
        self.key = key
        self.action = action
        self.value = value
        self.regex_pattern = regex_pattern
        self.from_attribute = from_attribute
        # TODO: from_context not yet supported
        # self.from_context = from_context
        self.converted_type = converted_type

        self._validate_action_config()

        self.compiled_regex: Optional[re.Pattern] = None
        if self.regex_pattern:
            self.compiled_regex = re.compile(self.regex_pattern)

    def _value_source_count(self) -> int:
        """Counts how many value sources are specified."""
        count = 0
        if self.value is not None:
            count += 1
        if self.from_attribute is not None and self.from_attribute != "":
            count += 1
        # TODO: from_context not yet supported
        # if self.from_context is not None and self.from_context != "":
        #    count += 1
        return count

    def _validate_action_config(self):
        """Validates the configuration for a specific action."""
        value_source_count = self._value_source_count()

        if self.action in [Action.INSERT, Action.UPDATE, Action.UPSERT]:
            if value_source_count == 0:
                raise ValueError(
                    f"Action '{self.action.value}' requires 'value', 'from_attribute', or 'from_context'."
                )
            if value_source_count > 1:
                raise ValueError(
                    f"Action '{self.action.value}' can only have one value source."
                )
            if self.regex_pattern or self.converted_type:
                raise ValueError(
                    f"Action '{self.action.value}' does not use 'regex_pattern' or 'converted_type'."
                )
        elif self.action in [Action.DELETE, Action.HASH]:
            # For DELETE and HASH, key or regex_pattern can be used.
            # If key is empty, regex_pattern must be provided to match keys.
            if not self.key and not self.regex_pattern:
                raise ValueError(
                    f"Action '{self.action.value}' requires 'key' or 'regex_pattern'."
                )
            if value_source_count > 0:
                raise ValueError(
                    f"Action '{self.action.value}' does not use value sources."
                )
            if self.converted_type:
                raise ValueError(
                    f"Action '{self.action.value}' does not use 'converted_type'."
                )
        elif self.action == Action.EXTRACT:
            if value_source_count > 0:
                raise ValueError(
                    f"Action '{self.action.value}' does not use value sources."
                )
            if not self.regex_pattern:
                raise ValueError(
                    f"Action '{self.action.value}' requires 'regex_pattern'."
                )
            if self.converted_type:
                raise ValueError(
                    f"Action '{self.action.value}' does not use 'converted_type'."
                )
            # Regex pattern will be compiled and checked for named groups in the processor.
        elif self.action == Action.CONVERT:
            if value_source_count > 0 or self.regex_pattern:
                raise ValueError(
                    f"Action '{self.action.value}' does not use value sources or 'regex_pattern'."
                )
            if not self.converted_type:
                raise ValueError(
                    f"Action '{self.action.value}' requires 'converted_type'."
                )
            if self.converted_type not in ["string", "int", "double", "bool"]:
                raise ValueError(
                    f"Invalid 'converted_type' for action '{self.action.value}': {self.converted_type}. "
                    "Must be 'string', 'int', 'double', or 'bool'."
                )
        else:
            raise ValueError(f"Unsupported action: {self.action.value}")

    def __repr__(self):
        return f"ActionKeyValue(key='{self.key}', action='{self.action.value}', ...)"


class Config:
    """
    Configuration for the attributes processor, similar to OpenTelemetry's Config.
    """

    def __init__(self, actions: List[ActionKeyValue]):
        if not actions:
            raise ValueError("Config must have at least one action.")
        self.actions = actions


# --- Attributes Processor Implementation ---

class AttributeProcessor:
    """
    A processor for modifying log record attributes based on a defined configuration.
    This class implements the functionality of the OpenTelemetry Collector's
    attributes processor for logs, interacting with Rotel's pyo3-like Python SDK data models.
    """

    def __init__(self, config: Config):
        self._actions = config.actions
        # In a real scenario, you might pass a logger here
        self._logger = print  # Simple print for demonstration

    def _get_source_attribute_value(
            self, action_kv: ActionKeyValue, attributes: Dict[str, KeyValue]
    ) -> Optional[Any]:
        """
        Determines the source value for an attribute action.
        Mimics `getSourceAttributeValue` from the Go implementation.
        """
        if action_kv.value is not None:
            return action_kv.value
        elif action_kv.from_attribute:
            # Use get_by_key on AttributesList to find the KeyValue object
            source_kv = attributes[action_kv.from_attribute]
            return source_kv.value if source_kv and source_kv.value else None
        # TODO : When rotel supports context from request add from_context support
        # elif action_kv.from_context:
        # Simulate context values. In a real Rotel environment,
        # this would come from a proper context object.
        # For demonstration, we use a static mock context.
        #    mock_context = {
        #        "client.address": "192.168.1.1",
        #        "metadata.user_agent": "Python/3.9",
        #        "auth.user_id": "user123",
        #        "some_other_context_key": "context_value",
        #    }
        #    return mock_context.get(action_kv.from_context)
        return None

    def _hash_attribute(self, value: Any) -> AnyValue | None:
        """
        Calculates the SHA-256 hash of a value.
        Mimics `sha2Hasher` from the Go implementation
        Handles various types by converting them to string representation first.
        """
        if value is None:
            return None
        # Convert value to string for hashing, similar to Go's behavior
        # where it hashes the string representation of the value.
        s = str(value).encode('utf-8')
        return AnyValue(hashlib.sha256(s).hexdigest())

    def _convert_value(self, value: Any, target_type: str) -> AnyValue | None:
        """
        Converts a value to the specified target type.
        Mimics `convertValue` from the Go implementation.
        """
        if value is None:
            return None

        try:
            if target_type == "string":
                return AnyValue(str(value))
            elif target_type == "int":
                return AnyValue(int(value))
            elif target_type == "double":
                return AnyValue(float(value))
            elif target_type == "bool":
                # Convert to boolean: "true", "1", non-zero are True; "false", "0", empty are False
                if isinstance(value, str):
                    vl = value.lower()
                    if vl in ("true", '1', 't', 'y', 'yes'):
                        return AnyValue(bool(True))
                    else:
                        return AnyValue(bool(False))
                    # return AnyValue(bool(value.lower() in ('true', '1', 't', 'y', 'yes')))
                elif isinstance(value, (int, float)):
                    return AnyValue(bool(value))
                return AnyValue(bool(value))  # Default Python bool conversion
            else:
                self._logger(f"Warning: Unsupported conversion target type: {target_type}")
                return AnyValue(value)  # Return original value if target type is unsupported
        except (ValueError, TypeError) as e:
            self._logger(f"Warning: Could not convert value '{value}' to type '{target_type}': {e}")
            return AnyValue(value)  # Return original value on conversion error

    def _extract_attributes(self, action_kv: ActionKeyValue, attributes: Dict[str, KeyValue]) -> List[KeyValue]:
        """
        Extracts values from a string attribute using a regex pattern.
        Mimics `extractAttributes` from the Go implementation.
        """

        extracted = []
        if not action_kv.compiled_regex:
            return extracted

        source_kv = attributes[action_kv.key]
        if source_kv and source_kv.value:
            v = source_kv.value
            if not isinstance(v, str):
                return extracted

            source_string = v
            matches = action_kv.compiled_regex.finditer(source_string)

            for match in matches:
                for attr_name, extracted_value in match.groupdict().items():
                    if extracted_value is not None:
                        extracted.append(KeyValue(attr_name, AnyValue(extracted_value)))

        return extracted

    def _get_matching_keys(self, regex: re.Pattern, attributes: List[KeyValue]) -> List[str]:
        """
        Returns a list of keys that match the given regex pattern.
        Mimics `getMatchingKeys` from the Go implementation.
        """
        matching_keys = []
        for kv in attributes:
            if regex.match(kv.key):
                matching_keys.append(kv.key)
        return matching_keys

    # def process_log_record(self, log_record: LogRecord):
    def process_attributes(self, attributes: List[KeyValue]) -> List[KeyValue]:
        attrs: Dict[str, KeyValue] = {}
        for kv in attributes:
            attrs[kv.key] = kv.value

        to_remove = []

        for action_kv in self._actions:
            try:
                target_kv = attrs[action_kv.key]
            except KeyError:
                target_kv = None

            if action_kv.action == Action.DELETE:
                if action_kv.key:  # If a specific key is provided
                    if target_kv is not None:
                        to_remove.append(action_kv.key)
                if action_kv.compiled_regex:  # If a regex pattern is provided
                    keys_to_delete = self._get_matching_keys(action_kv.compiled_regex, attributes)
                    for key_to_delete in keys_to_delete:
                        to_remove.append(key_to_delete)
            elif action_kv.action == Action.INSERT:
                if not target_kv:  # Only insert if key does not exist
                    source_value = self._get_source_attribute_value(action_kv, attrs)
                    if source_value is not None:
                        attributes.append(KeyValue(action_kv.key, AnyValue(source_value)))
            elif action_kv.action == Action.UPDATE:
                if target_kv:  # Only update if key exists
                    source_value = self._get_source_attribute_value(action_kv, attrs)
                    if source_value is not None:
                        target_kv.value = AnyValue(source_value)
            elif action_kv.action == Action.UPSERT:
                source_value = self._get_source_attribute_value(action_kv, attrs)
                if source_value is not None:
                    if target_kv:  # Update if exists
                        target_kv.value = AnyValue(source_value)
                    else:  # Insert if not exists
                        attributes.append(KeyValue(action_kv.key, AnyValue(source_value)))
            elif action_kv.action == Action.HASH:
                if action_kv.key:
                    if target_kv:
                        target_kv.value = self._hash_attribute(target_kv.value)
                if action_kv.compiled_regex:
                    keys_to_hash = self._get_matching_keys(action_kv.compiled_regex, attributes)
                    for key_to_hash in keys_to_hash:
                        kv_to_hash = attrs[key_to_hash]
                        if kv_to_hash:
                            kv_to_hash.value = self._hash_attribute(kv_to_hash.value)
            elif action_kv.action == Action.EXTRACT:
                extracted = self._extract_attributes(action_kv, attrs)
                for extracted_kv in extracted:
                    attributes.append(extracted_kv)
            elif action_kv.action == Action.CONVERT:
                if target_kv:
                    if action_kv.converted_type is not None:
                        converted_value = self._convert_value(target_kv.value, action_kv.converted_type)
                        target_kv.value = converted_value  # Update the value of existing KeyValue

        to_remove_dict = {}
        final_attrs: List[KeyValue] = []
        for key in to_remove:
            to_remove_dict[key] = True
        for kv in attributes:
            if kv.key not in to_remove_dict:
                final_attrs.append(kv)

        return final_attrs
