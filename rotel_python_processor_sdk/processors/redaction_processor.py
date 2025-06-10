"""
The RedactionProcessor provides OpenTelemetry-compatible redaction capabilities for modifying
attributes and log bodies on logs and spans. It supports redacting (deleting) keys,
masking (hashing or replacing) values based on key patterns or value patterns,
and explicitly allowing or ignoring keys/values to override redaction rules.

See redaction_processor_blocking_test.py for complete usage examples. Basic usage:

    # Create a config with desired redaction rules
    processor_config = RedactionProcessorConfig(
        # If true, all keys are initially allowed unless explicitly ignored.
        # If false, only keys in 'allowed_keys' are initially kept.
        allow_all_keys=False,

        # List of exact key names that are always allowed (not redacted/deleted),
        # overriding 'allow_all_keys=False' for these specific keys.
        allowed_keys=["safe_key", "http.method"],

        # List of exact key names that are always ignored (not redacted/deleted/masked),
        # overriding any blocking rules.
        ignored_keys=["telemetry.sdk.name", "telemetry.sdk.language"],

        # List of regex patterns. If an attribute key matches any pattern, its value will be masked.
        blocked_key_patterns=[".*password.*", ".*credit_card_number"],

        # List of regex patterns. If an attribute value (string) matches any pattern,
        # its value will be masked, unless it also matches an 'allowed_values' pattern.
        blocked_values=[".*SSN.*", ".*email@domain.com"],

        # List of regex patterns. If an attribute value (string) matches any of these patterns,
        # it will *not* be masked, even if it matches a 'blocked_values' pattern.
        allowed_values=[".*@mycompany.com"],

        # The hash function to use for masking values (e.g., "sha256", "md5").
        # If None, values are replaced with "[REDACTED]".
        # Available hash functions depend on the Python 'hashlib' module.
        hash_function="sha256",

        # The level of detail for summary attributes added to spans/logs/resources.
        # - "silent": No summary attributes are added.
        # - "info": Adds counts of redacted/masked/allowed/ignored keys.
        # - "debug": Adds counts and names of redacted/masked/allowed/ignored keys.
        summary="info" # Options: "debug", "info", "silent"
    )

    # Create and use processor
    processor = RedactionProcessor(processor_config)

    # Example for spans:
    # processor.process_spans(resource_spans)

    # Example for logs:
    # processor.process_logs(resource_logs)
"""
import hashlib
import re
from typing import List, Optional, Set, Dict

from rotel_sdk.open_telemetry.common.v1 import *
from rotel_sdk.open_telemetry.logs.v1 import *
from rotel_sdk.open_telemetry.trace.v1 import *


class RedactionProcessorConfig:
    def __init__(self,
                 allow_all_keys: bool = False,
                 allowed_keys: List[str] = None,
                 ignored_keys: List[str] = None,
                 blocked_key_patterns: List[str] = None,
                 blocked_values: List[str] = None,
                 allowed_values: List[str] = None,
                 hash_function: Optional[str] = None,
                 summary: str = "silent"):
        self.allow_all_keys = allow_all_keys
        self.allowed_keys = set(allowed_keys) if allowed_keys is not None else set()
        self.ignored_keys = set(ignored_keys) if ignored_keys is not None else set()

        self.blocked_key_patterns = [re.compile(p) for p in
                                     blocked_key_patterns] if blocked_key_patterns is not None else []
        self.blocked_values = [re.compile(p) for p in blocked_values] if blocked_values is not None else []
        self.allowed_values = [re.compile(p) for p in allowed_values] if allowed_values is not None else []

        if hash_function:
            if hash_function not in hashlib.algorithms_available:
                raise ValueError(
                    f"Hash function '{hash_function}' not supported. Available: {hashlib.algorithms_available}")
        self.hash_function = hash_function

        if summary not in ["debug", "info", "silent"]:
            raise ValueError(f"Summary level '{summary}' not supported. Must be 'debug', 'info', or 'silent'.")
        self.summary = summary


class RedactionProcessor:
    ATTR_VALUES_SEPARATOR = ","  # Matches Go's attrValuesSeparator

    # Constants for summary attribute names, matching processor.go for context
    # Note: Go uses "redactedKeys", "maskedKeys", "allowedKeys", "ignoredKeys" as internal tracking names.
    # The actual attribute names generated depend on context (span, log, metric, resource).
    # The Go code does this implicitly by passing context-specific meta-attribute names.
    # We will map these in our _add_meta_attrs calls.

    def __init__(self, config: RedactionProcessorConfig):
        self.config = config
        self._redacted_value_placeholder = "[REDACTED]"  # Default placeholder

    def _get_redacted_value(self, original_string: str) -> str:
        if self.config.hash_function:
            try:
                hasher = hashlib.new(self.config.hash_function)
                hasher.update(original_string.encode('utf-8'))
                return hasher.hexdigest()
            except Exception as e:
                print(
                    f"Warning: Failed to hash value with {self.config.hash_function}: {e}. Falling back to placeholder.")
                return self._redacted_value_placeholder
        return self._redacted_value_placeholder

    def _add_meta_attrs(self,
                        tracked_keys: Set[str],
                        # The set of keys for this meta-attribute category (e.g., deleted, masked)
                        attributes: Dict[str, KeyValue],  # The attributes map to add meta-attributes to
                        values_attr_name: str,
                        # The name for the string list attribute (e.g., "redaction.span.redacted_keys.names")
                        count_attr_name: str):  # The name for the count attribute (e.g., "redaction.span.redacted_keys.count")
        """
        Adds diagnostic information about redacted/masked/allowed/ignored attribute keys.
        This function strictly mimics the addMetaAttrs logic from the Go processor.
        """

        redacted_count = len(tracked_keys)
        if redacted_count == 0 or self.config.summary == "silent":
            return  # No keys to report for this category

        # Record summary as attributes
        if self.config.summary == "debug" and values_attr_name:
            existing_val = attributes.get(values_attr_name, None)
            combined_keys = set(tracked_keys)  # Start with current keys

            if existing_val is not None and isinstance(existing_val.value, str):
                # Add existing keys from the attribute to the combined set
                existing_keys_from_attr = set(existing_val.value.split(self.ATTR_VALUES_SEPARATOR))
                combined_keys.update(existing_keys_from_attr)

            kv = KeyValue(values_attr_name, AnyValue(self.ATTR_VALUES_SEPARATOR.join(sorted(list(combined_keys)))))
            attributes[values_attr_name] = kv

        existing_count_val = attributes.get(count_attr_name, None)
        current_total_count = redacted_count
        if existing_count_val is not None:
            if isinstance(existing_count_val.value, int):  # Check for INT type for count
                current_total_count += existing_count_val.value
            elif isinstance(existing_count_val.value, float):
                current_total_count += int(existing_count_val.value)

        attributes[count_attr_name] = KeyValue(count_attr_name, AnyValue(current_total_count))

    def _redact_attributes(self, attributes: List[KeyValue], context_type: str) -> List[KeyValue]:
        """
        Applies redaction rules to a MockPcommonMap of attributes based on the global config.
        This function strictly follows the order of operations and tracking from processor.go.
        """
        original_keys = [kv.key for kv in attributes]

        # Sets to track keys affected by different rule types for meta-data, matching Go names
        deleted_keys = set()
        masked_keys = set()  # This includes keys masked by blocked_key_patterns or blocked_values
        allowed_keys_for_meta = set()  # Keys that were explicitly allowed (kept) by allowed_keys
        ignored_keys_for_meta = set()  # Keys that were explicitly ignored (kept) by ignored_keys
        keys_to_delete = set()

        # --- Phase 1: Initial Key Deletion (allow_all_keys, allowed_keys, ignored_keys) ---

        if not self.config.allow_all_keys:
            # If not allowing all, then only keys in allowed_keys are initially kept
            for key in original_keys:
                if key not in self.config.allowed_keys:
                    # Candidate for deletion, unless it's in ignored_keys
                    if key not in self.config.ignored_keys:
                        keys_to_delete.add(key)
                        deleted_keys.add(key)
                    else:
                        ignored_keys_for_meta.add(key)
                else:
                    # Key is in allowed_keys
                    allowed_keys_for_meta.add(key)
                if key in self.config.ignored_keys:
                    # If a key is both allowed and ignored, it's counted as ignored for meta
                    ignored_keys_for_meta.add(key)
                    allowed_keys_for_meta.discard(key)  # Remove from allowed for cleaner meta

        else:  # self.config.allow_all_keys is True
            # All keys are initially considered allowed, but subject to ignored_keys
            for key in original_keys:
                if key in self.config.ignored_keys:
                    ignored_keys_for_meta.add(key)
                else:
                    allowed_keys_for_meta.add(
                        key)  # These are allowed by default, not explicitly by allowed_keys list

        filtered_attributes = []
        for kv in attributes:
            if kv.key not in keys_to_delete:
                filtered_attributes.append(kv)

        attributes = filtered_attributes

        # --- Phase 2: `blocked_key_patterns` ---
        # Apply to keys that remain after initial filtering
        current_keys = [kv.key for kv in attributes]
        kv_map = {kv.key: kv for kv in attributes}
        remaining_keys = set()
        # Get current keys after potential deletions
        for key in current_keys:
            for pattern in self.config.blocked_key_patterns:
                if pattern.search(key):
                    value_obj = kv_map[key].value
                    # Only apply to string values for now, consistent with Go's value handling in this context
                    if isinstance(value_obj.value, str):
                        original_value = value_obj.value
                        redacted_value = self._get_redacted_value(original_value)
                        value_obj.value = AnyValue(redacted_value)
                        masked_keys.add(key)  # Track as masked
                    break  # Matched a pattern, no need to check others for this key
                else:
                    remaining_keys.add(key)

        # --- Phase 3: `blocked_values` / `allowed_values` ---
        # Apply to values of attributes that *remain* and *were not already masked by key patterns*
        for key in remaining_keys:
            value_obj = kv_map[key].value
            if isinstance(value_obj.value, str):  # Only apply to string values
                original_str = value_obj.value
                should_block = False
                for pattern in self.config.blocked_values:
                    res = pattern.search(original_str)
                    if res:
                        should_block = True
                        break

                if should_block:
                    should_allow = False
                    for pattern in self.config.allowed_values:
                        if pattern.search(original_str):
                            should_allow = True
                            break

                    if not should_allow:  # If blocked and not allowed, then mask
                        redacted_value = self._get_redacted_value(original_str)
                        value_obj.value = AnyValue(redacted_value)
                        masked_keys.add(key)  # Track as masked

        self._add_meta_attrs(deleted_keys, kv_map, f"redaction.{context_type}.redacted_keys.names",
                             f"redaction.{context_type}.redacted_keys.count")
        self._add_meta_attrs(masked_keys, kv_map, f"redaction.{context_type}.masked_keys.names",
                             f"redaction.{context_type}.masked_keys.count")
        # Go processor sometimes has allowedKeys and ignoredKeys for body as well.
        # For attributes, the allowedKeys are explicitly collected based on whether they were initially kept by the list.
        # For ignoredKeys, it's those explicitly in the ignored_keys list.
        self._add_meta_attrs(allowed_keys_for_meta, kv_map, f"redaction.{context_type}.allowed_keys.names",
                             f"redaction.{context_type}.allowed_keys.count")
        self._add_meta_attrs(ignored_keys_for_meta, kv_map, "",
                             f"redaction.{context_type}.ignored_keys.count")  # names are not added for ignored_keys in Go

        final_attributes = []
        for key, value in kv_map.items():
            final_attributes.append(value)
        return final_attributes

    def process_spans(self, resource_spans: ResourceSpans):
        if resource_spans.resource is not None:
            resource_spans.resource.attributes = self._redact_attributes(resource_spans.resource.attributes,
                                                                         "resource")
        for ss in resource_spans.scope_spans:
            for span in ss.spans:
                attrs = self._redact_attributes(span.attributes, "span")
                span.attributes = attrs

    # TODO: Add support for metrics
    # def process_metrics(self, metrics: MockMetrics) -> MockMetrics:
    #     for rm in metrics.resource_metrics():
    #         self._redact_attributes(rm.resource().attributes(), "resource")
    #         for sm in rm.scope_metrics():
    #             for metric in sm.metrics():
    #                 for dp in metric.data_points():
    #                     if isinstance(dp, dict):  # Metric data points attributes are dicts in our mock
    #                         temp_map = MockPcommonMap(dp)
    #                         self._redact_attributes(temp_map, "metric")
    #                         dp.clear()
    #                         dp.update(temp_map.as_map())
    #     return metrics

    def process_logs(self, resource_logs: ResourceLogs):
        if resource_logs.resource is not None:
            resource_logs.resource.attributes = self._redact_attributes(resource_logs.resource.attributes, "resource")
        for sl in resource_logs.scope_logs:
            for log_record in sl.log_records:
                log_record.attributes = self._redact_attributes(log_record.attributes, "log")
                # Log body redaction: apply blocked_values/allowed_values
                self._redact_log_body(log_record)

    def _redact_log_body(self, log_record: LogRecord):

        """
        Applies regex-based blocking to the log body value (and recursively to nested structures).
        This uses the global blocked_values and allowed_values from the config.
        Also adds specific meta-attributes for log body redaction.
        """

        # Helper recursive function for processing value and tracking changes
        def _process_value_recursive(av: AnyValue, key: Optional[str], allowed_keys: set[str], ignored_keys: set[str],
                                     redacted_keys: set[str],
                                     masked_keys: set[str]) -> AnyValue:

            if isinstance(av.value, str):
                original_str = av.value
                temp_str = original_str

                body_value_masked = False
                for pattern in self.config.blocked_values:
                    # Check if it matches a blocked pattern
                    match = pattern.search(temp_str)
                    if match:
                        # If it matches, check if it's allowed
                        should_allow = False
                        for allowed_pattern in self.config.allowed_values:
                            if allowed_pattern.search(temp_str):
                                if key is not None:
                                    allowed_keys.add(key)
                                should_allow = True
                                break
                        if not should_allow:
                            # Perform substitution with the redacted value (only the matched part is hashed/replaced)
                            temp_str = pattern.sub(self._get_redacted_value(match.group(0)), temp_str)
                            body_value_masked = True

                if body_value_masked:
                    av.value = AnyValue(temp_str)
                    if key is not None:
                        masked_keys.add(key)

            elif isinstance(av.value, KeyValueList):
                for kv in av.value:
                    if kv.key in self.config.ignored_keys:
                        ignored_keys.add(kv.key)
                        continue
                    if not self.config.allow_all_keys and kv.key not in self.config.allowed_keys:
                        redacted_keys.add(kv.key)
                        continue
                    _process_value_recursive(kv.value, kv.key, allowed_keys, ignored_keys, redacted_keys, masked_keys)

            elif isinstance(av.value, ArrayValue):
                for v_item in av.value:
                    _process_value_recursive(v_item, None, allowed_keys, ignored_keys, redacted_keys, masked_keys)

        ignored_keys = set()  # Initialize ignored keys
        redacted_keys = set()  # Initialize redacted keys
        masked_keys = set()  # Initialize masked keys
        allowed_keys = set()  # Initialize allowed keys
        # End helper recursive function
        if log_record.body is None:
            return

        _process_value_recursive(log_record.body, None, allowed_keys, ignored_keys, redacted_keys,
                                 masked_keys)  # Start recursive processing

        if self.config.summary == "info" or self.config.summary == "debug":
            kv_map = {kv.key: kv for kv in log_record.attributes}
            self._add_meta_attrs(redacted_keys, kv_map, f"redaction.body.redacted.keys",
                                 f"redaction.body.redacted.count")
            self._add_meta_attrs(masked_keys, kv_map, f"redaction.body.masked.keys",
                                 f"redaction.body.masked.count")
            self._add_meta_attrs(allowed_keys, kv_map, f"redaction.body.allowed.keys",
                                 f"redaction.body.allowed.count")
            self._add_meta_attrs(ignored_keys, kv_map, "redaction.body.ignored.keys",
                                 f"redaction.body.ignored.count")  # names are not added for ignored_keys in Go

            final_attributes = []
            for key, value in kv_map.items():
                final_attributes.append(value)

            log_record.attributes = final_attributes
