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

        # --- Phase 3: `blocked_values` / `allowed_values` ---
        # Apply to values of attributes that *remain* and *were not already masked by key patterns*
        for key in current_keys:
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
                # The Go processor's 'redactLogs' function also handles log body using value matchers.
                # It also adds meta-attributes for log body redaction.
                self._redact_log_body(log_record.body, log_record.attributes)

    def _redact_log_body(self, log_body_value: AnyValue | None, log_attributes: List[KeyValue]):

        """
        Applies regex-based blocking to the log body value (and recursively to nested structures).
        This uses the global blocked_values and allowed_values from the config.
        Also adds specific meta-attributes for log body redaction.
        """

        redacted_this_body_count = 0

        # Helper recursive function for processing value and tracking changes
        def _process_value_recursive(av: AnyValue, ignored_keys: set[str], redacted_keys: set[str],
                                     masked_keys: set[str]) -> AnyValue:
            nonlocal redacted_this_body_count  # Use nonlocal to modify outer scope variables

            if isinstance(av.value, str):
                print(f"Processing log body and we have a string")
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
                                should_allow = True
                                break
                        if not should_allow:
                            # Perform substitution with the redacted value (only the matched part is hashed/replaced)
                            temp_str = pattern.sub(self._get_redacted_value(match.group(0)), temp_str)
                            body_value_masked = True

                if body_value_masked:
                    redacted_this_body_count += 1  # Count each instance of value masking in body
                    av.value = AnyValue(temp_str)

            elif isinstance(av.value, KeyValueList):
                for kv in av.value:
                    if kv.key in self.config.ignored_keys:
                        ignored_keys.add(kv.key)
                        continue
                    if not self.config.allow_all_keys and kv.key not in self.config.allowed_keys:
                        redacted_keys.add(kv.key)
                        continue
                    return _process_value_recursive(kv.value, ignored_keys, redacted_keys, masked_keys)

            elif isinstance(av.value, ArrayValue):
                for v_item in av.value:
                    _process_value_recursive(v_item, ignored_keys, redacted_keys, masked_keys)

        ignored_keys = set()  # Initialize ignored keys
        redacted_keys = set()  # Initialize redacted keys
        masked_keys = set()  # Initialize masked keys
        # End helper recursive function
        _process_value_recursive(log_body_value, ignored_keys, redacted_keys,
                                 masked_keys)  # Start recursive processing

        # Add meta-attributes for log body redaction, similar to Go's addMetaAttrs for body
        # The Go code for log body meta-attrs often uses counts (e.g., redaction.log.body.masked.count)
        # It doesn't typically list keys by name for body redaction as it's content-based.
        # We will follow the Go approach of just using a count here, as there isn't a "key" concept for the matched value.
        # if redacted_this_body_count > 0:
        #     if self.config.summary == "info" or self.config.summary == "debug":
        #         # Ensure the name is aligned with what Go would generate for log body.
        #         # The Go `redactLogs` calls `addMetaAttrs` with "redaction.log.body.masked.names" and "redaction.log.body.masked.count".
        #         # It passes `nil` for names if it's just a count, which means the names attribute is skipped.
        #         # We'll use a specific fixed name for consistency.
        #
        #         # We'll just update the count for log body directly, as the Go code implies
        #         # `redactionBodyMaskedCount` is passed, but `redactionBodyMaskedKeys` (names) is not.
        #         log_attributes_kv_map = {kv.key: kv for kv in log_attributes}
        #         existing_count_val = log_attributes_kv_map.get("redaction.log.body.masked.count")
        #         current_total_count = redacted_this_body_count
        #         if existing_count_val is not None:
        #             if isinstance(existing_count_val.value, int):
        #                 current_total_count += existing_count_val.value
        #             elif isinstance(existing_count_val.value, float):
        #                 current_total_count += int(existing_count_val.value)
        #
        #         log_attributes_kv_map["redaction.log.body.masked.count"] = KeyValue("redaction.log.body.masked.count",
        #                                                                             AnyValue(current_total_count))
        #         final_log_attributes = []
        #         for key, value in log_attributes_kv_map.items():
        #             final_log_attributes.append(value)
        #             return final_log_attributes
        # else:
        #     return log_attributes
