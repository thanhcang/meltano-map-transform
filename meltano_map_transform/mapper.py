"""A sample inline mapper app."""

from __future__ import annotations

from typing import TYPE_CHECKING

import singer_sdk.typing as th
from singer_sdk import _singerlib as singer
from singer_sdk.helpers._util import utc_now
from singer_sdk.mapper import PluginMapper
from singer_sdk.mapper_base import InlineMapper
import json
import os
import hashlib

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import PurePath


class StreamTransform(InlineMapper):
    """A map transformer which implements the Stream Maps capability."""

    name = "meltano-map-transformer"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "stream_maps",
            th.ObjectType(
                additional_properties=th.CustomType(
                    {
                        "type": ["object", "string", "null"],
                        "properties": {
                            "__filter__": {"type": ["string", "null"]},
                            "__source__": {"type": ["string", "null"]},
                            "__alias__": {"type": ["string", "null"]},
                            "__else__": {
                                "type": ["string", "null"],
                                "enum": [None, "__NULL__"],
                            },
                            "__key_properties__": {
                                "type": ["array", "null"],
                                "items": {"type": "string"},
                            },
                        },
                        "additionalProperties": {"type": ["string", "null"]},
                    },
                ),
            ),
            required=False,
            description="Stream maps",
        ),
        th.Property(
            "flattening_enabled",
            th.BooleanType(),
            description=(
                "'True' to enable schema flattening and automatically expand nested "
                "properties."
            ),
        ),
        th.Property(
            "flattening_max_depth",
            th.IntegerType(),
            description="The max depth to flatten schemas.",
        ),
    ).to_dict()

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Create a new inline mapper.

        Args:
            config: Mapper configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """

        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        self.mapper = PluginMapper(plugin_config=dict(self.config), logger=self.logger)
        self.stream_maps = {}
        self.set_stream_maps_from_env()

        self.logger.info("Makini mapper: working ")

    def map_schema_message(
        self,
        message_dict: dict,
    ) -> Generator[singer.Message, None, None]:
        """Map a schema message according to config.

        Args:
            message_dict: A SCHEMA message JSON dictionary.

        Yields:
            Transformed schema messages.
        """
        self._assert_line_requires(message_dict, requires={"stream", "schema"})
        schema = message_dict["schema"]

        # Register raw stream schema
        stream_id: str = message_dict["stream"]
        self.mapper.register_raw_stream_schema(
            stream_id,
            message_dict["schema"],
            message_dict.get("key_properties", []),
        )

        # customize
        field_mappings = self.stream_maps.get(stream_id, [])
        for field_name in field_mappings.keys():
            if field_name not in schema["properties"]:
                self.logger.info(f"Adding custom field '{field_name}' to schema.")
                schema["properties"][field_name] = {"type": ["string", "null"]}

        for stream_map in self.mapper.stream_maps[stream_id]:
            yield singer.SchemaMessage(
                stream_map.stream_alias,
                stream_map.transformed_schema,
                stream_map.transformed_key_properties,
                message_dict.get("bookmark_keys", []),
            )

    def map_record_message(
        self,
        message_dict: dict,
    ) -> Generator[singer.Message, None, None]:
        """Map a record message according to config.

        Args:
            message_dict: A RECORD message JSON dictionary.

        Yields:
            Transformed record messages.
        """
        self._assert_line_requires(message_dict, requires={"stream", "record"})

        stream_id: str = message_dict["stream"]
        record = message_dict["record"]

        for stream_map in self.mapper.stream_maps[stream_id]:
            mapped_record = stream_map.transform(message_dict["record"])
            
            # custom mapping
            if mapped_record is not None:
                field_mappings = self.stream_maps.get(stream_id, [])
                for mapping in field_mappings:
                    for field_name, key_expression in mapping.items():
                        try:
                            self.logger.debug(f"Evaluating key expression for '{field_name}': {key_expression}")
                            
                            # Extract the field names from the expression (e.g., "id,name")
                            required_fields = self.extract_fields_from_expression(key_expression)
                            field_values = [record.get(field) for field in required_fields]
                            if all(value is None or value == "" for value in field_values):
                                self.logger.warning(f"All required fields are empty for '{field_name}': {required_fields}")
                                mapped_record[field_name] = None
                                continue

                            concatenated_value = "".join(str(record.get(field, "")) for field in required_fields)            
                            hashed_value = self.md5_hash(concatenated_value)
                            mapped_record[field_name] = hashed_value

                            # self.logger.info(f"Key after genereated :  '{field_name}' is '{hashed_value}' ")

                        except Exception as e:
                            self.logger.error(f"Error evaluating key expression for '{field_name}' in stream '{stream_id}': {e}")
                            mapped_record[field_name] = None


            if mapped_record is not None:
                yield singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=message_dict.get("version"),
                    time_extracted=utc_now(),
                )

    def map_state_message(self, message_dict: dict) -> list[singer.Message]:
        """Do nothing to the message.

        Args:
            message_dict: A STATE message JSON dictionary.

        Returns:
            The same state message
        """
        return [singer.StateMessage(value=message_dict["value"])]

    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> Generator[singer.Message, None, None]:
        """Duplicate the message or alias the stream name as defined in configuration.

        Args:
            message_dict: An ACTIVATE_VERSION message JSON dictionary.

        Yields:
            An ACTIVATE_VERSION for each duplicated or aliased stream.
        """
        self._assert_line_requires(message_dict, requires={"stream", "version"})

        stream_id: str = message_dict["stream"]
        for stream_map in self.mapper.stream_maps[stream_id]:
            yield singer.ActivateVersionMessage(
                stream=stream_map.stream_alias,
                version=message_dict["version"],
            )

    def set_stream_maps_from_env(self):
        """Set the stream_maps from the environment variable MAPPER_STREAM_MAPS."""
        env_stream_maps = os.getenv("MAPPER_STREAM_MAPS", "[]")
        self.logger.info(f"Environment variable MAPPER_STREAM_MAPS: {env_stream_maps}")

        try:
            # Parse the JSON from the environment variable
            parsed_stream_maps = json.loads(env_stream_maps)

            # Iterate over the parsed stream maps and update the configuration
            for stream_map in parsed_stream_maps:
                for stream_name, mappings in stream_map.items():
                    if stream_name not in self.stream_maps:
                        self.stream_maps[stream_name] = []

                    # Ensure mappings are key-value pairs
                    if isinstance(mappings, list):
                        for mapping in mappings:
                            if isinstance(mapping, dict):
                                self.stream_maps[stream_name].append(mapping)
                            else:
                                self.logger.error(f"Invalid mapping format: {mapping}. Skipping.")
                    else:
                        self.logger.error(f"Invalid mappings format for stream '{stream_name}': {mappings}")

        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing MAPPER_STREAM_MAPS: {e}")
            self.stream_maps = {}


    def md5_hash(self,value: str) -> str:
        """Generate an MD5 hash of the given value."""
        if not value:
            return ""
        return hashlib.md5(value.encode("utf-8")).hexdigest()


    def extract_fields_from_expression(self, expression: str) -> list[str]:
        """Extract field names from a comma-separated expression."""
        if "," in expression:
        # Split by comma and strip whitespace
            return [field.strip() for field in expression.split(",")]
        else:
        # Return the expression as a single-element list
            return [expression.strip()] 
