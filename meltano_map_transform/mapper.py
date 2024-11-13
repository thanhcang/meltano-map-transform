import hashlib
import json
import os
from typing import TYPE_CHECKING
from singer_sdk import _singerlib as singer
from singer_sdk.helpers._util import utc_now
from singer_sdk.mapper_base import InlineMapper

if TYPE_CHECKING:
    from collections.abc import Generator


class MD5StreamTransform(InlineMapper):
    """A map transformer that applies MD5 hash transformations based on stream maps."""

    name = "md5-mapper"

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the MD5 inline mapper with configuration from environment variables."""
        super().__init__(*args, **kwargs)
        # Load stream maps from environment variable
        env_stream_maps = os.getenv("MAPPER_STREAM_MAPS", "[]")
        self.stream_maps = json.loads(env_stream_maps)

    def md5_hash(self, value: str) -> str:
        """Generate an MD5 hash of the given value."""
        if not value:
            return ""
        return hashlib.md5(value.encode("utf-8")).hexdigest()

    def map_record_message(
        self,
        message_dict: dict,
    ) -> Generator[singer.Message, None, None]:
        """Map a record message using MD5 hash transformations."""
        stream_id: str = message_dict["stream"]
        record = message_dict["record"]

        # Iterate through each mapping configuration for the stream
        for stream_map in self.stream_maps:
            if stream_id in stream_map:
                field_mappings = stream_map[stream_id]

                for field_name, fields_to_combine in field_mappings.items():
                    # Split the fields to combine (e.g., "id,name" -> ["id", "name"])
                    field_list = fields_to_combine.split(",")

                    # Check if 'id' is present and not null for required keys
                    if "id" in field_list and (not record.get("id")):
                        record[field_name] = None
                        continue

                    # Construct the string to hash
                    combined_value = ""
                    try:
                        for field in field_list:
                            value = str(record.get(field, ""))
                            combined_value += value

                        # Generate MD5 hash of the combined value
                        hashed_value = self.md5_hash(combined_value)
                        record[field_name] = hashed_value
                    except Exception as e:
                        self.logger.error(f"Error processing field '{field_name}' for stream '{stream_id}': {e}")
                        record[field_name] = None

        yield singer.RecordMessage(
            stream=stream_id,
            record=record,
            version=message_dict.get("version"),
            time_extracted=utc_now(),
        )

    def map_schema_message(
        self,
        message_dict: dict,
    ) -> Generator[singer.Message, None, None]:
        """Map a schema message according to the configuration."""
        self._assert_line_requires(message_dict, requires={"stream", "schema"})

        stream_id: str = message_dict["stream"]
        self.mapper.register_raw_stream_schema(
            stream_id,
            message_dict["schema"],
            message_dict.get("key_properties", []),
        )
        yield singer.SchemaMessage(
            stream_id,
            message_dict["schema"],
            message_dict.get("key_properties", []),
            message_dict.get("bookmark_keys", []),
        )

    def map_state_message(self, message_dict: dict) -> list[singer.Message]:
        """Return the state message unchanged."""
        return [singer.StateMessage(value=message_dict["value"])]

    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> Generator[singer.Message, None, None]:
        """Return an ACTIVATE_VERSION message."""
        self._assert_line_requires(message_dict, requires={"stream", "version"})

        stream_id: str = message_dict["stream"]
        yield singer.ActivateVersionMessage(
            stream=stream_id,
            version=message_dict["version"],
        )
