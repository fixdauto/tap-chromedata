"""chromedata tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

import ftplib
import io
import json
from zipfile import ZipFile

# TODO: Import your custom stream types here:
from tap_chromedata.streams import (
    QuickDataStream,
    AcesLegacyVehicleSchemaStream,
    AcesVehicleConfigSchemaStream,
    AcesVehicleSchemaStream,
    AcesVehicleMappingSchemaStream
    
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    QuickDataStream,
    AcesLegacyVehicleSchemaStream,
    AcesVehicleConfigSchemaStream,
    AcesVehicleSchemaStream,
    AcesVehicleMappingSchemaStream
]


class Tapchromedata(Tap):
    """chromedata tap class."""
    name = "tap-chromedata"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property("tap_chromedata_ftp_url", th.StringType, required=True),
        th.Property("tap_chromedata_ftp_user", th.StringType, required=True),
        th.Property("tap_chromedata_ftp_pass", th.StringType)
    ).to_dict()
    
    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
