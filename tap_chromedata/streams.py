"""Stream type classes for tap-chromedata."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_chromedata.client import chromedataStream
from tap_chromedata.client import aceslegacyvehiclebaseStream
from tap_chromedata.client import acesvehiclebaseStream
from tap_chromedata.client import acesvehicleconfigbaseStream
from tap_chromedata.client import acesvehiclemappingbaseStream
import ftplib
import io
import pandas as pd
import json
from zipfile import ZipFile

# TODO: Delete this is if not using json files for schema definition
#SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class UsersStream(chromedataStream):
    """Define custom stream."""
    name = "QuickData"
    primary_keys = ["~AutobuilderStyleID~"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("~ModelYear~", th.IntegerType),
        th.Property("~DivisionName~", th.StringType),
        th.Property("~ModelName~", th.StringType),
        th.Property("~HistStyleID~", th.IntegerType),
        th.Property("~StyleName~", th.StringType),
        th.Property("~StyleNameWOTrim~", th.StringType),
        th.Property("~Trim~", th.StringType),
        th.Property("~FullStyleCode~", th.StringType),
        th.Property("~StyleSequence~", th.IntegerType),
        th.Property("~MSRP~", th.NumberType),
        th.Property("~Invoice~", th.NumberType),
        th.Property("~Destination~", th.NumberType),
        th.Property("~ModelEffectiveDate~", th.StringType),
        th.Property("~ModelComment~", th.StringType),
        th.Property("~ManufacturerName~", th.StringType),
        th.Property("~ManufacturerID~", th.IntegerType),
        th.Property("~DivisionID~", th.IntegerType),
        th.Property("~HistModelID~", th.IntegerType),
        th.Property("~MarketClass~", th.StringType),
        th.Property("~MarketClassID~", th.IntegerType),
        th.Property("~SubdivisionName~", th.StringType),
        th.Property("~SubdivisionID~", th.IntegerType),
        th.Property("~StyleID~", th.IntegerType),
        th.Property("AutobuilderStyleID", th.StringType)
    ).to_dict()


class AcesLegacyVehicleSchemaStream(aceslegacyvehiclebaseStream):
    """Define custom stream."""
    name = "AcesLegacyVehicle"
    primary_keys = ["~LegacyVehicleID~"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("~VehicleConfigID~", th.IntegerType),
        th.Property("~LegacyVehicleID~", th.IntegerType)
    ).to_dict()
class AcesVehicleSchemaStream(acesvehiclebaseStream):
    """Define custom stream."""
    name = "AcesVehicle"
    primary_keys = ["~VehicleID~"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("~VehicleID~", th.IntegerType),
        th.Property("~YearID~", th.IntegerType),
        th.Property("~MakeID~", th.IntegerType),
        th.Property("~ModelID~", th.IntegerType),
        th.Property("~SubModelID~", th.IntegerType),
        th.Property("~RegionID~", th.IntegerType),
        th.Property("~BaseVehicleID~", th.IntegerType)
    ).to_dict()

class AcesVehicleConfigSchemaStream(acesvehicleconfigbaseStream):
    """Define custom stream."""
    name = "AcesVehicleConfigVehicle"
    primary_keys = ["~AcesVehicleConfigID~"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("~AcesVehicleConfigID~", th.IntegerType),
        th.Property("~VehicleConfigID~", th.IntegerType),
        th.Property("~VehicleID~", th.IntegerType),
        th.Property("~BedConfigID~", th.IntegerType),
        th.Property("~BodyStyleConfigID~", th.IntegerType),
        th.Property("~BrakeConfigID~", th.IntegerType),
        th.Property("~DriveTypeID~", th.IntegerType),
        th.Property("~EngineConfigID~", th.IntegerType),
        th.Property("~TransmissionID~", th.IntegerType),
        th.Property("~MfrBodyCodeID~", th.IntegerType),
        th.Property("~WheelBaseID~", th.IntegerType),
        th.Property("~SpringTypeConfigID~", th.IntegerType),
        th.Property("~SteeringConfigID~", th.IntegerType)
    ).to_dict()

class AcesVehicleMappingSchemaStream(acesvehiclemappingbaseStream):
    """Define custom stream."""
    name = "AcesVehicleMappingVehicle"
    primary_keys = ["~AcesVehicleConfigID~"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("~AcesVehicleMappingID~", th.IntegerType),
        th.Property("~VehicleMappingID~", th.IntegerType),
        th.Property("~VehicleID~", th.IntegerType),
        th.Property("~AcesVehicleConfigID~", th.IntegerType),
        th.Property("~StyleID~", th.IntegerType),
        th.Property("~OptionCodes~", th.StringType)
    ).to_dict()