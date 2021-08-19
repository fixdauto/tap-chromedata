"""Stream type classes for tap-chromedata."""
import csv

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from singer_sdk.streams import Stream
import re
import ftplib
import io
import json
from zipfile import ZipFile

"""Convert camelCase words to snake_case"""
def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
    
"""Base stream class with config parameter getters, tranversers to ACES Mapping data and data cleaning"""
class ChromeDataStream(Stream):
    flo=""
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["FTP_URL"]
    @property
    def url_user(self) -> str:
        """Return the API URL user, configurable via tap settings."""
        return self.config["FTP_USER"]
    
    @property
    def url_pass(self) -> str:
        """Return the API URL password, configurable via tap settings."""
        return self.config["FTP_PASS"]
    """Function for traversing through ACES Mapping folder in the FTP Server"""
    def reading_ftp(self):
        ftp = ftplib.FTP(self.url_base)
        ftp.login(self.url_user,self.url_pass)
        files = ftp.nlst()
        for file in files:
            if file=="ACES":
                innerfiles=ftp.nlst(file)
                for innerfile in innerfiles:
                    if ".zip" in innerfile:
                        flo = io.BytesIO()
                        ftp.retrbinary("RETR /"+innerfile, flo.write)
                        flo.seek(0)
                        self.flo=flo
    """Function to read and preprocessing data to UTF-8, converting headers to snake case and removing ~ as the quoting character in the data"""
    def data_cleaner(self,data):
        for j in range(len(data)):
            data[j]=data[j].decode('utf-8')
        data[0]=camel_to_snake(data[0])
        
        colnames=data[0].split(",")
        for j in range(len(colnames)):
            colnames[j]=colnames[j].replace("~","")
        
            if '\r\n' in colnames[j]:
                colnames[j]=colnames[j].replace("\r\n","")
        datareader=csv.DictReader(data,quotechar='~',dialect='unix')
        return datareader,colnames

"""Class for reading the Quickdata records for all the years, zipped in year-by-year folder in the FTP server"""
class QuickDataStream(ChromeDataStream):
    name = "QuickData"
    primary_keys = ["_autobuilder_style_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("_model_year", th.IntegerType),
        th.Property("_division_name", th.StringType),
        th.Property("_model_name", th.StringType),
        th.Property("_hist_style_id", th.IntegerType),
        th.Property("_style_name", th.StringType),
        th.Property("_style_name_wo_trim", th.StringType),
        th.Property("_trim", th.StringType),
        th.Property("_full_style_code", th.StringType),
        th.Property("_style_sequence", th.IntegerType),
        th.Property("msrp", th.NumberType),
        th.Property("_invoice", th.NumberType),
        th.Property("_destination", th.NumberType),
        th.Property("_model_effective_date", th.StringType),
        th.Property("_model_comment", th.StringType),
        th.Property("_manufacturer_name", th.StringType),
        th.Property("_manufacturer_id", th.IntegerType),
        th.Property("_division_id", th.IntegerType),
        th.Property("_hist_model_id", th.IntegerType),
        th.Property("_market_class", th.StringType),
        th.Property("_market_class_id", th.IntegerType),
        th.Property("_subdivision_name", th.StringType),
        th.Property("_subdivision_id", th.IntegerType),
        th.Property("_style_id", th.IntegerType),
        th.Property("_autobuilder_style_id", th.StringType)
    ).to_dict()
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["FTP_URL"]
    @property
    def url_user(self) -> str:
        """Return the API URL user, configurable via tap settings."""
        return self.config["FTP_USER"]
    @property
    def url_pass(self) -> str:
        """Return the API URL password, configurable via tap settings."""
        return self.config["FTP_PASS"]
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        ftp = ftplib.FTP(self.url_base)
        ftp.login(self.url_user,self.url_pass)
        files = ftp.nlst()
        for file in files:
            if file=="QuickData_ALL":
                innerfiles=ftp.nlst(file)
                for innerfile in innerfiles:
                    innerinnerfiles=ftp.nlst(innerfile)
                    for innerinnerfile in innerinnerfiles:
                        flo = io.BytesIO()
                        ftp.retrbinary("RETR /"+innerinnerfile, flo.write)
                        flo.seek(0)
                        with ZipFile(flo) as archive:
                            with archive.open('DeepLink.txt') as fd:

                                data=fd.readlines()
                                datareader,colnames=self.data_cleaner(data)
                                for row in datareader:
                                    for key in colnames:
                                        if row[key]=='':
                                            row[key]=None
                                        elif 'string' in self.schema['properties'][key]['type']:
                                            row[key]=row[key]
                                        elif 'integer' in self.schema['properties'][key]['type']:
                                            row[key]=int(row[key])
                                        elif 'number' in self.schema['properties'][key]['type']:
                                            if row[key].count(".")==1:
                                                arr=row[key].split(".")
                                                if arr[0].isnumeric() and arr[1].isnumeric():
                                                    row[key]=float(row[key])
                                                else:
                                                    row[key]=row[key]
                                            else:
                                                row[key]=row[key]
                                        
                                        else:
                                            row[key]=row[key]
                                    yield row
        ftp.close()
"""Class for reading the ACES Legacy Vehicle records in the ACES Mapping folder of the FTP server"""
class AcesLegacyVehicleSchemaStream(ChromeDataStream):
    """Define custom stream."""
    name = "AcesLegacyVehicle"
    primary_keys = ["_vehicle_config_id","_legacy_vehicle_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("_vehicle_config_id", th.IntegerType),
        th.Property("_legacy_vehicle_id", th.IntegerType)
    ).to_dict()
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["FTP_URL"]
    @property
    def url_user(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["FTP_USER"]
    @property
    def url_pass(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["FTP_PASS"]
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        self.reading_ftp()
        with ZipFile(self.flo) as archive:
            with archive.open('AcesLegacyVehicle.txt') as fd:

                data=fd.readlines()
                datareader,colnames=self.data_cleaner(data)
                
                for row in datareader:
                    for key in colnames:
                        if row[key]=='':
                            row[key]=None
                        elif 'string' in self.schema['properties'][key]['type']:
                            row[key]=row[key]
                        elif 'integer' in self.schema['properties'][key]['type']:
                            row[key]=int(row[key])
                        elif 'number' in self.schema['properties'][key]['type']:
                            if row[key].count(".")==1:
                                arr=row[key].split(".")
                                if arr[0].isnumeric() and arr[1].isnumeric():
                                    row[key]=float(row[key])
                                else:
                                    row[key]=row[key]
                            else:
                                row[key]=row[key]
                        else:
                            row[key]=row[key]
                    yield row
"""Class for reading the ACES Vehicle records in the ACES Mapping folder of the FTP server"""
class AcesVehicleSchemaStream(ChromeDataStream):
    """Define custom stream."""
    name = "AcesVehicle"
    primary_keys = ["_vehicle_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("_vehicle_id", th.IntegerType),
        th.Property("_year_id", th.IntegerType),
        th.Property("_make_id", th.IntegerType),
        th.Property("_model_id", th.IntegerType),
        th.Property("_sub_model_id", th.IntegerType),
        th.Property("_region_id", th.IntegerType),
        th.Property("_base_vehicle_id", th.IntegerType)
    ).to_dict()
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        self.reading_ftp()
        with ZipFile(self.flo) as archive:
            with archive.open('AcesVehicle.txt') as fd:

                data=fd.readlines()
                datareader,colnames=self.data_cleaner(data)
                
                for row in datareader:
                    for key in colnames:
                        if row[key]=='':
                            row[key]=None
                        elif 'string' in self.schema['properties'][key]['type']:
                            row[key]=row[key]
                        elif 'integer' in self.schema['properties'][key]['type']:
                            row[key]=int(row[key])
                        elif 'number' in self.schema['properties'][key]['type']:
                            if row[key].count(".")==1:
                                arr=row[key].split(".")
                                if arr[0].isnumeric() and arr[1].isnumeric():
                                    row[key]=float(row[key])
                                else:
                                    row[key]=row[key]
                            else:
                                row[key]=row[key]
                        else:
                            row[key]=row[key]
                    yield row

"""Class for reading the ACES Vehicle Config records in the ACES Mapping folder of the FTP server"""
class AcesVehicleConfigSchemaStream(ChromeDataStream):
    """Define custom stream."""
    name = "AcesVehicleConfigVehicle"
    primary_keys = ["_aces_vehicle_config_id","_vehicle_config_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("_aces_vehicle_config_id", th.IntegerType),
        th.Property("_vehicle_config_id", th.IntegerType),
        th.Property("_vehicle_id", th.IntegerType),
        th.Property("_bed_config_id", th.IntegerType),
        th.Property("_body_style_config_id", th.IntegerType),
        th.Property("_brake_config_id", th.IntegerType),
        th.Property("_drive_type_id", th.IntegerType),
        th.Property("_engine_config_id", th.IntegerType),
        th.Property("_transmission_id", th.IntegerType),
        th.Property("_mfr_body_code_id", th.IntegerType),
        th.Property("_wheel_base_id", th.IntegerType),
        th.Property("_spring_type_config_id", th.IntegerType),
        th.Property("_steering_config_id", th.IntegerType)
    ).to_dict()
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        self.reading_ftp()
        with ZipFile(self.flo) as archive:
            with archive.open('AcesVehicleConfig.txt') as fd:

                data=fd.readlines()
                datareader,colnames=self.data_cleaner(data)
                
                for row in datareader:
                    for key in colnames:
                        if row[key]=='':
                            row[key]=None
                        elif 'string' in self.schema['properties'][key]['type']:
                            row[key]=row[key]
                        elif 'integer' in self.schema['properties'][key]['type']:
                            row[key]=int(row[key])
                        elif 'number' in self.schema['properties'][key]['type']:
                            if row[key].count(".")==1:
                                arr=row[key].split(".")
                                if arr[0].isnumeric() and arr[1].isnumeric():
                                    row[key]=float(row[key])
                                else:
                                    row[key]=row[key]
                            else:
                                row[key]=row[key]
                        else:
                            row[key]=row[key]
                    yield row

"""Class for reading the ACES Vehicle Mapping records in the ACES Mapping folder of the FTP server"""
class AcesVehicleMappingSchemaStream(ChromeDataStream):
    """Define custom stream."""
    name = "AcesVehicleMappingVehicle"
    primary_keys = ["_aces_vehicle_mapping_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("_aces_vehicle_mapping_id", th.IntegerType),
        th.Property("_vehicle_mapping_id", th.IntegerType),
        th.Property("_vehicle_id", th.IntegerType),
        th.Property("_aces_vehicle_config_id", th.IntegerType),
        th.Property("_style_id", th.IntegerType),
        th.Property("_option_codes", th.StringType)
    ).to_dict()
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        self.reading_ftp()
        with ZipFile(self.flo) as archive:
            with archive.open('AcesVehicleMapping.txt') as fd:

                data=fd.readlines()
                datareader,colnames=self.data_cleaner(data)
                
                for row in datareader:
                    for key in colnames:
                        if row[key]=='':
                            row[key]=None
                        elif 'string' in self.schema['properties'][key]['type']:
                            row[key]=row[key]
                        elif 'integer' in self.schema['properties'][key]['type']:
                            row[key]=int(row[key])
                        elif 'number' in self.schema['properties'][key]['type']:
                            if row[key].count(".")==1:
                                arr=row[key].split(".")
                                if arr[0].isnumeric() and arr[1].isnumeric():
                                    row[key]=float(row[key])
                                else:
                                    row[key]=row[key]
                            else:
                                row[key]=row[key]
                        else:
                            row[key]=row[key]
                    yield row
