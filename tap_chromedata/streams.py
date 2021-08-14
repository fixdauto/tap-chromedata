"""Stream type classes for tap-chromedata."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from singer_sdk.streams import Stream
import re
#from tap_chromedata.client import chromedataStream
#from tap_chromedata.client import aceslegacyvehiclebaseStream
#from tap_chromedata.client import acesvehiclebaseStream
#from tap_chromedata.client import acesvehicleconfigbaseStream
#from tap_chromedata.client import acesvehiclemappingbaseStream
import ftplib
import io
import json
from zipfile import ZipFile

# TODO: Delete this is if not using json files for schema definition
#SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class QuickDataStream(Stream):
    """Define custom stream."""
    name = "QuickData"
    primary_keys = ["autobuilder_style_id"]
    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("model_year", th.IntegerType),
        th.Property("division_name", th.StringType),
        th.Property("model_name", th.StringType),
        th.Property("hist_style_id", th.IntegerType),
        th.Property("style_name", th.StringType),
        th.Property("style_name_wo_trim", th.StringType),
        th.Property("trim", th.StringType),
        th.Property("full_style_code", th.StringType),
        th.Property("style_sequence", th.IntegerType),
        th.Property("msrp", th.NumberType),
        th.Property("invoice", th.NumberType),
        th.Property("destination", th.NumberType),
        th.Property("model_effective_date", th.StringType),
        th.Property("model_comment", th.StringType),
        th.Property("manufacturer_name", th.StringType),
        th.Property("manufacturer_id", th.IntegerType),
        th.Property("division_id", th.IntegerType),
        th.Property("hist_model_id", th.IntegerType),
        th.Property("market_class", th.StringType),
        th.Property("market_class_id", th.IntegerType),
        th.Property("subdivision_name", th.StringType),
        th.Property("subdivision_id", th.IntegerType),
        th.Property("style_id", th.IntegerType),
        th.Property("autobuilder_style_id", th.StringType)
    ).to_dict()
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_URL"]
    @property
    def url_user(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_USER"]
    @property
    def url_pass(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_PASS"]
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        def camel_to_snake(name):
            name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
        ftp = ftplib.FTP(self.url_base)
        ftp.login(self.url_user,self.url_pass)
        files = ftp.nlst()
        for file in files:
            if file=="QuickData_ALL":
                innerfiles=ftp.nlst(file)
                for innerfile in innerfiles:
                    #innerfile=innerfile.split("/")[-1]
                    innerinnerfiles=ftp.nlst(innerfile)
                    for innerinnerfile in innerinnerfiles:
                        #innerinnerfile=innerinnerfile.split("/")[-1]
                        

                        flo = io.BytesIO()
                        ftp.retrbinary("RETR /"+innerinnerfile, flo.write)
                        flo.seek(0)
                        with ZipFile(flo) as archive:
                            with archive.open('DeepLink.txt') as fd:
                                data=fd.readlines()
                                for j in range(len(data)):

                                    data[j]=data[j].decode('utf-8')
                                    tildcount=0
                                    if '\r\n' in data[j]:
                                        data[j]=data[j].replace('\r\n','')
                                    for i in range(len(data[j])):
                                        if data[j][i]=='~':
                                            if tildcount==0:
                                                tildcount+=1
                                            else:
                                                tildcount-=1
                                        if data[j][i]==",":
                                            if tildcount==1:
                                                data[j]=data[j][:i]+' '+data[j][i+1:]
                                    data[j] = data[j].split(',')
                                    
                                colnames=data[0]
                                for i in range(len(colnames)):
                                    if '~' in colnames[i]:
                                        colnames[i]=colnames[i].replace('~','')
                                    colnames[i]=camel_to_snake(colnames[i])
                                for j in range(1,len(data)):
                                    row_dict={}
                                    for i in range(0,len(data[j])):
                                        if data[j][i].isnumeric():
                                            row_dict[colnames[i]]=int(data[j][i])
                                        elif data[j][i].count(".")==1:
                                            arr=data[j][i].split(".")
                                            if arr[0].isnumeric() and arr[1].isnumeric():
                                                row_dict[colnames[i]]=float(data[j][i])
                                            else:
                                                row_dict[colnames[i]]=data[j][i]
                                        elif data[j][i]=='':
                                            row_dict[colnames[i]]=None
                                        else:
                                            row_dict[colnames[i]]=data[j][i]
                                    data[j]=row_dict
                                data=data[1:]
                                for row in data:
                                    yield row
        ftp.close()

class AcesLegacyVehicleSchemaStream(Stream):
    """Define custom stream."""
    name = "AcesLegacyVehicle"
    primary_keys = ["vehicle_config_id","legacy_vehicle_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("vehicle_config_id", th.IntegerType),
        th.Property("legacy_vehicle_id", th.IntegerType)
    ).to_dict()
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_URL"]
    @property
    def url_user(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_USER"]
    @property
    def url_pass(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_PASS"]
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        def camel_to_snake(name):
            name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
        ftp = ftplib.FTP(self.url_base)
        ftp.login(self.url_user,self.url_pass)
        files = ftp.nlst()
        for file in files:
            if file=="ACES":
                innerfiles=ftp.nlst(file)
                #print(innerfiles)
                for innerfile in innerfiles:
                    if ".zip" in innerfile:
                        #print(innerfile)
                
                        flo = io.BytesIO()
                        ftp.retrbinary("RETR /"+innerfile, flo.write)
                        flo.seek(0)
                        with ZipFile(flo) as archive:
                            for f in archive.filelist:
                        
                                if f.filename=="AcesLegacyVehicle.txt":
                                    #print(f.filename)
                                    with archive.open(f.filename) as fp:
                                        data=fp.readlines()
                                        for j in range(len(data)):
                                            data[j]=data[j].decode('utf-8')
                                            tildcount=0
                                            if '\r\n' in data[j]:
                                                data[j]=data[j].replace('\r\n','')
                                            #print(data[j])
                                            for i in range(len(data[j])):
                                                if data[j][i]=='~':
                                                    if tildcount==0:
                                                        tildcount+=1
                                                    else:
                                                        tildcount-=1
                                                if data[j][i]==",":
                                                    if tildcount==1:
                                                        data[j]=data[j][:i]+' '+data[j][i+1:]
                                            data[j] = data[j].split(',')
                                        colnames=data[0]
                                        for i in range(len(colnames)):
                                            if '~' in colnames[i]:
                                                colnames[i]=colnames[i].replace('~','')
                                            colnames[i]=camel_to_snake(colnames[i])
                                        for j in range(1,len(data)):
                                            row_dict={}
                                            for i in range(0,len(data[j])):
                                                if data[j][i].isnumeric():
                                                    row_dict[colnames[i]]=int(data[j][i])
                                                elif data[j][i].count(".")==1:
                                                    arr=data[j][i].split(".")
                                                    if arr[0].isnumeric() and arr[1].isnumeric():
                                                        row_dict[colnames[i]]=float(data[j][i])
                                                    else:
                                                        row_dict[colnames[i]]=data[j][i]
                                                elif data[j][i]=='':
                                                    row_dict[colnames[i]]=None
                                                else:
                                                    row_dict[colnames[i]]=data[j][i]
                                            data[j]=row_dict
                                        data=data[1:]
                                        for row in data:
                                            yield row
        ftp.close()

class AcesVehicleSchemaStream(Stream):
    """Define custom stream."""
    name = "AcesVehicle"
    primary_keys = ["vehicle_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("vehicle_id", th.IntegerType),
        th.Property("year_id", th.IntegerType),
        th.Property("make_id", th.IntegerType),
        th.Property("model_id", th.IntegerType),
        th.Property("sub_model_id", th.IntegerType),
        th.Property("region_id", th.IntegerType),
        th.Property("base_vehicle_id", th.IntegerType)
    ).to_dict()
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_URL"]
    @property
    def url_user(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_USER"]
    @property
    def url_pass(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_PASS"]
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        def camel_to_snake(name):
            name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
        ftp = ftplib.FTP(self.url_base)
        ftp.login(self.url_user,self.url_pass)
        files = ftp.nlst()
        for file in files:
            if file=="ACES":
                innerfiles=ftp.nlst(file)
                #print(innerfiles)
                for innerfile in innerfiles:
                    if ".zip" in innerfile:
                        #print(innerfile)
                
                        flo = io.BytesIO()
                        ftp.retrbinary("RETR /"+innerfile, flo.write)
                        flo.seek(0)
                        with ZipFile(flo) as archive:
                            for f in archive.filelist:
                        
                                if f.filename=="AcesVehicle.txt":
                                    #print(f.filename)
                                    with archive.open(f.filename) as fp:
                                        data=fp.readlines()
                                        for j in range(len(data)):
                                            data[j]=data[j].decode('utf-8')
                                            tildcount=0
                                            if '\r\n' in data[j]:
                                                data[j]=data[j].replace('\r\n','')
                                            #print(data[j])
                                            for i in range(len(data[j])):
                                                if data[j][i]=='~':
                                                    if tildcount==0:
                                                        tildcount+=1
                                                    else:
                                                        tildcount-=1
                                                if data[j][i]==",":
                                                    if tildcount==1:
                                                        data[j]=data[j][:i]+' '+data[j][i+1:]
                                            data[j] = data[j].split(',')
                                        colnames=data[0]
                                        for i in range(len(colnames)):
                                            if '~' in colnames[i]:
                                                colnames[i]=colnames[i].replace('~','')
                                            colnames[i]=camel_to_snake(colnames[i])
                                        for j in range(1,len(data)):
                                            row_dict={}
                                            for i in range(0,len(data[j])):
                                                if data[j][i].isnumeric():
                                                    row_dict[colnames[i]]=int(data[j][i])
                                                elif data[j][i].count(".")==1:
                                                    arr=data[j][i].split(".")
                                                    if arr[0].isnumeric() and arr[1].isnumeric():
                                                        row_dict[colnames[i]]=float(data[j][i])
                                                    else:
                                                        row_dict[colnames[i]]=data[j][i]
                                                elif data[j][i]=='':
                                                    row_dict[colnames[i]]=None
                                                else:
                                                    row_dict[colnames[i]]=data[j][i]
                                            data[j]=row_dict
                                        data=data[1:]
                                        for row in data:
                                            yield row
        ftp.close()


class AcesVehicleConfigSchemaStream(Stream):
    """Define custom stream."""
    name = "AcesVehicleConfigVehicle"
    primary_keys = ["aces_vehicle_config_id","vehicle_config_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("aces_vehicle_config_id", th.IntegerType),
        th.Property("vehicle_config_id", th.IntegerType),
        th.Property("vehicle_id", th.IntegerType),
        th.Property("bed_config_id", th.IntegerType),
        th.Property("body_style_config_id", th.IntegerType),
        th.Property("brake_config_id", th.IntegerType),
        th.Property("drive_type_id", th.IntegerType),
        th.Property("engine_config_id", th.IntegerType),
        th.Property("transmission_id", th.IntegerType),
        th.Property("mfr_body_code_id", th.IntegerType),
        th.Property("wheel_base_id", th.IntegerType),
        th.Property("spring_type_config_id", th.IntegerType),
        th.Property("steering_config_id", th.IntegerType)
    ).to_dict()
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_URL"]
    @property
    def url_user(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_USER"]
    @property
    def url_pass(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_PASS"]
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        def camel_to_snake(name):
            name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
        ftp = ftplib.FTP(self.url_base)
        ftp.login(self.url_user,self.url_pass)
        files = ftp.nlst()
        for file in files:
            if file=="ACES":
                innerfiles=ftp.nlst(file)
                #print(innerfiles)
                for innerfile in innerfiles:
                    if ".zip" in innerfile:
                        #print(innerfile)
                
                        flo = io.BytesIO()
                        ftp.retrbinary("RETR /"+innerfile, flo.write)
                        flo.seek(0)
                        with ZipFile(flo) as archive:
                            for f in archive.filelist:
                        
                                if f.filename=="AcesVehicleConfig.txt":
                                    #print(f.filename)
                                    with archive.open(f.filename) as fp:
                                        data=fp.readlines()
                                        for j in range(len(data)):
                                            data[j]=data[j].decode('utf-8')
                                            tildcount=0
                                            if '\r\n' in data[j]:
                                                data[j]=data[j].replace('\r\n','')
                                            #print(data[j])
                                            for i in range(len(data[j])):
                                                if data[j][i]=='~':
                                                    if tildcount==0:
                                                        tildcount+=1
                                                    else:
                                                        tildcount-=1
                                                if data[j][i]==",":
                                                    if tildcount==1:
                                                        data[j]=data[j][:i]+' '+data[j][i+1:]
                                            data[j] = data[j].split(',')
                                        colnames=data[0]
                                        for i in range(len(colnames)):
                                            if '~' in colnames[i]:
                                                colnames[i]=colnames[i].replace('~','')
                                            colnames[i]=camel_to_snake(colnames[i])
                                        for j in range(1,len(data)):
                                            row_dict={}
                                            for i in range(0,len(data[j])):
                                                if data[j][i].isnumeric():
                                                    row_dict[colnames[i]]=int(data[j][i])
                                                elif data[j][i].count(".")==1:
                                                    arr=data[j][i].split(".")
                                                    if arr[0].isnumeric() and arr[1].isnumeric():
                                                        row_dict[colnames[i]]=float(data[j][i])
                                                    else:
                                                        row_dict[colnames[i]]=data[j][i]
                                                elif data[j][i]=='':
                                                    row_dict[colnames[i]]=None
                                                else:
                                                    row_dict[colnames[i]]=data[j][i]
                                            data[j]=row_dict
                                        data=data[1:]
                                        for row in data:
                                            yield row
        ftp.close()


class AcesVehicleMappingSchemaStream(Stream):
    """Define custom stream."""
    name = "AcesVehicleMappingVehicle"
    primary_keys = ["aces_vehicle_mapping_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("aces_vehicle_mapping_id", th.IntegerType),
        th.Property("vehicle_mapping_id", th.IntegerType),
        th.Property("vehicle_id", th.IntegerType),
        th.Property("aces_vehicle_config_id", th.IntegerType),
        th.Property("style_id", th.IntegerType),
        th.Property("option_codes", th.StringType)
    ).to_dict()
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_URL"]
    @property
    def url_user(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_USER"]
    @property
    def url_pass(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        #print(self.config["FTP_URL"],self.config["FTP_USER"],self.config["FTP_PASS"])
        return self.config["FTP_PASS"]
    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        def camel_to_snake(name):
            name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
        ftp = ftplib.FTP(self.url_base)
        ftp.login(self.url_user,self.url_pass)
        files = ftp.nlst()
        for file in files:
            if file=="ACES":
                innerfiles=ftp.nlst(file)
                #print(innerfiles)
                for innerfile in innerfiles:
                    if ".zip" in innerfile:
                        #print(innerfile)
                
                        flo = io.BytesIO()
                        ftp.retrbinary("RETR /"+innerfile, flo.write)
                        flo.seek(0)
                        with ZipFile(flo) as archive:
                            for f in archive.filelist:
                        
                                if f.filename=="AcesVehicleMapping.txt":
                                    #print(f.filename)
                                    with archive.open(f.filename) as fp:
                                        data=fp.readlines()
                                        for j in range(len(data)):
                                            data[j]=data[j].decode('utf-8')
                                            tildcount=0
                                            if '\r\n' in data[j]:
                                                data[j]=data[j].replace('\r\n','')
                                            #print(data[j])
                                            for i in range(len(data[j])):
                                                if data[j][i]=='~':
                                                    if tildcount==0:
                                                        tildcount+=1
                                                    else:
                                                        tildcount-=1
                                                if data[j][i]==",":
                                                    if tildcount==1:
                                                        data[j]=data[j][:i]+' '+data[j][i+1:]
                                            data[j] = data[j].split(',')
                                        colnames=data[0]
                                        for i in range(len(colnames)):
                                            if '~' in colnames[i]:
                                                colnames[i]=colnames[i].replace('~','')
                                            colnames[i]=camel_to_snake(colnames[i])
                                        for j in range(1,len(data)):
                                            row_dict={}
                                            for i in range(0,len(data[j])):
                                                if data[j][i].isnumeric():
                                                    row_dict[colnames[i]]=int(data[j][i])
                                                elif data[j][i].count(".")==1:
                                                    arr=data[j][i].split(".")
                                                    if arr[0].isnumeric() and arr[1].isnumeric():
                                                        row_dict[colnames[i]]=float(data[j][i])
                                                    else:
                                                        row_dict[colnames[i]]=data[j][i]
                                                elif data[j][i]=='':
                                                    row_dict[colnames[i]]=None
                                                else:
                                                    row_dict[colnames[i]]=data[j][i]
                                            data[j]=row_dict
                                        data=data[1:]
                                        for row in data:
                                            yield row
        ftp.close()
