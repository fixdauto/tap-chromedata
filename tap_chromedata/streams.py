"""Stream type classes for tap-chromedata."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from singer_sdk.streams import Stream

#from tap_chromedata.client import chromedataStream
#from tap_chromedata.client import aceslegacyvehiclebaseStream
#from tap_chromedata.client import acesvehiclebaseStream
#from tap_chromedata.client import acesvehicleconfigbaseStream
#from tap_chromedata.client import acesvehiclemappingbaseStream
import ftplib
import io
import pandas as pd
import json
from zipfile import ZipFile

# TODO: Delete this is if not using json files for schema definition
#SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class QuickDataStream(Stream):
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
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        #  NotImplementedError("The method is not yet implemented (TODO)")
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
                            """
                            with archive.open('DeepLink.txt') as fd:
                                df = pd.read_csv(fd)
                                df_dict=df.to_dict(orient='records')
                                
                                for row in df_dict:
                                    yield row
                            """
                            with archive.open('DeepLink.txt') as fd:
                                data=fd.readlines()
                                for j in range(len(data)):

                                    data[j]=data[j].decode('utf-8')
                                    tildcount=0
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
                                for j in range(1,len(data)):
                                    row_dict={}
                                    for i in range(0,len(data[j])):
                                        row_dict[colnames[i]]=data[j][i]
                                    data[j]=row_dict
                                data=data[1:]
                                for row in data:
                                    yield row
        ftp.close()

class AcesLegacyVehicleSchemaStream(Stream):
    """Define custom stream."""
    name = "AcesLegacyVehicle"
    primary_keys = ["~LegacyVehicleID~"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("~VehicleConfigID~", th.IntegerType),
        th.Property("~LegacyVehicleID~", th.IntegerType)
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
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        #  NotImplementedError("The method is not yet implemented (TODO)")
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
                                        for j in range(1,len(data)):
                                            row_dict={}
                                            for i in range(0,len(data[j])):
                                                row_dict[colnames[i]]=data[j][i]
                                            data[j]=row_dict
                                        data=data[1:]
                                        for row in data:
                                            yield row
        ftp.close()

class AcesVehicleSchemaStream(Stream):
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
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        #  NotImplementedError("The method is not yet implemented (TODO)")
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
                                        for j in range(1,len(data)):
                                            row_dict={}
                                            for i in range(0,len(data[j])):
                                                row_dict[colnames[i]]=data[j][i]
                                            data[j]=row_dict
                                        data=data[1:]
                                        for row in data:
                                            yield row
        ftp.close()


class AcesVehicleConfigSchemaStream(Stream):
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
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        #  NotImplementedError("The method is not yet implemented (TODO)")
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
                                        for j in range(1,len(data)):
                                            row_dict={}
                                            for i in range(0,len(data[j])):
                                                row_dict[colnames[i]]=data[j][i]
                                            data[j]=row_dict
                                        data=data[1:]
                                        for row in data:
                                            yield row
        ftp.close()


class AcesVehicleMappingSchemaStream(Stream):
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
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        #  NotImplementedError("The method is not yet implemented (TODO)")
        #print(self.config_jsonschema[0])
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
                                        for j in range(1,len(data)):
                                            row_dict={}
                                            for i in range(0,len(data[j])):
                                                row_dict[colnames[i]]=data[j][i]
                                            data[j]=row_dict
                                        data=data[1:]
                                        for row in data:
                                            yield row
        ftp.close()