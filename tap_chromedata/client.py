"""Custom client handling, including chromedataStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.streams import Stream

import ftplib
import io
import pandas as pd
import json
from zipfile import ZipFile



class chromedataStream(Stream):
    """Stream class for chromedata streams."""

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
        ftp = ftplib.FTP('ftp.chromedata.com')
        ftp.login('u326277','102022a0')
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
class aceslegacyvehiclebaseStream(Stream):
    """Stream class for chromedata streams."""

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
        ftp = ftplib.FTP('ftp.chromedata.com')
        ftp.login('u326277','102022a0')
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
class acesvehiclebaseStream(Stream):
    """Stream class for chromedata streams."""

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
        ftp = ftplib.FTP('ftp.chromedata.com')
        ftp.login('u326277','102022a0')
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

class acesvehicleconfigbaseStream(Stream):
    """Stream class for chromedata streams."""

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
        ftp = ftplib.FTP('ftp.chromedata.com')
        ftp.login('u326277','102022a0')
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
class acesvehiclemappingbaseStream(Stream):
    """Stream class for chromedata streams."""

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
        ftp = ftplib.FTP('ftp.chromedata.com')
        ftp.login('u326277','102022a0')
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