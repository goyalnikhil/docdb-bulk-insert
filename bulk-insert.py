import sys
import json
import os
import argparse
import math
import logging
import multiprocessing
import threading
import pymongo
import boto3


def handler_function() :
    print("hello world")

def checkS3Access(args):
    print("checking S3")

def loadJsonFromFile(args, fileName):
    #function to load JSON string from a file on disk
    with open(fileName, 'r') as file:
        data = json.load(file)
        print(data)

if __name__ == "__main__":
    handler_function()

    argParser = argparse.ArgumentParser(description="Bulk data insert")

    argParser.add_argument('--uri', required=True, type=str, help='DocDB connecton string URI')
    argParser.add_argument('--database', required=True, type=str, help='Database to use')
    argParser.add_argument('--collection', required=True, type=str, help='Collection to use')
    argParser.add_argument('--batch-size', required=True, type=int, help='Number of documents to insert per batch')
    argParser.add_argument('--bucket-name', required=True, type=str, help='S3 bucket URI fo the documents')
    argParser.add_argument('--threads', required=True, type=int, help='Number of threads')

    args = argParser.parse_args()

    checkS3Access(args)

    loadJsonFromFile(args, "test.json")