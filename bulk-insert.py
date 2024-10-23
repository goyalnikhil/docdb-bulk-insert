import time
import json
import argparse
import logging
import pymongo
import lzma

#logging = logging.getLogger(__name__)

def loadJsonFromFile(args):
    #Connect to DocumentDB
    client = pymongo.MongoClient(args.uri)
    db = client[args.database]
    collection = db[args.collection]
    #Load JSON string from a file on disk
    inputFile = open(args.input_file, 'r')
    json_data = json.load(inputFile)
    totalDocs = len(json_data['AddOrUpdate'])

    logging.info(f'{args.input_file},{totalDocs},')
    logging.info('inserts in batch,time to insert')

    thisBatchIns = 0
    insList = []
    batchInsertTime = 0

    for doc in json_data['AddOrUpdate']:
        insDoc = {}
        insDoc["_id"] = doc["documentId"]
        insDoc["OrderingId"] = int(time.time())
        rawDoc = json.dumps(doc)
        if args.compress:
            insDoc["Raw"] = lzma.compress(rawDoc.encode('utf-8'))
        else:
            insDoc["Raw"] = rawDoc
        insDoc["ExtraFields"] = {}
        insDoc["Deleted"] = False
        insList.append(pymongo.InsertOne(insDoc))
        thisBatchIns += 1
        if thisBatchIns == args.batch_size:
            batchStartTime = time.time()
            result = collection.bulk_write(insList, ordered=True)
            batchInsertTime += (time.time() - batchStartTime) * 1000
            logging.info(f'{thisBatchIns},{batchInsertTime}')
            insList = []
            thisBatchIns = 0
    if len(insList) > 0:
        batchStartTime = time.time()
        result = collection.bulk_write(insList, ordered=True)
        batchInsertTime += (time.time() - batchStartTime) * 1000
        logging.info(f'{len(insList)},{batchInsertTime}')

    logging.info(f'{args.input_file},{totalDocs},{batchInsertTime}')

if __name__ == "__main__":
    logging.basicConfig(filename='bulk-insert.log', format='%(asctime)s,%(message)s', datefmt='%m-%d-%Y %I:%M:%S', level=logging.INFO)
    argParser = argparse.ArgumentParser(description="Bulk data insert")

    argParser.add_argument('--uri', required=True, type=str, help='DocDB connecton string URI')
    argParser.add_argument('--database', required=True, type=str, help='Database to use')
    argParser.add_argument('--collection', required=True, type=str, help='Collection to use')
    argParser.add_argument('--batch-size', required=True, type=int, help='Number of documents to insert per batch')
    argParser.add_argument('--input-file', required=True, type=str, help='Input file with JSON data')
    argParser.add_argument('--compress', required=False, action='store_true', help='Compress the raw JSON before storing in the Raw attribute')

    args = argParser.parse_args()

    loadJsonFromFile(args)