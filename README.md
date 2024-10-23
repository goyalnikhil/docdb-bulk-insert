# docdb-bulk-insert

## Usage
usage: bulk-insert.py [-h] --uri URI --database DATABASE --collection COLLECTION --batch-size BATCH_SIZE --input-file INPUT_FILE [--compress]

Bulk data insert

optional arguments:
  -h, --help            show this help message and exit
  --uri URI             DocDB connecton string URI
  --database DATABASE   Database to use
  --collection COLLECTION
                        Collection to use
  --batch-size BATCH_SIZE
                        Number of documents to insert per batch
  --input-file INPUT_FILE
                        Input file with JSON data
  --compress            Compress the raw JSON before storing in the Raw attribute
