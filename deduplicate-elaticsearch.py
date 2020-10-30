# A description and analysis of the code this is forked from can be found at
# https://alexmarquardt.com/2018/07/23/deduplicating-documents-in-elasticsearch/

import hashlib
from elasticsearch import Elasticsearch, helpers
import datetime
import secrets

ES_HOST = secrets.ES_HOST
ES_USER = secrets.ES_USER
ES_PASSWORD = secrets.ES_PASSWORD
ES_PORT = secrets.ES_PORT
ES_INDEX = secrets.ES_INDEX

es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT, 'use_ssl': True}], http_auth=(ES_USER, ES_PASSWORD))
dict_of_duplicate_docs = {}

# The following line defines the fields that will be
# used to determine if a document is a duplicate
keys_to_include_in_hash = ["@timestamp","logsource","message"]


# Process documents returned by the current search/scroll
def populate_dict_of_duplicate_docs(hit):

    combined_key = ""
    for mykey in keys_to_include_in_hash:
        combined_key += str(hit['_source'][mykey])

    _id = hit["_id"]

    hashval = hashlib.md5(combined_key.encode('utf-8')).digest()

    # If the hashval is new, then we will create a new key
    # in the dict_of_duplicate_docs, which will be
    # assigned a value of an empty array.
    # We then immediately push the _id onto the array.
    # If hashval already exists, then
    # we will just push the new _id onto the existing array
    dict_of_duplicate_docs.setdefault(hashval, []).append(_id)


# Loop over all documents in the index, and populate the
# dict_of_duplicate_docs data structure.
def scroll_over_all_docs(tmonth):
    tdelta = datetime.timedelta(days=1)
    enddate = (tmonth+tdelta)-datetime.timedelta(microseconds=1)
    print(tmonth)
    print(enddate)
    for hit in helpers.scan(es, index=ES_INDEX+str(tmonth.month).zfill(2), request_timeout=120 ,query={"query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "range": {
            "@timestamp": {
              "gte": datetime.datetime.strftime(tmonth,"%Y-%m-%dT%H:%M:%S%z"),
              "lte": datetime.datetime.strftime(enddate,"%Y-%m-%dT%H:%M:%S%z"),
              "format": "strict_date_hour_minute_second"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }
  }}):
        populate_dict_of_duplicate_docs(hit)


def loop_over_hashes_and_remove_duplicates(tmonth):
    # Search through the hash of doc values to see if any
    # duplicate hashes have been found
    topush=[]
    for hashval, array_of_ids in dict_of_duplicate_docs.items():
      if len(array_of_ids) > 1:
        # Pop out a doc from the array of duplicates to ensure at least one copy exists
        array_of_ids.pop()
        for doc in array_of_ids:
            # Add document delete operation to list of queries we will be sending to Elasticsearch via Bulk 
            topush.append({'_op_type':'delete','_index':ES_INDEX+str(tmonth.month).zfill(2),'_id':doc})
    # Print how many are being deleted
    print(len(topush))
    # Send list of delete queries to Elasticsearch via Bulk
    helpers.bulk(client=es, actions=topush, stats_only=True, request_timeout=120, raise_on_error=False)

       

def main():
    # Define start and end date for deduplication periods
    # delta is increments to step through
    #   - smaller increment has a lower memory overhead for the host running the script, with the tradeoff of more queries sent to Elasticsearch
    #     too small an increment results in either no duplicates being found or Elasticsearch being overwhelmed by too many requests
    #   - larger increment has more memory overhead (1 Month is about 4GB) for host running script but less queries sent to Elasticsearch
    #     one may need to increase request timeout values if using a large increment (days=30, days=60, days=180 etc )
    # Start and End date of deduplication period, stored as Python Datetime object in format (YYYY, M, D)
    start_date = datetime.datetime(2019, 2, 3)
    end_date = datetime.datetime(2019, 9, 30)
    # How many docs we want to check at a time, one hour equals hour-blocks, one day equals daily etc.
    delta = datetime.timedelta(days=1)

    # Start looping through docs, searching and deleting in increments of delta 
    while start_date <= end_date:
        scroll_over_all_docs(start_date)
        loop_over_hashes_and_remove_duplicates(start_date)
        start_date += delta


main()
