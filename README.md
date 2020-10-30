# elasticsearch-deduplication
_Elasticsearch Deduplication Script_
---

A simple Python script which assists in removing duplicate documents in Elasticsearch

For a full description on the code this is based on and how it works including an analysis of the memory requirements, see: https://alexmarquardt.com/2018/07/23/deduplicating-documents-in-elasticsearch/

---
**Requires:**

Access to Elasticsearch 

Python 3.6+

pip

Python Packages

* [elasticsearch](https://github.com/elastic/elasticsearch-py) 

  - _"Official low-level client for Elasticsearch. Its goal is to provide common ground for all Elasticsearch-related code in Python; because of this it tries to be opinion-free and very extendable."_

---
This is accomplished by:

* Scrolling through documents in specifed index, currently designed for month-based index patterns, filtered by optional Elasticsearch Query
* Creating a hash of a document based on defined criteria
* Using the "pop()" method for Python Lists to ensure one copy of document remains and deleting the rest 

The following files are expected to exist in the directory
from which this module is executed:

* secrets.py  
    - A collection of Elasticsearch vars and Authentication credentials with expected defintions:
        - ES_HOST = "URL_WITHOUT_SCHEMA"
        - ES_USER = "elastic"
        - ES_PASSWORD = "elastic"
        - ES_PORT = "9200"
        - ES_INDEX = "source-YYYY."
---

Example Usage:
```
import hashlib
from elasticsearch import Elasticsearch, helpers
import datetime

es = Elasticsearch([{'host':ES_HOST, 'port': ES_PORT, 'use_ssl': True}], http_auth=(ES_USER, ES_PASSWORD))
dict_of_duplicate_docs = {}

def main():
    # Define start and end date for deduplication periods
    # delta is increments to step through
    #   - smaller increment has a lower memory overhead for the host running the script, with the tradeoff of more queries sent to Elasticsearch; too small an increment results in either no duplicates being found or Elasticsearch being overwhelmed by too many requests
    #   - larger increment has more memory overhead for host running script but less queries sent to Elasticsearch, one may need to increase request timeout values if using a very large delta
    start_date = datetime.datetime(2019, 1, 1)
    end_date = datetime.datetime(2019, 12, 31)
    delta = datetime.timedelta(days=1)

    while start_date <= end_date:
        scroll_over_all_docs(start_date)
        loop_over_hashes_and_remove_duplicates(start_date)
        start_date += delta
```

