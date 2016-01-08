from solr import Solr
import json

def remove_last_modified(docs):
    '''
    Removes last modified value from from docs
    :param docs:
    :return:
    '''
    for d in docs:
        u = {'id': d['id'],
            'lastModified' : {'set': None},
            'dates' : {'set': None}
        }
        print("%s" % u)
        yield u

def store_stream(docs, filename):
    '''
    save the documents in json line file
    :param docs:
    :param filename: name of file
    :return:
    '''
    with open(filename, 'w') as o:
        count = 0
        for d in docs:
            o.write(json.dumps(d))
            o.write("\n")
            count += 1
        return count

def read_stream(filename):
    '''
    Reads json line stream
    :param filename: path to json line
    :return: doc stream
    '''
    with open(filename) as inf:
        for l in inf:
            yield json.loads(l)

if __name__ == '__main__':
    url = "http://imagecat.dyndns.org:8983/solr/imagecatdev"
    filename = "docs.docs.jsonl"
    solr = Solr(url)
    docs = solr.query_iterator("lastModified:[1960-01-01T00:00:00Z TO 2005-12-31T00:00:00Z]",
                        rows=1000, fl='id')

    count = store_stream(docs, filename)
    print("Wrote %d docs to %s" % (count, filename))
    docs = read_stream(filename)
    updates = remove_last_modified(docs)

    count, success = solr.post_iterator(updates, False)
    print(success)
    print(count)

