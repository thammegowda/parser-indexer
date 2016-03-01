from solr import Solr
import csv
import sys


def generate_solr_updates(csv_file):
    '''
    Generates Solr atomic updates
    :param csv_file: csv file with fields
    :return: stream of atomic updates
    '''
    with open(csv_file) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield {
                'id': row['id'],
                'sha1sum_s_md' : {'set':row['sha1sum']}
            }


if __name__ == '__main__':
    # Get the CSV file from kitware https://data.kitware.com/api/v1/file/56901bc88d777f429eac90aa/download
    infile = sys.argv[1]
    print("Reading from %s" % infile)
    solr_url = "http://localhost:8983/solr/imagecatdev"
    solr = Solr(solr_url)

    updates = generate_solr_updates(infile)
    count, res = solr.post_iterator(updates, commit=True, buffer_size=1000)

    print("Res : %s; count=%d" %(res, count))
