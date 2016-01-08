from solr import Solr
import csv



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
                'id': row['path'],
                'smqtk_sha1_s_md' : {'set':row['smqtk_sha1']},
                'smqtk_hg_semiauto_d_md': {'set':row['smqtk_hg_semiauto']},
                'smqtk_hg_revolver_d_md': {'set':row['smqtk_hg_revolver']},
                'smqtk_hg_long_gun_d_md': {'set':row['smqtk_hg_long_gun']}
            }


if __name__ == '__main__':
    # Get the CSV file from kitware https://data.kitware.com/api/v1/file/56901bc88d777f429eac90aa/download
    infile = "/home/tg/tmp/fields/smqtk_data.csv"
    solr_url = "http://imagecat.dyndns.org:8983/solr/imagecatdev"
    solr = Solr(solr_url)

    updates = generate_solr_updates(infile)
    count, res = solr.post_iterator(updates, commit=True, buffer_size=1000)

    print("Res : %s; count=%d" %(res, count))
