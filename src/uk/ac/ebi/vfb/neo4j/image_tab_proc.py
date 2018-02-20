from .KB_tools import KB_pattern_writer
from .neo4j_tools import results_2_dict_list

import pandas as pd
import argparse
import yaml


parser = argparse.ArgumentParser()
parser.add_argument("--endpoint",
                    help="Endpoint for connection to neo4J prod")
parser.add_argument("--usr",
                    help="username")
parser.add_argument("--pwd",
                    help="password")
parser.add_argument("--record_name",
                    help="Path to image data curation table")
parser.add_argument("--id_range_start",
                    help="Integer specifying start of ID range to be use"
                         "for new IDs")

args = parser.parse_args()


kbr = KB_pattern_writer(args.endpoint, args.usr, args.pwd)

metafile = open(args.record_name + ".yaml", 'r')
cur_meta = yaml.load(metafile.read())

cur_meta_approved_keys = ['']# This should live in YAML

tab = pd.read_csv(args.record_name + ".tsv")

for r in tab.iterrows():
    kbr.add_anatomy_image_set(imaging_type=cur_meta['imaging_type'],
                              label=r['fu'],
                              start=args.id_range_start,
                              template=cur_meta['template'],
                              anatomical_type=r['bar'])





