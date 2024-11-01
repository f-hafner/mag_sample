#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Read in proquest dissertation attributes into table pq_attributes

goid, title, abstract_wordcount
"""

import os
import pandas as pd

from helpers.variables import datapath
import xml.etree.ElementTree as ET

def clean_title(title):
    if title:
        # Keep only alphanumeric characters, spaces, and basic punctuation
        # This will keep: a-z, A-Z, 0-9, space, period, comma, hyphen, apostrophe
        import re
        title = re.sub(r'[^a-zA-Z0-9\s.,\-\']', ' ', title)
        # Collapse multiple spaces into single space
        title = ' '.join(title.split())
    return title

def parse_xml(file_path):
    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Extract abswordcount from the RECORD tag attribute
    abswordcount = int(root.attrib.get('abswordcount', 0))

    # Extract goid from the GOID tag
    goid = root.findtext('GOID')

    # Extract title from the Title tag within TitleAtt
    title = root.find('.//TitleAtt/Title').text

    return abswordcount, goid, clean_title(title)

def load_xml_data(directory_path, test=False):
    data = []
    files = os.listdir(directory_path)
    # Limit to the first 100 files if test is True
    if test:
        files = files[:100]
    for filename in files:
        if filename.endswith('.xml'):
            file_path = os.path.join(directory_path, filename)
            abswordcount, goid, title = parse_xml(file_path)
            data.append({'goid': goid, 'abswordcount': abswordcount,
'title': title})

    return pd.DataFrame(data)


if __name__ == "__main__":
    path_proquest = "metadata_mar/"
    df = load_xml_data(datapath+path_proquest, test=False)
    df = df.drop_duplicates()
    has_duplicates = df['goid'].duplicated().any()
    print("df has duplicated goid: " + str(has_duplicates))
    df.to_csv(datapath+"pq_attributes.tsv", sep='\t', index=False)