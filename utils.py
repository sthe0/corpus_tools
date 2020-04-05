# -*- coding: utf-8 -*-
from glob import glob

import chardet
import itertools
import os.path
import shutil

def reset_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)
    
def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

def merge_files(corpus_path, topics):
    merged_utf8_path = os.path.join(corpus_path, 'merged', 'utf-8')
    reset_dir(merged_utf8_path)
    for topic in topics:
        utf8_topic_path = os.path.join(corpus_path, 'utf-8', topic)
        reset_dir(os.path.join(merged_utf8_path, topic))
        merged_utf8_topic_path = os.path.join(merged_utf8_path, topic, 'src.txt')
        with open(merged_utf8_topic_path, 'w') as fout:
            for utf8_fname in itertools.chain(glob(utf8_topic_path + '/*.html'), glob(utf8_topic_path + '/*.txt')):
                with open(utf8_fname) as fin:
                    fout.write(fin.read() + '.\n')

def detect_encoding(fname):
    with open(fname) as f:
        return chardet.detect(f.read())['encoding']
    
def fix_file_encoding(fname):
    enc = detect_encoding(fname).lower()[:5]
    with open(fname) as f:
        contents = f.read()
        if enc != 'utf-8':
            contents = contents.decode('cp1251')
        else:
            contents = contents.decode('utf-8')
    with open(fname, 'w') as f:
        f.write(contents.encode('utf-8'))    

def fix_encoding(corpus_path, topics):
    for topic in topics:
        topic_path = os.path.join(corpus_path, 'utf-8', topic)
        for fname in itertools.chain(glob(topic_path + '/*.txt'), glob(topic_path + '/*.html')):
            fix_file_encoding(fname)
