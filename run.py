#!/usr/bin/env python
# -*- coding: utf-8 -*-
from utils import *
from lib import *

import argparse
import sys


def ngram_mode_main(args):
    print >>sys.stderr, 'Starting ngram mode...'
    m = process_texts(
        args.corpus_path,
        'ngrams-{0}-{1}'.format(args.min_order, args.max_order),
        processor=text_processor_ngrams(args.min_order, args.max_order, args.tokenize, args.get_ngrams, args.csv_to_txm),
        reset_xml_dir=True
    )


def vgroups_mode_main(args):
    print >>sys.stderr, 'Starting verb phrase mode...'
    m = process_texts(
        args.corpus_path,
        'verb-phrasess',
        processor=text_processor_named_groups(args.tokenize, args.tree_tagger, args.tree_tagger_model, args.get_named_groups, args.morphology_dict, args.verb_dict, args.csv_to_txm),
        reset_xml_dir=True
    )


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    ngram_mode_parser = subparsers.add_parser('ngrams', formatter_class=argparse.ArgumentDefaultsHelpFormatter, help='create ngram based corpus')
    ngram_mode_parser.add_argument('-m', '--min-order', type=int, default=2, help='minimum n-gram order')
    ngram_mode_parser.add_argument('-M', '--max-order', type=int, default=4, help='maximum n-gram order')
    ngram_mode_parser.add_argument('--tokenize', default=os.path.join('resources', 'Tokenize'), help='Path to Tokenize tool')
    ngram_mode_parser.add_argument('--get-ngrams', default=os.path.join('resources', 'GetNGrams'), help='Path to GetNGrams tool')
    ngram_mode_parser.add_argument('--csv-to-txm', default=os.path.join('resources', 'CsvToTxm'), help='Path to CsvToTxm tool')
    ngram_mode_parser.add_argument('corpus_path', help='Path to corpus')
    ngram_mode_parser.set_defaults(func=ngram_mode_main)
    vgroups_mode_parser = subparsers.add_parser('verb-phrases', formatter_class=argparse.ArgumentDefaultsHelpFormatter, help='create verb phrase based corpus')
    vgroups_mode_parser.add_argument('--tokenize', default=os.path.join('resources', 'Tokenize'), help='Path to Tokenize tool')
    vgroups_mode_parser.add_argument('--tree-tagger', default='tree-tagger', help='Path to tree-tagger tool')
    vgroups_mode_parser.add_argument('--tree-tagger-model', default=os.path.join('resources', 'ru.par'), help='Path to tree-tagger model')
    vgroups_mode_parser.add_argument('--get-named-groups', default=os.path.join('resources', 'GetNamedGroups'), help='Path to GetNamedGroups tool')
    vgroups_mode_parser.add_argument('--morphology-dict', default=os.path.join('resources', 'DataFiles', 'ru_dict.bin'), help='Path to morphology dictionary')
    vgroups_mode_parser.add_argument('--verb-dict', default=os.path.join('resources', 'DataFiles', 'ru_verbs.bin'), help='Path to verb dictionary')
    vgroups_mode_parser.add_argument('--csv-to-txm', default=os.path.join('resources', 'CsvToTxm'), help='Path to CsvToTxm tool')
    vgroups_mode_parser.add_argument('corpus_path', help='Path to corpus')
    vgroups_mode_parser.set_defaults(func=vgroups_mode_main)
    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == '__main__':
    sys.exit(main())
