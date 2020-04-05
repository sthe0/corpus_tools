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
        processor=text_processor_ngrams(args.min_order, args.max_order),
        reset_xml_dir=True
    )


def vgroups_mode_main(args):
    print >>sys.stderr, 'Starting verb phrase mode...'
    m = process_texts(
        args.corpus_path,
        'verb_phrasess',
        processor=text_processor_named_groups,
        reset_xml_dir=True
    )


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()
    ngram_mode_parser = subparsers.add_parser('ngrams', help='create ngram based corpus')
    ngram_mode_parser.add_argument('-m', '--min-order', type=int, default=2, help='minimum n-gram order')
    ngram_mode_parser.add_argument('-M', '--max-order', type=int, default=4, help='maximum n-gram order')
    ngram_mode_parser.set_defaults(func=ngram_mode_main)
    vgroups_mode_parser = subparsers.add_parser('verb_phrases', help='create verb phrase based corpus')
    vgroups_mode_parser.set_defaults(func=vgroups_mode_main)
    parser.add_argument('corpus_path', help='Path to corpus')
    args = parser.parse_args()
    args.func(args)
    return 0


if __name__ == '__main__':
    sys.exit(main())
