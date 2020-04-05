# -*- coding: utf-8 -*-
from glob import glob
import itertools
import multiprocessing
import os.path
import os
import shutil
import subprocess
import sys

from utils import *

EOF_MARK = '%EndOfFile%'

def start_process(processes=None, **kwargs):
    p = multiprocessing.Process(**kwargs)
    p.start()
    if processes is not None:
        processes.append(p)
    return p

def iteritems(q):
    while True:
        item = q.get()
        if item is None:
            break
        yield item

def stdin2q(q, output_read, input_write):
    input_write.close()
    for line in output_read:
        q.put(line.rstrip())
    q.put(None)
    
def q2stdout(q, output_read, input_write):
    output_read.close()
    for line in iteritems(q):
        input_write.write(line.rstrip() + '\n')

def run_shell_process(cmd, q_in, processes=None):
    input_read, input_write = os.pipe()
    output_read, output_write = os.pipe()
    q_out = multiprocessing.Queue()
    p = subprocess.Popen(
        [cmd],
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        bufsize=1,
        universal_newlines=True,
        close_fds=True
    )
    p_in = start_process(target=q2stdout, args=(q_in, p.stdout, p.stdin), processes=processes)
    p_out = start_process(target=stdin2q, args=(q_out, p.stdout, p.stdin), processes=processes)
    p.stdin.close()
    p.stdout.close()
    p.join = lambda: p.wait()
    if processes is not None:
        processes.append(p)
    return q_out

def run(cmd):
    subprocess.check_call(
        [cmd],
        shell=True
    )

def labelled2eof(q_lines_in, q_labels_in, processes):
    q_lines_out = multiprocessing.Queue()
    def worker(q_lines_in, q_labels_in, q_lines_out):
        for line, is_last in zip(iteritems(q_lines_in), iteritems(q_labels_in)):
            q_lines_out.put(line)
            if is_last:
                q_lines_out.put(EOF_MARK)
        q_lines_out.put(None)

    start_process(target=worker, args=(q_lines_in, q_labels_in, q_lines_out), processes=processes)            
    return q_lines_out
        
def eof2labelled(q_lines_in, processes):
    q_lines_out = multiprocessing.Queue()
    q_labels_out = multiprocessing.Queue()
    def worker(q_lines_in, q_lines_out, q_labels_out):
        prev_line = None
        for line in iteritems(q_lines_in):
            if line.rstrip() == EOF_MARK:
                if prev_line is None:
                    raise RuntimeError('Unexpected empty file!')
                q_lines_out.put(prev_line)
                q_labels_out.put(True)
                line = None
            elif prev_line is not None:
                q_lines_out.put(prev_line)
                q_labels_out.put(False)
            prev_line = line
        if prev_line is not None:
            raise RuntimeError('Improperly EOF-chunked lines!')
        q_lines_out.put(None)
        q_labels_out.put(None)
        
    start_process(target=worker, args=(q_lines_in, q_lines_out, q_labels_out), processes=processes)
    return q_lines_out, q_labels_out

def qdup(q_in, processes=None):
    q_out_1 = multiprocessing.Queue()
    q_out_2 = multiprocessing.Queue()
    def worker(q_in, q_out_1, q_out_2):
        for item in iteritems(q_in):
            q_out_1.put(item)
            q_out_2.put(item)
        q_out_1.put(None)
        q_out_2.put(None)
    start_process(target=worker, args=(q_in, q_out_1, q_out_2), processes=processes)
    return q_out_1, q_out_2

def get_first_column(q_in, processes):
    q_out = multiprocessing.Queue()
    def worker(q_in, q_out):
        for line in iteritems(q_in):
            q_out.put(line.split('\t')[0])
        q_out.put(None)
    start_process(target=worker, args=(q_in, q_out), processes=processes)
    return q_out

def join_lines(q_lines_in_1, q_labels_in_1, q_lines_in_2, q_labels_in_2, processes):
    q_lines_out = multiprocessing.Queue()
    q_labels_out = multiprocessing.Queue()
    def worker(q_lines_in_1, q_labels_in_1, q_lines_in_2, q_labels_in_2, q_lines_out, q_labels_out):
        for line_1, label_1, line_2, label_2 in zip(
            iteritems(q_lines_in_1), iteritems(q_labels_in_1), iteritems(q_lines_in_2), iteritems(q_labels_in_2)
        ):
            if label_1 != label_2:
                raise RuntimeError('Bad label alignment!')
            if any(map(lambda x: x is None, [line_1, label_1, line_2, label_2])):
                raise RuntimeError('Unexpected None!')
            q_lines_out.put(line_1.rstrip() + '\t' + line_2.rstrip())
            q_labels_out.put(label_1)
        q_lines_out.put(None)
        q_labels_out.put(None)
        
    start_process(
        target=worker,
        args=(q_lines_in_1, q_labels_in_1, q_lines_in_2, q_labels_in_2, q_lines_out, q_labels_out),
        processes=processes
    )
    return q_lines_out, q_labels_out

def parse_input(q, processes):
    q_meta = multiprocessing.Queue()
    q_lines_in_eof = multiprocessing.Queue()
    
    def worker(q, q_meta, q_lines_in_eof):
        for topic, text_id, utf8_fname, xml_fname, csv_fname in iteritems(q):
            has_lines = False
            with open(utf8_fname) as fin:
                for line in fin:
                    has_lines = True
                    q_lines_in_eof.put(line.rstrip())
            if has_lines:
                q_lines_in_eof.put(EOF_MARK)
                q_meta.put((topic, text_id, xml_fname, csv_fname))
        q_meta.put(None)
        q_lines_in_eof.put(None)
        
    start_process(target=worker, args=(q, q_meta, q_lines_in_eof), processes=processes)
    return q_meta, q_lines_in_eof

def dump_result(q_meta, q_lines, header):
    lines = []
    for line in iteritems(q_lines):
        if line.rstrip() == EOF_MARK:
            topic, text_id, xml_fname, csv_fname = q_meta.get()
            print >>sys.stderr, xml_fname
            with open(csv_fname, 'w') as fout:
                fout.write(header + '\n')
                fout.write('\n'.join(lines))
                lines = []
            run('./CsvToTxm "{0}:{1}" <"{2}" >"{3}"'.format(topic, text_id, csv_fname, xml_fname))
        else:
            lines.append(line)

def text_processor_named_groups(q):
    processes = []
    q_meta, q_lines_in_eof = parse_input(q, processes=processes)

    q_lines_tokenized_eof = run_shell_process('./Tokenize' + ' ' + EOF_MARK, q_lines_in_eof, processes)
    q_lines_tokenized_eof_1, q_lines_tokenized_eof_2 = qdup(q_lines_tokenized_eof, processes)
    
    q_lines_tokenized_first_column_eof = get_first_column(q_lines_tokenized_eof_1, processes)
    q_lines_tokenized_first_column_labelled, q_labels_tokenized_first_column = eof2labelled(q_lines_tokenized_first_column_eof, processes)
    
    q_lines_tokenized_labelled_2, q_labels_tokenzied_2 = eof2labelled(q_lines_tokenized_eof_2, processes)

    q_lines_tagged_labelled = run_shell_process(
        'tree-tagger -lemma /Applications/treetagger/models/ru.par',
        q_lines_tokenized_first_column_labelled,
        processes
    )

    q_lines_joined_eof = labelled2eof(*join_lines(
        q_lines_tokenized_labelled_2, q_labels_tokenzied_2,
        q_lines_tagged_labelled, q_labels_tokenized_first_column,
        processes
    ), processes=processes)

    q_lines_named_groups_eof = run_shell_process(
        './GetNamedGroups' + ' ' + EOF_MARK,
        q_lines_joined_eof,
        processes
    )

    ng_header = '\t'.join(['w', 'ng_stem', 'ng_morph_tags', 'ng_type', 'tt_tag', 'tt_lemma'])
    dump_result(q_meta, q_lines_named_groups_eof, ng_header)

    for p in processes:
        p.join()

def text_processor_ngrams(min_order, max_order):
    def worker(q):
        processes = []
        q_meta, q_lines_in_eof = parse_input(q, processes=processes)

        q_lines_tokenized_eof = run_shell_process('./Tokenize' + ' ' + EOF_MARK, q_lines_in_eof, processes)

        q_lines_ngrams_eof = run_shell_process(
            './GetNGrams {0} {1} {2}'.format(min_order, max_order, EOF_MARK),
            q_lines_tokenized_eof,
            processes
        )

        dump_result(q_meta, q_lines_ngrams_eof, header='w\torder\tcount')

        for p in processes:
            p.join()

    return worker

def make_tasks(corpus_path, reset_xml_dir=False, fix_encoding=True, topics=None):
    metadata = []
    if reset_xml_dir:
        reset_dir(os.path.join(corpus_path, 'xml'))
    if reset_xml_dir:
        reset_dir(os.path.join(corpus_path, 'csv'))
    tasks = []
    for topic in topics:
        utf8_topic_path = os.path.join(corpus_path, 'utf-8', topic)
        xml_topic_path = os.path.join(corpus_path, 'xml', topic)
        csv_topic_path = os.path.join(corpus_path, 'csv', topic)
        create_dir_if_not_exists(xml_topic_path)
        create_dir_if_not_exists(csv_topic_path)
        for fname in itertools.chain(glob(utf8_topic_path + '/*.html'), glob(utf8_topic_path + '/*.txt')):
            if fix_encoding:
                fix_file_encoding(fname)
            text_id = str(len(metadata))
            metadata.append((topic, os.path.basename(fname)))
            tasks.append((
                topic,
                text_id,
                fname,
                os.path.join(xml_topic_path, text_id + '.xml'),
                os.path.join(csv_topic_path, text_id + '.csv')
            ))
    return tasks, metadata

def create_corpus(metadata, corpus_path, corpus_tag):
    csv_lines = [[
        'id', 
        'category',
        'fname'
    ]]
    src_dir = os.path.join(corpus_path, 'xml')
    dst_dir = os.path.join(corpus_path, corpus_tag)
    reset_dir(dst_dir)
    for src_fname in glob(os.path.join(src_dir, '*', '*.xml')):
        text_id = int(os.path.basename(src_fname)[:-len('.xml')])
        dst_fname = os.path.join(dst_dir, os.path.basename(src_fname))
        shutil.copyfile(src_fname, dst_fname)
        topic, fname = metadata[text_id]
        csv_lines.append(map(lambda x: '"{0}"'.format(x), [text_id, topic, fname]))
    with open(os.path.join(dst_dir, 'metadata.csv'), 'w') as fout:
        fout.write('\n'.join(map(lambda x: ','.join(x), csv_lines)))
    return csv_lines

def process_tasks(tasks, processor, n_processes=4):
    workers = []
    q = multiprocessing.Queue()
    for i in range(n_processes):
        worker = multiprocessing.Process(target=processor, args=(q,))
        worker.start()
        workers.append(worker)
        
    for task in tasks:
        q.put(task)
        
    for _ in range(len(workers)):
        q.put(None)
        
    for worker in workers:
        worker.join()
        
def detect_topics(corpus_path):
    topics = []
    for item in glob(os.path.join(corpus_path, 'utf-8', '*')):
        if os.path.isdir(item):
            topics.append(os.path.basename(item))
    return topics
        
def process_texts(corpus_path, corpus_tag, processor, reset_xml_dir=False, topics=None):
    if topics is None:
        topics = detect_topics(corpus_path)
        print >>sys.stderr, 'Automatically detected topics:', ', '.join(topics)
    print >>sys.stderr, 'making tasks...'
    tasks, metadata = make_tasks(corpus_path, reset_xml_dir=reset_xml_dir, fix_encoding=False, topics=topics)
    print >>sys.stderr, 'processing tasks...'
    process_tasks(tasks, processor, n_processes=4)
    print >>sys.stderr, 'creating corpus...'
    metadata = create_corpus(metadata, corpus_path, corpus_tag)
    print >>sys.stderr, 'done.'
    return metadata
