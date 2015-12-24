# coding=utf-8
"""
Create by WeiChen on 2015/12/2
"""
from __future__ import print_function, unicode_literals
import sys
import re
import os
import numpy as np
import datetime
import time
import json

sys.path.append("../")
import jieba
import jieba.posseg as pseg


def preprocess(folder_path, output_file):
    title_counter = 0
    f = open(output_file, 'a+')
    # for each file
    for filename in os.listdir(folder_path):
        if filename.startswith('.'):
            continue
        os.chdir(folder_path)
        if os.path.isfile(filename):
            rawdict = {}
            title_counter += 1
            rawdict["title"] = filename
            content = open(filename.encode(sys.getfilesystemencoding()), 'r').read()
            word_in_file = []
            lines = re.split('。|、|？|！|\?|!+|；|;|!\?|\?!|\?+', content.decode('utf-8'))

            for line in lines:
                word_in_line = []
                words = jieba.cut(line, cut_all=False, HMM=False)
                p = re.compile('[一-龥]')
                for word in list(words):
                    if p.match(word) and len(word) > 1:
                        word_in_line.append(word)
                word_in_file.append(word_in_line)
            rawdict["words"] = word_in_file
            json_str = json.dumps(rawdict)
            f.write(json_str + "\n".encode('utf-8'))
    f.close()
    print("\nParse %s articles" % title_counter)
    print("Output: %s" % output_file)


def main():
    t_start = time.time()
    dt = datetime.datetime.fromtimestamp(t_start).strftime('%Y%m%d_%H%M')
    desktop = os.path.join(os.path.expanduser("~"), 'Desktop')
    path = desktop + "/News/"
    preprocess(path, desktop + "/word_" + dt + ".json")
    t_stop = time.time()
    print("---Total cost %s seconds---" % round(t_stop - t_start, 2))


if __name__ == '__main__':
    main()
