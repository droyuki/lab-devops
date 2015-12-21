# coding=utf-8
"""
Create by WeiChen on 2015/12/2
"""
from __future__ import print_function, unicode_literals
import sys
import re
import os
import datetime
import time

sys.path.append("../")
import jieba
import jieba.posseg as pseg


def set_dict(dictionary, user_dict):
    jieba.set_dictionary(dictionary)
    jieba.load_userdict(user_dict)


def gen_dict(folder_path, output_file):
    title_counter = 0
    dict_set = set()
    for filename in os.listdir(folder_path):
        if filename.startswith('.'):
            continue
        os.chdir(folder_path)
        if os.path.isfile(filename):
            title_counter += 1
            # f.write(str(title_counter).encode('utf-8') + ". ".encode('utf-8') +
            # filename.encode('utf-8') + "\n".encode('utf-8'))
            content = open(filename.encode(sys.getfilesystemencoding()), 'r').read()
            words = jieba.cut(content, cut_all=False, HMM=False)
            word_set = set(list(words))
            p = re.compile('[一-龥]')
            for word in word_set:
                if p.match(word) and len(word) > 1:
                    dict_set.add(word)
                    # f.write(word.encode('utf-8') + "\n".encode('utf-8'))
                    # f.write("------\n".encode('utf-8'))
    f = open(output_file, 'a+')
    for word in dict_set:
        f.write(word.encode('utf-8') + "\n".encode('utf-8'))
    f.close()
    print("\nParse %s articles" % title_counter)
    print("Output: %s" % output_file)


def gen_dict_by_article(folder_path, output_file):
    title_counter = 0
    dict_set = set()
    f = open(output_file, 'a+')
    for filename in os.listdir(folder_path):
        if filename.startswith('.'):
            continue
        os.chdir(folder_path)
        if os.path.isfile(filename):
            title_counter += 1
            # f.write(str(title_counter).encode('utf-8') + ". ".encode('utf-8') +
            # filename.encode('utf-8') + "\n".encode('utf-8'))
            content = open(filename.encode(sys.getfilesystemencoding()), 'r').read()
            words = jieba.cut(content, cut_all=False, HMM=False)
            word_set = set(list(words))
            p = re.compile('[一-龥]')
            for word in word_set:
                if p.match(word) and len(word) > 1:
                    f.write(word.encode('utf-8') + "\n".encode('utf-8'))
            f.write(word.encode('utf-8') + "\n".encode('utf-8'))
    f.close()
    print("\nParse %s articles" % title_counter)
    print("Output: %s" % output_file)


def only_cut(folder_path, output_file):
    title_counter = 0
    f = open(output_file, 'a+')
    for filename in os.listdir(folder_path):
        if filename.startswith('.'):
            continue
        os.chdir(folder_path)
        if os.path.isfile(filename):
            title_counter += 1
            content = open(filename.encode(sys.getfilesystemencoding()), 'r').read()
            words = jieba.cut(content, cut_all=False, HMM=False)
            word_list = list(words)
            p = re.compile('[一-龥]')
            for word in word_list:
                if p.match(word) and len(word) > 1:
                    f.write(word.encode('utf-8') + " ".encode('utf-8'))
            f.write("\n".encode('utf-8'))
    f.close()
    print("\nParse %s articles" % title_counter)
    print("Output: %s" % output_file)


def only_extract(path, output_file):
    f = open(output_file, 'a+')
    title_counter = 0
    for filename in os.listdir(path):
        if filename.startswith('.'):
            continue
        os.chdir(path)
        if os.path.isfile(filename):
            title_counter += 1
            content = open(filename.encode(sys.getfilesystemencoding()), 'r').read()
            all_words = pseg.cut(content, HMM=False)
            word_list = list(all_words)
            pattern = re.compile('[一-龥]')
            for word in word_list:
                if pattern.match(word.word) and len(word.word) > 1:
                    f.write(word.word.encode('utf-8') + "/".encode('utf-8') + word.flag.encode('utf-8') + " ".encode('utf-8'))
    f.close()
    print("\nParse %s articles" % title_counter)
    print("Output: %s" % output_file)


def extract(path, output_file):
    f = open(output_file, 'a+')
    dict_set = set()
    title_counter = 0
    for filename in os.listdir(path):
        if filename.startswith('.'):
            continue
        os.chdir(path)
        if os.path.isfile(filename):
            title_counter += 1
            content = open(filename.encode(sys.getfilesystemencoding()), 'r').read()
            all_words = pseg.cut(content, HMM=False)
            word_set = set(list(all_words))
            pattern = re.compile('[一-龥]')
            for word in word_set:
                if pattern.match(word.word) and len(word.word) > 1:
                    dict_set.add(word.word)
                    f.write(word.encode('utf-8') + "\n".encode('utf-8'))
    f.close()
    print("\nParse %s articles" % title_counter)
    print("Output: %s" % output_file)


def find_subject(path, output_file):
    dict_set = set()
    title_counter = 0
    for filename in os.listdir(path):
        if filename.startswith('.'):
            continue
        os.chdir(path)
        if os.path.isfile(filename):
            title_counter += 1
            content = open(filename.encode(sys.getfilesystemencoding()), 'r').read()
            all_words = pseg.cut(content, HMM=False)
            word_set = set(list(all_words))
            pattern = re.compile('[n]')
            for word in word_set:
                if pattern.match(word.flag) and len(word.word) > 1:
                    dict_set.add(word)
    f = open(output_file, 'a+')
    for word in dict_set:
        f.write(word.word.encode('utf-8') + "/".encode('utf-8') + word.flag.encode('utf-8') + "\n".encode('utf-8'))
    f.close()
    print("\nParse %s articles" % title_counter)
    print("Output: %s" % output_file)


def show_hint():
    print("\nUsage: runMe.py <command> <option>\n"
          " commands:\n"
          "  -c          just cutting words.\n"
          "  -n          extract negative words.\n"
          "  -p          extract positive words.\n"
          "  -f          extract finance words.\n"
          "  -s          find subjects"
          "options:\n"
          "  -e          extracting part of speech.\n")


def main(argv):
    desktop = os.path.join(os.path.expanduser("~"), 'Desktop')
    path = desktop + "/News/"
    t_start = time.time()
    dt = datetime.datetime.fromtimestamp(t_start).strftime('%Y%m%d_%H%M')
    if len(argv) < 2:
        show_hint()
        sys.exit()
    elif len(argv) == 2 or len(argv) == 3:
        if argv[1].startswith('-'):
            option = argv[1][1:]  # 取出sys.argv[1]的數值但是忽略掉'-'
            if option == 'p':
                print('Extracting negative words ...')
                set_dict('ntusd-positive.txt', 'ntusd-positive.txt')
                if len(argv) == 3 and argv[2] == "-e":
                    output_file = desktop + u'/新聞正向情緒詞庫(含詞性)_' + dt + '.txt'
                    extract(path, output_file)
                elif len(argv) == 2:
                    output_file = desktop + u'/新聞正向情緒詞庫_' + dt + '.txt'
                    gen_dict(path, output_file)
                else:
                    show_hint()
            elif option == 'n':
                print('Extracting positive words ...')
                set_dict('ntusd-negative.txt', 'ntusd-negative.txt')
                if len(argv) == 3 and argv[2] == "-e":
                    output_file = desktop + u'/新聞負向情緒詞庫(含詞性)_' + dt + '.txt'
                    extract(path, output_file)
                elif len(argv) == 2:
                    output_file = desktop + u'/新聞負向情緒詞庫_' + dt + '.txt'
                    gen_dict(path, output_file)
                else:
                    show_hint()
            elif option == 'f':
                print('Extracting finance words ...')
                set_dict('financeDict.txt', 'financeDict.txt')
                if len(argv) == 3 and argv[2] == "-e":
                    output_file = desktop + u'/新聞財經詞(含詞性)_' + dt + '.txt'
                    extract(path, output_file)
                elif len(argv) == 2:
                    output_file = desktop + u'/新聞財經詞_' + dt + '.txt'
                    gen_dict(path, output_file)
                else:
                    show_hint()
            elif option == 'c':
                print('Extracting part of speech ...')
                set_dict('../extra_dict/dict.txt.big', "total.txt")
                if len(argv) == 3 and argv[2] == "-e":
                    output_file = desktop + u'/新聞斷詞(含詞性)_' + dt + '.txt'
                    extract(path, output_file)
                elif len(argv) == 2:
                    output_file = desktop + u'/新聞斷詞_' + dt + '.txt'
                    gen_dict_by_article(path, output_file)
                else:
                    show_hint()
            elif option == 's':
                print('Finding subjects ...')
                set_dict('../extra_dict/dict.txt.big', "total.txt")
                if len(argv) == 2:
                    output_file = desktop + u'/新聞主詞_' + dt + '.txt'
                    find_subject(path, output_file)
                else:
                    show_hint()
            else:
                show_hint()
    else:
        show_hint()
    t_stop = time.time()
    print("---Total cost %s seconds---" % round(t_stop - t_start, 2))


if __name__ == '__main__':
    main(sys.argv)
