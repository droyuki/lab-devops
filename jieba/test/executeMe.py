# coding=utf-8
from __future__ import print_function, unicode_literals
import sys
import re
import os

sys.path.append("../")
import jieba


def extract(path, output_file):
    f = open(output_file, 'a+')
    for filename in os.listdir(path):
        if filename.startswith('.'):
            continue
        os.chdir(path)
        if os.path.isfile(filename):
            print("Loading: %s" % filename)
            f.write("Loading: ".encode('utf-8') + filename.encode('utf-8') + "\n".encode('utf-8'))
            content = open(filename.encode(sys.getfilesystemencoding()), 'r').read()
            words = jieba.cut(content, cut_all=False, HMM=False)
            word_set = set(list(words))
            p = re.compile('[一-龥]')
            for word in word_set:
                if p.match(word) and len(word) > 2:
                    f.write(word.encode('utf-8') + "\n".encode('utf-8'))
            f.write("------\n".encode('utf-8'))
    print("Output: %s" % output_file)


def show_hint():
    print("Usage: executeMe.py <command>\n"
          "commands:\n"
          "  -n          extract negative words\n"
          "  -p          extract positive words\n"
          "  -f          extract finance words\n")


def main(argv):
    if len(argv) < 2:
        show_hint()
        sys.exit()
    if argv[1].startswith('-'):
        option = argv[1][1:]  # 取出sys.argv[1]的數值但是忽略掉'-'
        if option == 'p':
            print('Extracting negative words ...')
            jieba.set_dictionary('ntusd_positive.big')
            jieba.load_userdict('ntusd-positive.txt')
            path = u"/Users/WeiChen/Project/lab-devops/BigBoost/News/"
            output_file = u'/Users/WeiChen/Desktop/新聞正向情緒_jieba.txt'
            extract(path, output_file)

        elif option == 'n':
            print('Extracting positive words ...')
            jieba.set_dictionary('ntusd_negative.big')
            jieba.load_userdict('ntusd-negative.txt')
            output_file = u'/Users/WeiChen/Desktop/新聞負向情緒_jieba.txt'
            path = u"/Users/WeiChen/Project/lab-devops/BigBoost/News/"
            extract(path, output_file)
        elif option == 'f':
            print('Extracting finance words ...')
            jieba.set_dictionary('financeDict.big')
            jieba.load_userdict('financeDict.txt')
            output_file = u'/Users/WeiChen/Desktop/新聞財經詞_jieba.txt'
            path = u"/Users/WeiChen/Project/lab-devops/BigBoost/News/"
            extract(path, output_file)
        else:
            show_hint()
    else:
        show_hint()


if __name__ == '__main__':
    main(sys.argv)