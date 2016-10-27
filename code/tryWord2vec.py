# -*- coding: utf-8 -*-
import gensim.models.word2vec as wv
import codecs
import os

PREFIX = '../data/'
STOP_WORDS_PATH = PREFIX + 'stopwords.txt'
DE_TEXT_PATH = PREFIX + 'de_text.txt'
DE_MODEL_PATH = PREFIX + 'de_model.txt'
TEXT_PATH = PREFIX + 'text.txt'
MODEL_PATH = PREFIX + 'model.txt'

def deleteStopWords(stopword_file, text_file, de_text_file):
    words = open(stopword_file).readlines()
    text = (open(text_file).readline()).decode('utf8', 'ignore')
    for word in words:
        text = text.replace(u' ' + word.strip().decode('utf8', 'ignore') + u' ', u' ')
    f = open(DE_TEXT_PATH, 'w')
    f.write(text.encode('utf8', 'ignore'))
    f.close()

def generateModel(text_file, save_name):
    sentence = wv.Text8Corpus(text_file)
    model = wv.Word2Vec(sentence)
    model.save(save_name)
    return model

def reGenerate():
    deleteStopWords(STOP_WORDS_PATH, TEXT_PATH, DE_TEXT_PATH)
    de_model = generateModel(DE_TEXT_PATH, DE_MODEL_PATH)
    # model = generateModel(TEXT_PATH, 'model.txt')

if __name__ == '__main__':
    if not os.path.exists(DE_MODEL_PATH):
        reGenerate()
    de_model = wv.Word2Vec.load(DE_MODEL_PATH)

    while True:
        names = raw_input("Input words : ").decode('gb2312', 'ignore').split(u' ')
        try:
            de_result = de_model.most_similar(names)
            for i in range(len(de_result)):
                print de_result[i][0].encode('gb2312', 'ignore')
            print '\n'
        except:
            print 'Word is not exist in model'
