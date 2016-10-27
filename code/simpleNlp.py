# -*- coding: utf-8 -*-
import sqlite3
import random
import re
import numpy as np
import jieba
import jieba.posseg as pseg
from gensim import corpora, models, similarities
import cPickle
import os

PREFIX = '../data/'
STOP_WORDS_PATH = PREFIX + 'stopwords.txt'
DB_PATH = PREFIX + 'dragonball_tieba.db'
TOKEN_SENTENCES = PREFIX + 'token_sentences.pkl'
ORIGIN_SENTENCES = PREFIX + 'origin_sentences.pkl'
DICTIONARY = PREFIX + 'dictionary.pkl'
CORPUS = PREFIX + 'corpus.pkl'
LSI = PREFIX + 'lsi.pkl'
LSI_IDXS = PREFIX + 'lsi_idxs.pkl'
TFIDF = PREFIX + 'tfidf.pkl'

jieba.load_userdict(PREFIX + 'dictionary.txt')

def filtSentence(sentence):
    #print sentence
    pat = re.compile(u'[,\.，。﹏-～“”]|<.*?>|%#\s\w+\s;')
    sentence = re.sub(pat, u' ', sentence)
    sentence = re.sub(re.compile('\s+'), ' ', sentence)
    #print sentence
    return sentence

def tokenizeSentence(sentence):
    text = u''
    result = pseg.cut(sentence)
    attrs = [u'nr', u'nz', u'n', u'v', u'vn']
    for r, s in result:
        #if s not in attrs or len(r) < 2:
        #    continue
        text += r + u' '
    return text.split(u' ')

def loadStopWords():
    # get stop words
    stopwords = {}
    print 'Loading stop words'
    words = map(lambda x : x.strip(), open(STOP_WORDS_PATH).readlines())
    signs = [u'…', u'！', u'？', u'↑', u'~', u'-']
    for word in words:
        word = word.decode('gb2312')
        stopwords[word] = 1
    return stopwords

stopwords = loadStopWords()

def processSentence(sentence):
    sentence = tokenizeSentence(filtSentence(sentence))
    sentence = filter(lambda word : word not in stopwords, sentence)
    return sentence

def getSentences():
    conn = sqlite3.connect(DB_PATH)
    sql = 'SELECT DISTINCT(CONTENT) FROM COMMENT'
    origin_sentences = map(lambda x:x[0], conn.execute(sql).fetchall())
    conn.close()
    origin_sentences = map(filtSentence, origin_sentences)
    origin_sentences = filter(lambda x:len(x) < 30 and len(x) > 15, origin_sentences)
    token_sentences = map(tokenizeSentence, origin_sentences)

    # filt stop words
    print 'Filt stop words'
    clear_token_sentences = [filter(lambda word : word not in stopwords, sentence)
                             for sentence in token_sentences]

    # filt low frequency word
    print 'Filt low frequency words'
    frequency = {}
    for sentence in clear_token_sentences:
        s = set(sentence)
        for word in s:
            if word not in frequency:
                frequency[word] = sentence.count(word)
            else:
                frequency[word] += sentence.count(word)
                
    final_sentences = [filter(lambda word : word in frequency and frequency[word] > 1, sentence)
                       for sentence in clear_token_sentences]

    return final_sentences, origin_sentences

def generateModels(sentences):
    dictionary = corpora.Dictionary(sentences)
    print 'Build corpus'
    corpus = [dictionary.doc2bow(sentence) for sentence in sentences]
    print 'Build tfidf model'
    tfidf = models.TfidfModel(corpus)
    print 'Build tfidf index'
    tfidf_idxs = similarities.MatrixSimilarity(tfidf[corpus], num_best = 10)
    print 'Build lsi model'
    lsi = models.lsimodel.LsiModel(tfidf[corpus], id2word=dictionary)
    print 'Build lsi index'
    lsi_idxs = similarities.MatrixSimilarity(lsi[corpus], num_best = 10)
    return dictionary, corpus, tfidf, tfidf_idxs, lsi, lsi_idxs

    #lsi = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=10)

if __name__ == '__main__':
    conn = sqlite3.connect(DB_PATH)

    if not os.path.exists(TFIDF):
        tksents, origin_sentences = getSentences()
        cPickle.dump(tksents, open(TOKEN_SENTENCES,'w'))
        cPickle.dump(origin_sentences, open(ORIGIN_SENTENCES,'w'))
        models = generateModels(tksents)
        dictionary, corpus, tfidf, tfidf_idxs, lsi, lsi_idxs = models
        cPickle.dump(dictionary, open(DICTIONARY,'w'))
        cPickle.dump(corpus, open(CORPUS,'w'))
        cPickle.dump(lsi, open(LSI,'w'))
        cPickle.dump(lsi_idxs, open(LSI_IDXS,'w'))
        cPickle.dump(tfidf, open(TFIDF,'w'))
    else:
        tksents = cPickle.load(open(TOKEN_SENTENCES))
        origin_sentences = cPickle.load(open(ORIGIN_SENTENCES))
        dictionary = cPickle.load(open(DICTIONARY))
        corpus = cPickle.load(open(CORPUS))
        lsi = cPickle.load(open(LSI))
        lsi_idxs = cPickle.load(open(LSI_IDXS))
        tfidf = cPickle.load(open(TFIDF))
        tfidf_idxs = similarities.MatrixSimilarity(tfidf[corpus], num_best = 10)

    while True:
        sentence = raw_input("Input sentence : ")
        sentence = processSentence(sentence.decode('gb2312', 'ignore'))
        bow = dictionary.doc2bow(sentence)
        sims = tfidf_idxs[tfidf[bow]]
        print (u'关键词 ： ' + u' '.join(sentence)).encode('gb2312', 'ignore')
        print '================================'
        for idx, sim in sims:
            print u'\t' + origin_sentences[idx]
