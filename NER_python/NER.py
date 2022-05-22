# -*- coding: utf-8 -*-

from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsSyntaxParser,
    NewsNERTagger,
    NamesExtractor,
)
from elasticsearch import Elasticsearch

es = Elasticsearch('http://127.0.0.1:9200')
es.info()
if es.ping():
    print('Yay Connect')
else:
    print('Awww it could not connect!')

segmenter = Segmenter()
morph_vocab = MorphVocab()

emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
syntax_parser = NewsSyntaxParser(emb)
ner_tagger = NewsNERTagger(emb)

names_extractor = NamesExtractor(morph_vocab)
res = es.search(index="crawler", body={"query":{"match_all":{}},"size":1000})
for hit in res['hits']['hits']:
    markup = ner_tagger("%(TEXT)s" % hit["_source"])
    print("%(TITLE)s" % hit["_source"])
    markup.print()
    print('\n'*5)
es.close()
