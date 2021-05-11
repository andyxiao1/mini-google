#!/bin/bash
sudo pip install nltk
sudo python -m nltk.downloader -d /usr/lib/nltk_data wordnet
sudo python -m nltk.downloader -d /usr/lib/nltk_data stopwords