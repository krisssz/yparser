# -*- coding: utf-8 -*-
"""
Created on Tue Feb 17 20:56:43 2015

@author: Gábor & Krisz
"""

import os, csv, time
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas.io.html import read_html


class Ticker:
	
	def __init__(self, ticker, date=None):
		self.ticker = ticker
		self.YAHOO_ENDPOINT = 'http://finance.yahoo.com/q/'
		self.PAGE_URLS = {
			'profile':			'{}pr'.format(self.YAHOO_ENDPOINT),
			'major_holders':		'{}mh'.format(self.YAHOO_ENDPOINT),
			'insider_transactions': '{}it'.format(self.YAHOO_ENDPOINT),
			'key_statistics':	   '{}ks'.format(self.YAHOO_ENDPOINT),
			# 'headline':			 '{}h'.format(self.YAHOO_ENDPOINT),
			'competitors':		  '{}co'.format(self.YAHOO_ENDPOINT),
		}
		self.text = dict()
		self.data = dict()
		for name in self.PAGE_URLS:
			self.data[name] = dict()
		
		self.params = {'s':ticker}
		if date:
			self.params['t'] = date
	
	def get(self, page_type):
		assert page_type in self.PAGE_URLS.keys()
		url = self.PAGE_URLS[page_type]
		resp = requests.get(url, params=self.params)
		if resp.status_code != 200:
			self.text[page_type] = ''			
		self.text[page_type] = resp.text
		return self
	
	def get_all(self, parse=True):
		for type in self.PAGE_URLS:
			self.get(type)
			
		if parse:
			self.parse_all()
		
		return self
	
	def parse(self, page_type): # 'page_type' should be a list
		if 'profile' in page_type:
			self.parse_profile()
		
		if 'major_holders' in page_type:
			self.parse_major_holders()
		
		if 'insider_transactions' in page_type:
			self.parse_insider_transactions()
		
		if 'key_statistics' in page_type:
			self.parse_key_statistics()
		
		if 'competitors' in page_type:
			self.parse_competitors()
	
		return self
		
	def parse_all(self):
		list = []
		for type in self.PAGE_URLS:
			list.append(type)
		self.parse(list)
		return self
	
	def parse_profile(self):
		q = bs(self.text['profile'])
		
		list = q.findAll(attrs={"id": "yfs_l84_" + self.ticker.lower()})
		if(list): # ugly
			price = unicode(list[0].findAll(text=True)[0])
		
			list = q.findAll(attrs={"id": "yfs_t53_" + self.ticker.lower()})
			if(list):
				date = unicode(list[0].findAll(text=True)[0])
				
				d = {'price': price, 'date': date, 'parse_date': time.strftime('%y-%m-%d')}
				df = pd.DataFrame(d, index=['0'])
				self.data['profile']['price'] = df
		
		list = q.findAll(attrs={"class": "yfnc_datamodoutline1"})
		
		if list:
			df = read_html(unicode(list[0]), header=None)[1]
			self.data['profile']['details'] = df
		
		if 1 < len(list):
			df = read_html(unicode(list[1]), header=None)[1]
			self.data['profile']['key_executives'] = df
			
		return self
	
	def parse_major_holders(self):
		q = bs(self.text['major_holders'])
		
		list = q.findAll(attrs={"id": "yfi_holders_breakdown"})
		if list:
			df = read_html(unicode(list[0]), header=None)[1] # A visszaadott df lista masodik eleme hasznos
			self.data['major_holders']['breakdown'] = df
		
		list = q.findAll(attrs={"class": "yfnc_tableout1"})
		if list:
			df = read_html(unicode(list[0]), header=None)[1]
			self.data['major_holders']['persons'] = df
		
		if list:
			df = read_html(unicode(list[1]), header=None)[1]
			self.data['major_holders']['institutions'] = df
		
		if list:
			df = read_html(unicode(list[2]), header=None)[1]
			self.data['major_holders']['funds'] = df
		
		return self

	def parse_insider_transactions(self):
		q = bs(self.text['insider_transactions'])
		
		sups = q.findAll('sup') # <sup> tag remover
		for sup in sups:
			sup.extract()
		
		list = q.findAll(attrs={"class": "yfnc_tableout1"})
		if list:
			df = read_html(unicode(list[len(list)-1]), header=None)[1]
			self.data['insider_transactions']['insider_transactions'] = df
			
		return self
		
	def parse_key_statistics(self):
		q = bs(self.text['key_statistics'])
		
		list = q.findAll(attrs={"class": "yfnc_datamodoutline1"})
		
		sups = q.findAll('sup') # <sup> tag remover
		for sup in sups:
			sup.extract()
		
		heads = q.findAll(attrs={"colspan": "2"}) # Header remover
		for head in heads:
			head.extract()
		
		if list:
			df = read_html(unicode(list[0]), header=None)[1]
			
			for i in range (1, 10):
				df_tmp = read_html(unicode(list[i]), header=None)[1]
				df = pd.concat([df, df_tmp])
			
			self.data['key_statistics']['key_statistics'] = df.transpose()
		
		return self
	
	def parse_competitors(self):
		q = bs(self.text['competitors'])
		
		list = q.findAll(attrs={"class": "yfnc_datamodoutline1"})
		if list:
			df = read_html(unicode(list[0]), header=None)[1]
			self.data['competitors']['competitors'] = df
			
		return self
	
	def as_df(self, header=True): # deprecated function
		self.df = read_html(unicode(self.table), header=header)
		return self

	def to_csv(self, path, page_type): # page_type should be a list
		for type in page_type:
			for d in self.data[type]:
				self.data[type][d].to_csv('%s\\%s_%s.csv' % (path, self.ticker, d), header=True if(d == 'price') else False , encoding='utf-8')
		
		return self
		
# ####
# Main
# ####

path = '\\'.join([os.getcwd(),time.strftime('%y-%m-%d')])
if not os.path.exists(path): os.makedirs(path)

tickersFile = csv.reader(open('e:\BME\Szakdolgozat\Yahoo Ticker Symbols - Jan 2015.csv')) # origin: http://investexcel.net/all-yahoo-finance-stock-tickers/
tickersFile.next() # Fejléc eldobása

CNT = 0 # 0 ready
CNT_START = 0
CNT_MAX = 100
for t in tickersFile:
	if CNT == CNT_START + CNT_MAX:
		break
	
	if CNT_START <= CNT:
		ticker = Ticker(t[0])
		ticker.get_all()
		ticker.to_csv(path,['profile','major_holders','insider_transactions','key_statistics','competitors'])
		print(CNT)

	CNT += 1
