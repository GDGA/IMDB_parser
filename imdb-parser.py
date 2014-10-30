#!/usr/bin/python
import threading, urllib2, urllib
import sys
import time
import codecs
import re
import os
import MySQLdb
from pprint import pprint
from bs4 import BeautifulSoup, NavigableString
from optparse import OptionParser


use = "Usage: %prog -u url -l limit\n This script parses justeat site and outputs csv file of the following format :\n "

parser = OptionParser(usage = use)
parser.add_option("-g", "--genre", action='store', type="string", dest="genre", default=None, help="Just provide genre like action/thriller etc.")
parser.add_option("-v", "--verbose", action='store_true', dest="verbose", default=False, help="show progress")
parser.add_option("-s", "--start", action='store', type="int", dest="start", default=1, help="start from")
parser.add_option("-l", "--last", action='store', type="int", dest="start", default=1, help="last number")
parser.add_option("-t", "--threads", action='store', type="int", dest="thread", default=1, help="Threads to run")
(opts, args) = parser.parse_args()

_db= None
def connectDB():
    global _cursor
    global _db
    _db = MySQLdb.connect(host="localhost", # your host, usually localhost
                            user="root", # your username
                          passwd="ubuntu", # your password
                          db="imdb_data") # name of the data base
    
    if _db is not None:
        print "connected"
    else:
        print "unable to connect"

def stripslashes(s):
    if type(s) is str:
        s=codecs.decode( s.encode('utf-8'), 'string_escape')
        s=s.decode('utf-8').replace("'","")
    else:
        s= unicode(s)
    s= ''.join([i if ord(i) < 128 else ' ' for i in s])
    return s.replace(u'\xeb',"")
      
def urlopen_with_retry(request,attr):
   retries= t_retries=5
   
   while retries>0:
      try:
       if retries< t_retries:
         print  "Retrying... \n"
       doc= urllib2.urlopen(request).read()
       return doc;
      except urllib2.URLError,e:
         print "Error reading page "+attr+" ."
         retries= retries-1
     


class Unbuffered:
   def __init__(self, stream):
       self.stream = stream
   def write(self, data):
       self.stream.write(data)
       self.stream.flush()
   def __getattr__(self, attr):
       return getattr(self.stream, attr)


def writeToFile(arr,filename):
    global _file
    _dir= os.path.dirname(os.path.dirname(filename))
    dir_= filename[len(_dir)+1:].split("/");
    dir_= [x for x in dir_ if x != ""]
    for part in dir_[:-1]:
      _dir= _dir+"/"+part
      try:
        os.stat(_dir)
      except:
        os.mkdir(_dir)
    _file= open(filename, 'a+')
    _file.write('"'+'";"'.join(arr)+'"\n')

def parse_genrepage(genre,count):
    global _db
    _cursor= _db.cursor()
    print("Parsing genere: "+genre+"("+str(count)+") ....\n")
    Url= "http://www.imdb.com/search/title?genres="+genre.lower()+"&start="+str(count)+"&count=100"
    headers = {'User-Agent' : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.66 Safari/537.36'}
    request = urllib2.Request(Url, None, headers)
    html_doc =  urlopen_with_retry(request,Url)
    if html_doc is not None:
         soup = BeautifulSoup(html_doc)
         childs= soup.find("table",attrs={"class": "results"}).find_all("tr")
         for tr in childs:
           if tr.find("td") is not None:
                try:
                    titleEl= tr.find("td",attrs={"class":"title"})
                    title= titleEl.a.string
                    url= "http://www.imdb.com"+titleEl.a["href"];
                    year_= re.findall(r'[0-9]{4}', titleEl.find("span",attrs={"class":"year_type"}).string.strip("(").strip(")") )
                    if len(year_)>0:
                        year= year_[0]
                    else:
                        year=0000
                    
                    tv= re.findall(r'TV', titleEl.find("span",attrs={"class":"year_type"}).string.strip("(").strip(")") )
                    if len(tv)>0:
                        type_= "TV Series"
                    else:
                        type_= "Movie"
                    rating = titleEl.find("div",attrs={"class":"rating"})
                    
                    if rating is not None and rating["title"] is not None:
                       rating=rating["title"].replace(",","").replace(".","")
                       rating= map(int, re.findall(r'[0-9]+', rating));
                       if len(rating)!=0:
                           rating_count= rating[2];
                           rating= float(float(rating[0])/float(rating[1]));
                       else:
                           rating=0
                           rating_count=0
                    else:
                       rating=0
                       rating_count=0
                    if titleEl.find("span",attrs={"class":"runtime"}) is not None:
                       runtime= titleEl.find("span",attrs={"class":"runtime"}).string
                    else:
                       runtime=""
                    if titleEl.find("span",attrs={"class":"outline"}) is not None:
                       outline= ("".join(titleEl.find("span",attrs={"class":"outline"}).findAll(text=True))).replace("\"","'")
                    else:
                       outline=""
                    if titleEl.find("span",attrs={"class":"credit"}) is not None:
                       credit= ("".join(titleEl.find("span",attrs={"class":"credit"}).findAll(text=True))).replace("\"","'")
                    else:
                       credit=""
                    if titleEl.find("span",attrs={"class":"genre"}) is not None:
                       genre_= "".join(titleEl.find("span",attrs={"class":"genre"}).findAll(text=True))
                    else:
                       genre_= ""
                    _cursor.execute("INSERT INTO `movies`(`url`, `title`, `rating`, `rating_count`, `credits`,`outline`, `genre`, `type`, `year`, `runtime`, `timestamp`) VALUES (\""+"\",\"".join([stripslashes(x) for x in  [url,title,rating,rating_count,credit,outline,genre_,type_,year,runtime] ])+"\",CURRENT_TIMESTAMP)")
                    _db.commit()
                    print title;
                except KeyError:
                    print "KeyError"
                except MySQLdb.IntegrityError:
                    flag=1
                except MySQLdb.InterfaceError:
                    connectDB()
                    _cursor= _db.cursor()
                    _db.commit()
                except MySQLdb.OperationalError:
                    connectDB()
                    _cursor= _db.cursor()
                    _cursor.execute("INSERT INTO `movies`(`url`, `title`, `rating`, `rating_count`, `credits`,`outline`, `genre`, `type`, `year`, `runtime`, `timestamp`) VALUES (\""+"\",\"".join([stripslashes(x) for x in  [url,title,rating,rating_count,credit,outline,genre_,type_,year,runtime] ])+"\",CURRENT_TIMESTAMP)")
                    _db.commit()
                    print title;
    else:
         print "Error parsing"
    return count+100
        
def main():
    connectDB()
    if opts.genre is None:
       print "Please specify Zomato URL\n"
       parser.print_help()
       exit(-1)
    else:
        if opts.start:
            count=opts.start
        while True:
            threads=[]
            for i in range(0,opts.thread):
                threads.append(threading.Thread(target=parse_genrepage, args =[opts.genre,count]))
                count= count+100
            for t in threads:
                t.start()
            for t in threads:
                t.join()


if __name__ == "__main__":
   main()