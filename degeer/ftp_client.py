"""
This is the FTP Client Interface whish can connect
to the FTP directoryand dump all file into the dump_directory.
This class take an DUMP_DIRECTORY DIR.Wait this file take
only the files stored  to day into the FTP directory
Ce script est un script qui permet de copier depuis
un dossier FTP dansun dossier local,il prend comme
parametre le dossier a copier dans le FTP et le cossier
en local.Attendez ce script ne copie que le fichier du jour
qui ont ete deposer dans le FTP ,et non de la veille
"""

from datetime import datetime,timedelta
from datetime import date
import os, sys, os.path
import  time

try:
    from ftplib import FTP
except:
    raise 'This module need ftplib'

FTP_HOST     = 'X'
FTP_USER     = 'X'
FTP_PASSWORD = 'X'

class FTP2Local(object):
    def __init__(self,
               REMOTE_DIR = 'SEN_A_PCCI/DEJA_REABO',
               LOCAL_DIR = 'SEN_A_PCCI/DEJA_REABO',
               *args, **kwargs):
      
        self.ftp = FTP(FTP_HOST, FTP_USER, FTP_PASSWORD)
        self.ftp.cwd(REMOTE_DIR)
        self.ftp.set_debuglevel(2)
        self.ftp.set_pasv(False)
        self.filenames     = []
        self.FTP_HOST      =FTP_HOST
        self.FTP_PASSWORD  =FTP_PASSWORD
        self.REMOTE_DIR    =REMOTE_DIR
        self.LOCAL_DIR     =LOCAL_DIR 
        
    def host(self):
        return  self.FTP_HOST

    def extract_file(self):
        self.ftp.retrlines('NLST', self.filenames.append)
        for f in self.filenames:
            mod = self.ftp.sendcmd("MDTM "+ f)
            if mod:
                if mod[:3] == '213':
                    mod = mod[3:].strip()
                    year, month, day =\
                    mod[:4], mod[4:6], mod[6:8]
                    dt = datetime(int(year),
                                  int(month),
                                  int(day))
                    # chech the datetime when the file
                    # was last  modified
                    today =  date.today()
                    if dt.date() >= today:
                       fi  = '%s/%s'%(self.LOCAL_DIR,f)
                       fi  = open( fi, 'wb')
                       print  
                       print f , today
                       self.ftp.retrbinary('RETR ' + f,
                                           fi.write)
                       fi.close()

        self.ftp.quit()
def test():
    ftp= FTP2Local()
    ftp.extract_file()
    
FTP_DIR  = [
    'SEN_A_PCCI/DEJA_REABO',
    'SEN_A_PCCI/TMK',
    'COSA_A_PCCI/DEJA_REABO',
    'COSA_A_PCCI/TMK'
]

def run():
    '''
    Ce script doit s'excuter toutes les heures
    car en principe j'ignore quand le client depose les
    fichiers dans le FTP
    '''
    while True:
        try:
            for remote in FTP_DIR:
                ftp= FTP2Local(REMOTE_DIR= remote ,
                               LOCAL_DIR =remote)
                ftp.extract_file()
                print  'NOUS ATTENDONS 1 HEURE AVANT DE\
                COPIER A NOUVEAU DEPUIS LE FTP'
                time.sleep(60)
                
        except KeyboardInterrupt:
             print  'STOPPING FTP2Local'
             break   
        except:
             print  'Probleme de connection avec le serveur'
             raise
    # Arret de FTP2Local
    print  'FTP2Local STOPPED'
  
if __name__=='__main__':
   '''
   '''
   import  time
   import threading
   t= threading.Thread(target = run)
   print  'DEMARAGE ARRET  COPIE DU FTP VERS LOCAL '
   t.start()
   while True:
       #print >> sys.stdout, 'WAIT'
       try:
            time.sleep(10)
       except KeyboardInterrupt, e:
            # Le script est arrete, il faut arreter
            #tous les threads qui sont entrain de tourner
            break     
   t._Thread__stopped=True
   print  'ARRET  COPIE DU FTP VERS LOCAL'
   
   



   



    
