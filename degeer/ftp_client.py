"""
This is the FTP Client Interface whish can connect  to the FTP directory
and dump all file into the dump_directory.This class take an DUMP_DIRECTORY
DIR.Wait this file take only the files stored  today



Ce script est un script qui permet de copier depuis un dossier FTP dans
un dossier local,il prend comme parametre le dossier a copier dans le
FTP et le cossier en local.Attendez ce script ne copie que les fichiers du jour
qui ont ete deposer dans le FTP ,et non de la veille.

Tu  peux faire tout ce qu tu veux avec, regarde ftplib.py de python
pour les autres fonctions  a implementer, pour le moment ce script
me suffit moi, pour le peu de chose que nous faisons


"""
__author__ = 'Alioun Dia'
__version__= "0.1"
__date__   = '2012-02-10'


from datetime import datetime
from datetime import date
from ftplib import FTP
import os, sys, os.path

FTP_HOST  = '0.0.0.0'
FTP_USER  = 'X_02'
FTP_PASSWORD = 'Y_02'
class FTP2Local(object):
      def __init__(self, REMOTE_DIR = 'SEN_A_PCCI/DEJA_REABO', LOCAL_DIR = 'SEN_A_PCCI/DEJA_REABO',
                   *args, **kwargs):
        

            self.ftp = FTP(FTP_HOST, FTP_USER, FTP_PASSWORD)
            self.ftp.cwd(REMOTE_DIR)
            self.filenames = []
            self.FTP_HOST     =FTP_HOST
            self.FTP_PASSWORD =FTP_PASSWORD
            self.REMOTE_DIR   =REMOTE_DIR
            self.LOCAL_DIR    =LOCAL_DIR 
            
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
                                      int(month), int(day))
                        # chech the datetime when the file was last  modified
                        today =  date.today()
                        # copie only the files received today
                        if dt.date() >= today:
                           fi = open('%s/%s'%(self.LOCAL_DIR,f) , 'wb')
                           self.ftp.retrbinary('RETR ' + f, fi.write)
                           fi.close()
            self.ftp.close()

# Test Script
def test():
    ftp= FTP2Local()
    ftp.extract_file()


# ROOT DIRS

FTP_DIR  = [
'SEN_A_PCCI/DEJA_REABO',
'SEN_A_PCCI/TMK',
'COSA_A_PCCI/DEJA_REABO',
'COSA_A_PCCI/TMK'
]

def run():
    '''
    S'execute dans un thread, veille a ce que ce script
    ne s'arrete jamais
    '''
    import  time
    while True:
        try:
            for remote in FTP_DIR:
                ftp= FTP2Local(REMOTE_DIR= remote ,LOCAL_DIR =remote)
                ftp.extract_file()
            print  'Nous attendons quelques instants avant de copier a nouveau'
            time.sleep(60)
                      
        except KeyboardInterrupt:
             print  'STOPPING FTP2Local'
             break


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
            # Le script est arrete, il faut arreter tous les
            # threads qui sont entrain de tourner
            break
        
   t._Thread__stopped=True
   print  'ARRET  COPIE DU FTP VERS LOCAL'
   
   



   



    
