"""
Deja Reabonne Handler , ce fichier permet de prendre
un fichier deja reabonne quelquonque, de le lire , ensuite
il va checker les correspondance avec la base de donnees
si il y'a des fiches avec un stat (N, R) donc en mesure de monter
il va mettre leurs stats a (X) ie comme etant des  fiches  deja
reabonnees, ensuite il va mettre les donnes dans la table
DEJA_REABONNES, et enfin il va renommer le fichier En
Fichier_Traites.XLS et il envoi un email
"""
import os
import time
import xlrd

# Pour les fichier du Senegal, mauvais fichier
# C'est un fichier HTML
import re
import csv
RE     = re.compile('<td>(.*)</td>')
from datetime import datetime, date
TODAY  =datetime.now().strftime('%Y-%m-%d')
import send

from  DB import DB
class DejaReaboHandler(object):
    def __init__(self,sql_main, sql_deja_rea, root ='SEN_A_PCCI\DEJA_REABO'):
        '''
        Il prends en parametre le fichier <sql_main> , le fichier
        pemettrant de faire Update dans la table Main
        Update main set main_called_old = main_called , main_called =X
        Where main_numeroabonne like '%AEE_RECURE%'
        and main_numeroabonne = ''
        En deusieme lieu cette classe va prendre comme parametre le
        fichier sql_deja_rea
        Insert into Deja_reabonne(value1, value2, value3)
        values()
        root est le repertoire vers le quel il va localiser le
        ficier a traiter 
        '''

        self.sql_main =sql_main
        self.sql_deja_rea = sql_deja_rea
        self.root  = root
        self.map   = {}

    def handle_senegal(self):
        '''
        Starting to handle  root dir 
        '''
        print  'HANDLING  (%s) DIR  '%self.root
        print  '\n\t'
        print  '-'*40
        while True:
            self.TODAY     =datetime.now().strftime('%Y-%m-%d')
            self.dat_form  =date.today().strftime('%Y%m01')
            try:
                # Un dictionnaire qui contient un nom de fichiers
                # et les lines de ce fiochier
                for root_, dir, files in os.walk(self.root):
                    for f in  files:
                        if '_Traite_' in f :
                              continue
                        f_name  = '%s/%s'%(root_, f)
                        try:
                          f  = open(f_name, 'r+')

                        except :
                          #ce fichier est utilise par un  autre processus , ce n'est
                          #pas grave on attends le prochain call
                          raise
                          time.sleep(10)
                          continue

                        # par mesure de precaution
                        # si le fichier est deja traite

                        name, ext = os.path.splitext(f_name)
                        file_tr   ='%s_Traite_%s.XLS'%(name, ext)
                        if os.path.exists(file_tr):
                           # Si le fichier traite existant n'a pas ete depose/modifie ce
                           # jour alors il faut le supprimer pour permettre au systeme
                           # de prendre en compte le fichier qui qui a ete depose
                           # pour le senegal et qui le remplace, cela evite le
                           # probleme du fichier de meme nom envoye sur plusieurs
                           # jours differents et qui contient des donnees differentes

                           dat_mod = datetime.fromtimestamp(
                                             os.stat(file_tr).st_mtime)
                           to_day = date.today()

                           # si aujourdhui n'est pas superieurs a la date
                           # de mofication du fichier traite alors supprime
                           # le fichier
                           if  to_day > dat_mod.date():
                              try:
                                  os.remove(file_tr)
                              except:
                                  pass
                           else :
                                continue
                                 
                        row  =[]
                        rows =[]
                        for l in f.readlines():
                          mo =RE.match(l)
                          if mo:
                             row.append(mo.group(1))
                          if len(row)==18:
                             main_fichier = f_name.split('/')[-1]

                             # Initialise Main_Tel comme TelepHone a domicile
                             main_tel     = row[10]

                             if (len(main_tel)!= 9) or (not main_tel.isdigit()):
                                # bureau
                                main_tel = row[12]
                                if (len(main_tel)!= 9) or (not main_tel.isdigit()):
                                    # portable 1
                                    main_tel = row[8]
                                    if (len(main_tel)!= 9) or (not main_tel.isdigit()):
                                          # portable 9
                                          main_tel = row[9]
                             # Si le Telephone ne correspond toujours pas au
                             #Senegal, nous mettons donc row[10] , telephone a domicile
                             if (len(main_tel)!= 9) or (not main_tel.isdigit()):
                                  main_tel     =\
                                    row[10] if len(row[10])<=9 else row[10][:9]

                             if 'SEN_AS_AAE_RECUR' in main_fichier:
                                  main_fichier =\
                                    'CANAL_PA_ARRIVANT_A_ECHEANCE_SENEGAL_1M_%s'\
                                    %self.dat_form  
                             if 'SEN_AS_EF_RECUR'  in main_fichier:
                                  main_fichier =\
                                    'CANAL_OA_ECHUS_FRAIS_SENEGAL_%s'\
                                    %self.dat_form 
                            
                             row.extend([f_name.split('/')[-1], self.TODAY,
                                         main_fichier,
                                         '00221'+ (main_tel or '')])
                             rows.append(row)
                             row= []
                        f.close()
                        try:
                          os.rename(f_name, '%s_Traite_%s.XLS'%(name,ext))
                        except OSError, e:
                          pass
                        else:
                          for row in rows:
                              print row[-1],',', row[-2], '\n'
                              #row[-2], row[-3], row[-4]

                          #time.sleep(10)
                          self.map[f_name] =  rows
                          
                          # output date and  filename
                          #self.flush()
                          self.update_main_from_file_senegal()
                          self.map={}
                          time.sleep(20)
                    # End for
                # End for
                 
            except KeyboardInterrupt:
                print  'STPPING HANDLING (%s) DIR '%self.root
                break
            except:
                pass

        print 'STOPPED HANDLING (%s) DIR '%self.root

    def handle_afr(self):
        '''
        Starting to handle  root dir 
        '''
        print  'HANDLING  (%s) DIR  '%self.root
        print  '\n\t'
        print  '-'*40
        while True:
         self.TODAY     =datetime.now().strftime('%Y-%m-%d')
         self.dat_form  =date.today().strftime('%Y%m01')
         try:
             # Un dictionnaire qui contient un nom de fichiers
             # et les lines de ce fiochier
             for root_, dir, files in os.walk(self.root):
                  for f in  files:
                      if '_Traite_' in f :
                         continue
                                
                      f_name  = '%s/%s'%(root_, f)
                      try:
                          f  = open(f_name, 'r+')

                      except :
                          #ce fichier est utilise par un  autre processus , ce n'est
                          #pas grave on attends le prochain call
                          raise
                          time.sleep(10)
                          continue

                      # par mesure de precaution
                      # si le fichier est deja traite
                          
                      name, ext = os.path.splitext(f_name)
                      file_tr   ='%s_Traite_%s.CSV'%(name, ext)
                      if os.path.exists(file_tr):
                         # Si le fichier traite existant n'a pas ete depose/modifie ce
                         # jour alors il faut le supprimer pour permettre au systeme
                         # de prendre en compte le fichier qui qui a ete depose
                         # pour le senegal et qui le remplace, cela evite le
                         # probleme du fichier de meme nom envoye sur plusieurs
                         # jours differents et qui contient des donnees differentes

                         dat_mod = datetime.fromtimestamp(
                                           os.stat(file_tr).st_mtime)
                         to_day = date.today()

                         # Si le fichier traite n'as pas ete traite ce jour (Aujourdhui)
                         # alors le supprimer, sinon il faut supprime le fichier
                         # telecharge
                         if to_day > dat_mod.date():
                            try:
                                os.remove(file_tr)
                            except:
                               pass
                         else:
                            continue       
                      row  =[]
                      rows =[]
                      #print f
                      rd  =csv.reader(f, dialect = csv.excel,
                           delimiter = ';')
                      for row in rd:
                             main_fichier = f_name.split('/')[-1]                                        
                             row.extend([f_name.split('/')[-1],
                                         self.TODAY, main_fichier,
                                         row[13] if len(row[13])<=15 \
                                         else row[13][:15]])
                             rows.append(row)
                      f.close()
                      try:
                          os.rename(f_name , '%s_Traite_%s.CSV'%(name,ext))
                      except OSError, e:
                          pass
                      else:
                          for row in rows:
                              #print row[-1],',', row[-2],  ',' ,row[-3],  ',', row[-4]
                              pass
                          self.map[f_name] =  rows
                          #output date and  filename
                          #self.flush()
                          self.update_main_from_file_afr()
                          self.map={}
                          time.sleep(10)
         except KeyboardInterrupt:
            print  'STPPING HANDLING (%s) DIR '%self.root
            break
         except:
            pass
                   
    def flush(self):
        ''' Retrun un dictinnaire that contain name and file data'''
        for f_name, data in self.map.items():
            print  'FILE :', file
            print '-'*40
          
    def update_main_from_file_afr(self):
      try:
        query1, query2 = self.sql_main.split(';')
        query1 = open(query1 ,'r').read()
        query2 = open(query2,'r').read()
        abo_for_query =','.join(self.num_abo_list)
        query2 = query2.replace( '#', '(' + abo_for_query + ')')
        while  True:
            try:
                  db = DB()
            except:
                  time.sleep(10)
            else:
                 break
        db.do_query(query2)
        # Les reanonnes AFR
        db.excute_many_for_afr2(query1, self.map)

      except:
        import sys
        print sys.exc_info()

      else:
        # send an email
        while True:
            try:
                body  ="""
        Bonjour,</br>
        Les fichiers suivants on etes traites comme des clients deja reabonnes.</br>
        Les clients y figurant ne seront plus contactes par le systeme.</br>
        %s</br>
        Pour un volume de (%s) lignes  recues </br> 
        Cordialement</br>
        Service Informatique</br>
                        """%("</br>".join(self.map.keys() or []),
                             len(self.num_abo_list)
                             )
                       
                
                to = [ 'eyfaye@pcci.sn',
                       'smgning@pcci.sn',
                       'magoudiaby@pcci.sn',
                       'andoye@pcci.sn',
                       'tsenghor@pcci.sn',
                       'adia@pcci.sn',
                       'kmbaye@pcci.sn',
                       'rlaoualy@pcci.sn',
                       'cmbacke@pcci.sn',
                       'sdial@pcci.sn']
                """
                to = ['adia@pcci.sn',
                     'dia.aliounes@gmail.com']
                """
                subject="[PCCI] Deja reabonnes du %s """%self.TODAY     
                send.send(att_path = None , body =body,
                                  to = to , subject =subject)
            except:
                # Outlook a des preoblemes mais nous devons absolument
                # envoyer l'email puisque le chargement a deja eu lieu
                time.sleep(10)
            else:
                # L'email est partie tout va bien
                break

    def update_main_from_file_senegal(self):
      try:
        query1, query2 = self.sql_main.split(';')
        query1 = open(query1 ,'r').read()
        query2 = open(query2,'r').read()
        abo_for_query =','.join(self.num_abo_list)
        query2 = query2.replace( '#', '(' + abo_for_query + ')')
        while  True:
            try:
                  db = DB()
            except:
                  time.sleep(10)
            else:
                 break
        db.do_query(query2)
        db.excute_many_for_senegal(
            query1, self.map)
      
      except:
        import sys
        print sys.exc_info()

      else:
          # send an email
          while True:
                try:
                    body  ="""
        Bonjour,</br>
        Les fichiers suivants on etes traites comme des clients deja reabonnes.</br>
        Les clients y figurant ne seront plus contactes par le systeme.</br>
        %s</br>
        Pour un volume de (%s) lignes  recues </br> 
        Cordialement</br>
        Service Informatique</br>
                            """%("</br>".join(self.map.keys() or []),
                                 len(self.num_abo_list)
                                 )
                           
                    
                    to = [ 'eyfaye@pcci.sn',
                           'smgning@pcci.sn',
                           'magoudiaby@pcci.sn',
                           'andoye@pcci.sn',
                           'tsenghor@pcci.sn',
                           'adia@pcci.sn',
                           'kmbaye@pcci.sn',
                           'rlaoualy@pcci.sn',
                           'cmbacke@pcci.sn',
                           'sdial@pcci.sn']
                    """
                    to = ['adia@pcci.sn',
                         'dia.aliounes@gmail.com']
                    """
                    subject="[PCCI] Deja reabonnes du %s """%self.TODAY     
                    send.send(att_path = None , body =body,
                                      to = to , subject =subject)
                except:
                    # Outlook a des preoblemes mais nous devons absolument
                    # envoyer l'email puisque le chargement a deja eu lieu
                    time.sleep(10)
                else:
                    # L'email est partie tout va bien
                    break

    @property        
    def num_abo_list(self):
        out  =[]
        for file ,rows in  self.map.items():
         #print file
         for  row in rows:
              #print row
              if row[0].isdigit():
                  out.append("'"+row[0]+ "'")
              print  '-'*40

        print  'NUM ABO'
        print  '-'*40
        return out

def test_senegal():
    d =DejaReaboHandler(
    sql_main ='MAIN_DEJA_REABONNES_SENEGAL__.sql;MAIN_DEJA_REABONNES_SENEGAL.sql' ,
    sql_deja_rea =None)
    d.handle_senegal()
def test_afr():
    d =DejaReaboHandler(
        sql_main ='MAIN_DEJA_REABONNES_AFR__.sql;MAIN_DEJA_REABONNES_SENEGAL.sql' ,
        sql_deja_rea =None,
        root ='COSA_A_PCCI\DEJA_REABO'
                        )
    d.handle_afr()


if __name__=='__main__':
   '''
   '''
   import  time
   import threading
   t1=  threading.Thread(
         target = DejaReaboHandler(
         sql_main  ='MAIN_DEJA_REABONNES_SENEGAL__.sql;MAIN_DEJA_REABONNES_SENEGAL.sql',
         sql_deja_rea =None ,
               root ='SEN_A_PCCI\DEJA_REABO').handle_senegal)
   
   t2=  threading.Thread(
        target  = DejaReaboHandler(
        sql_main ='MAIN_DEJA_REABONNES_AFR__.sql;MAIN_DEJA_REABONNES_AFR.sql',
        root ='COSA_A_PCCI\DEJA_REABO',
        sql_deja_rea =None).handle_afr)

   
   print  'DEMARAGE ARRET  COPIE DU FTP VERS LOCAL '
   t1.start()
   t1.deamon =True
   time.sleep(10)
   t2.deamon =True
   t2.start()

   while True:
       #print >> sys.stdout, 'WAIT'
       try:
            time.sleep(10)
       except KeyboardInterrupt, e:
            # Le script est arrete, il faut arreter tous les
            # threads qui sont entrain de tourner
            break
        
   t1._Thread__stopped=True
   t2._Thread__stopped=True
   print  'ARRET  COPIE DU FTP VERS LOCAL'
   

    

        

