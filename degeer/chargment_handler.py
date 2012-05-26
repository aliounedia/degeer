""" Deja Reabonne Handler , ce fichier permet de prendre un fichier
deja reabonne quelquonque, de le lire , ensuite il va checker les
correspondance avec la base de donnees si il y'a des fiches avec un
stat (N, R) donc en mesure de monter il va mettre leurs stats a (X) ie
comme etant des  fiches  deja reabonnees, ensuite il va mettre les
donnes dans la table DEJA_REABONNES, et enfin il va renommer le
fichier En Fichier_Traites.XLS et il envoi un email """
import os
import time
import xlrd
from datetime import timedelta

# Pour les fichier du Senegal, mauvais fichier
# C'est un fichier HTML
import re
import csv
RE    = re.compile('<td>(.*)</td>')
from datetime import datetime , date
TODAY  =datetime.now().strftime('%Y-%m-%d')

import send
import traceback
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
      self.root   = root
      self.map    = {}
          
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
                            #ce fichier est utilise par un  autre processus ,
                            #ce n'est
                            #pas grave on attends le prochain call
                            raise
                            time.sleep(10)
                            continue
                        # par mesure de precaution
                        # si le fichier est deja traite
                        
                        name, ext = os.path.splitext(f_name)
                        file_tr   ='%s_Traite_%s.XLS'%(name, ext)
                        if os.path.exists(file_tr):
                                try:
                                    os.remove(f_name)
                                except:
                                    pass
                                continue
                        print  f_name
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
                               # Si le Telephone ne correspond toujours pas au Senegal,
                               #nous mettons donc row[10] , telephone a domicile
                               if (len(main_tel)!= 9) or (not main_tel.isdigit()):
                                    main_tel     = (
                                        row[10] if len(row[10])<=9 else row[10][:9])
                               # Ici nous traitons le fichier AAE recurrents pour le
                               # Senegal avec  toujours un delai de traitement de 5 jours
                               if 'SEN_AS_AAE_RECUR' in main_fichier:
                                   main_fichier = (
                                        'CANAL_PA_ARRIVANT_A_ECHEANCE_SENEGAL_1M_%s'%\
                                        self.dat_form)

                               # Les Echus frais reccuerents pour le Senegal, avec
                               # egalement ici des delais de traitement de 5 jours 
                               if 'SEN_AS_EF_RECUR'  in main_fichier:
                                   main_fichier = (
                                        'CANAL_OA_ECHUS_FRAIS_SENEGAL_%s'%self.dat_form)
                               row.extend([f_name.split('/')[-1],  self.TODAY,
                                           main_fichier, '00221'+ main_tel])

                               # Bloquer les formules Grand Prestige
                               # Pour le moment Mi Ange Demande de bloquer les fichiers Grand
                               # Prestige pour tous les segments
                               if  row[5] and  "GRAND PRESTIGE" in row[5].upper():
                                   row = []
                                   continue
                               rows.append(row)
                               # Fini de traiter une ligne de ce fichier, il faut vider
                               # le tableau qui accuille les lignes en tampon
                               # avant de passer a une nouvelle ligne
                               row= []
                        # Lorque nous terminons de parcourrir un fichier
                        # il faut fermer le fichier avant de passer a un
                        # nouveau
                        f.close()
                        try:
                            os.rename(f_name , '%s_Traite_%s.XLS'%(name,ext))
                        except OSError, e:
                            pass
                        else:
                            for row in rows:
                                print row[-1],',', row[-2], '\n'
                            self.map[f_name] =  rows
                            self.update_main_from_file_senegal()
                            self.map={}
                    time.sleep(60)
                    # End for
                #End for
            except KeyboardInterrupt:
                print  'STPPING HANDLING (%s) DIR '%self.root
                break
            except:
                traceback.print_exc()
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
                          #ce fichier est utilise par un  autre processus ,
                          #ce n'est pas grave on attends le prochain call
                          raise
                          time.sleep(10)
                          continue

                      # par mesure de precaution
                      # si le fichier est deja traite
                          
                      name, ext = os.path.splitext(f_name)
                      file_tr   ='%s_Traite_%s.CSV'%(name, ext)
                      if os.path.exists(file_tr):
                         try:
                             os.remove(f_name)
                         except:
                             pass
                         continue
                      print  f_name
                      row  =[]
                      rows =[]
                      #print f
                      rd  =csv.reader(f, dialect = csv.excel,
                           delimiter = ';')
                      for row in rd:
                           main_fichier = f_name.split('/')[-1]

                           # Initialise Main_Tel comme TelepHone a domicile
                           # Tel Domicile 1
                           main_tel     = row[11]
                           #print row
                           #time.sleep(10)
                           if (len(main_tel)< 11) or (not main_tel.isdigit()):
                              # Tel bureau 1
                              main_tel = row[13]
                              if (len(main_tel)< 11) or (not main_tel.isdigit()):
                                 main_tel = row[15]
                                 if (len(main_tel)< 11) or (not main_tel.isdigit()):
                                      #portable 9
                                      main_tel = row[16]
                           # Si le Telephone ne correspond toujours
                           # pas au Senegal, nous mettons
                           # donc row[10] , telephone a domicile
                           if (len(main_tel)< 11) or (not main_tel.isdigit()):
                                main_tel     =(
                                    row[11] if len(row[11])<=15 else row[11][:15])

                           # Ici nous traitons les fichiers AAE 1 Mois reccurrents
                           # des segments a triter sur un intervalle de 4 jours
                           if 'AFR_AS_AAE1M_RECUR' in main_fichier:
                                if  row[22] in  ('BURKINA FASO'):
                                      main_fichier =\
                                        'CANAL_OA_ARRIVANT_A_ECHEANCE_BURKINA_1MC_%s'\
                                        %self.dat_form
                                elif  row[22] in  ('CAMEROUN'):
                                      main_fichier =\
                                        'CANAL_OA_ARRIVANT_A_ECHEANCE_CAMEROUN_1MC_%s'\
                                        %self.dat_form
                                elif  row[22] in  ('CONGO'):
                                      main_fichier =\
                                        'CANAL_OA_ARRIVANT_A_ECHEANCE_CONGO_1MC_%s'\
                                        %self.dat_form
                                elif  row[22] in  ('REP.DEMOCRATIQUE DU CONGO'):
                                      main_fichier =\
                                        'CANAL_OA_ARRIVANT_A_ECHEANCE_RDC_1MC_%s'\
                                        %self.dat_form
                                      
                                elif  row[22] in  ('GUINEE BISSAU', 'GUINEE EQUATORIALE'):
                                      main_fichier =\
                                        'CANAL_OA_ARRIVANT_A_ECHEANCE_GUINEE_1MC_%s'\
                                        %self.dat_form
                                      
                                else: 
                                      main_fichier =\
                                        'CANAL_OA_ARRIVANT_A_ECHEANCE_AFR_1MC_%s'\
                                        %self.dat_form
                                      
                           # Ici nous traitons les fichiers AAE 1 Mois reccurrents
                           # des segments a triter sur un intervalle de 4 jours          
                           if 'AFR_AS_EF_RECUR'  in main_fichier:
                                if  row[22] in  ('BURKINA FASO'):
                                      main_fichier =\
                                         'CANAL_PA_ECHUS_FRAIS_BURKINA_%s'\
                                         %self.dat_form
                                elif  row[22] in  ('CAMEROUN'):
                                      main_fichier =\
                                         'CANAL_PA_ECHUS_FRAIS_CAMEROUN_%s'\
                                         %self.dat_form
                                elif  row[22] in  ('CONGO'):
                                      main_fichier =\
                                         'CANAL_PA_ECHUS_FRAIS_CONGO_%s'\
                                         %self.dat_form
                                elif  row[22] in  ('REP.DEMOCRATIQUE DU CONGO'):
                                      main_fichier =\
                                         'CANAL_PA_ECHUS_FRAIS_RDC_%s'\
                                         %self.dat_form
                                      
                                elif  row[22] in  ('GUINEE BISSAU', 'GUINEE EQUATORIALE'):
                                      main_fichier =\
                                         'CANAL_PA_ECHUS_FRAIS_GUINEE_%s'\
                                         %self.dat_form
                                else: 
                                      main_fichier =\
                                         'CANAL_PA_ECHUS_FRAIS_AFR_%s'\
                                         %self.dat_form   

                           # Ici nous traitons les fichiers Welcom Call
                           # En principe pour ces fichiers il ny'a pas de
                           # delai de traitment fige
                           if 'AFR_AS_RECRU_RECUR' in main_fichier:
                                      main_fichier = 'CANAL_PA_WELCOME_CALL_%s'\
                                                     %self.dat_form

                           # Canal Plus vient de separer les fichier AFR autres pays
                           # par distributeurs (403,455.....)
                           # Il faut voir si il ne sagit pas de ces types de fichiers
                           # et si c'est le cas il faut les mettres soit dans le segment
                           # CANAL_OA_ARRIVANT_A_ECHEANCE_AFR_1MC
                           # ou CANAL_PA_ECHUS_FRAIS_AFR_20120401
                           _afr_re = "(AFR_AS_EF_|AFR_AS_AAE1M_)[0-9]{3}"
                           mo = re.match(_afr_re,main_fichier)
                           if mo:
                             # il sagit bien de ces types de ficheirs
                             # AFR autres pays recurrents
                             if 'AFR_AS_AAE1M_' in main_fichier:
                                 main_fichier =\
                                            'CANAL_OA_ARRIVANT_A_ECHEANCE_AFR_1MC_%s'\
                                            %self.dat_form
                             else:
                                 main_fichier =\
                                            'CANAL_PA_ECHUS_FRAIS_AFR_%s'\
                                            %self.dat_form   

                          
                           row.extend([f_name.split('/')[-1],
                                       self.TODAY, main_fichier,  main_tel])
                           
                           # Bloquer les formules Grand Prestige
                           # Pour le moment Mi Ange Demande de bloquer
                           #les fichiers Grand Prestige pour tous les segments
                           if  row[6] and  "GRAND PRESTIGE" in row[6].upper():
                               continue
                          
                           rows.append(row)
                           # Ici nous n'avons pas besion de vider la ligne
                           # row= [], car nous faisons  for row  in 

                      # Lorque nous terminons de parcourrir un fichier
                      # il faut fermer le fichier avant de passer a un
                      # nouveau
                      f.close()
                      try:
                          # il faut renomer le fichier  que nous venons de traiter
                          os.rename(f_name ,
                                    '%s_Traite_%s.CSV'%(name,ext))
                      except OSError, e:
                          pass
                      else:
                          for row in rows:
                              print row[-1],',', row[-2], '\n'
                              #row[-2], row[-3], row[-4]

                          self.map[f_name] =  rows
                          self.update_main_from_file_afr()
                          self.map={}
                    time.sleep(60)
                    # End for
                #End of
            except KeyboardInterrupt:
                print  'STPPING HANDLING (%s) DIR '%self.root
                break
            except:
                traceback.print_exc()
                time.sleep(20)
                pass
            
    def flush(self):
        ''' Retrun un dictinnaire that contain name and file data'''
        for f_name, data in self.map.items():
            print  'FILE :', file
            #print data
            print '-'*40
            
    def update_main_from_file_senegal(self):
        try:
            query = open(self.sql_main ,'r').read()
            abo_for_query =','.join(self.num_abo_list)
            query = query.replace( '#', '(' + abo_for_query + ')')
            print '\n'
            print query
            print  '\n'
            while  True:
                try:
                      db = DB()
                except:
                      time.sleep(10)
                else:
                     break
            db.excute_many_for_senegal(
                query, self.map
                )

        except:
            import sys
            print  '='*40, 'ERROR', "="*40
            print sys.exc_info()

        else:
            # send an email
            while True:
                try:
                    body  ="""
            Bonjour,</br>
            Les fichiers suivants on etes traites comme  des fichiers a charger.</br>
            Les clients y figurant sont en  production </br>
            %s</br>
            Pour un volume de (%s) lignes  recues </br>
            Ce volume est reparti comme suit</br>
            %s</br>
            Cordialement</br>
            Service Informatique</br>
                            """%("</br>".join(self.map.keys() or []),
                    len(self.num_abo_list),
                    self.main_fichier
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
                    subject="[PCCI] Chargement fichier du %s """%self.TODAY     
                    send.send(att_path = None , body =body,
                                    to = to , subject =subject)
                except:
                    # Outlook a des preoblemes mais nous devons absolument
                    # envoyer l'email puisque le chargement a deja eu lieu
                    print 'Exception into send Message'
                    traceback.print_exc()
                    time.sleep(30)
                else:
                    # L'email est partie tout va bien
                    break
                
    def update_main_from_file_afr(self):
        try:
            query = open(self.sql_main ,'r').read()
            abo_for_query =','.join(self.num_abo_list)
            query = query.replace('#', '(' + abo_for_query + ')')
            print '\n'
            print query
            print  '\n'
            while  True:
                try:
                      db = DB()
                except:
                      time.sleep(10)
                else:
                     break
            db.excute_many_for_afr(
                query,
                self.map)
        except:
            import sys
            print  '='*40, 'ERROR', "="*40
            print sys.exc_info()

        else:
            # send an email
            while True:
                try:
                    body  ="""
            Bonjour,</br>
            Les fichiers suivants on etes traites comme  des fichiers a charger.</br>
            Les clients y figurant sont en  production </br>
            %s</br>
            Pour un volume de (%s) lignes  recues </br>
            Ce volume est reparti comme suit</br>
            %s</br>
            Cordialement</br>
            Service Informatique</br>
                            """%("</br>".join(self.map.keys() or []),
                                             len(self.num_abo_list),
                                             self.main_fichier
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
                    subject="[PCCI]Chargement fichier du %s """%self.TODAY     
                    send.send(att_path = None , body =body,
                                to = to , subject =subject)
                except:
                    # Outlook a des preoblemes mais nous devons absolument
                    # envoyer l'email puisque le chargement a deja eu lieu
                    print 'Exception into send Message'
                    traceback.print_exc()
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
                  out.append(row[0])
              print  '-'*40

            print  'NUM ABO'
            print  '-'*40
            return out

    @property        
    def main_fichier(self):
        out  ={}
        for file ,rows in  self.map.items():
            for row in rows:
                # Le main fichier
                if not row[0].isdigit():
                   continue
                if row[-2] in out:
                   out[row[-2]] += 1
                else:
                   out[row[-2]] =1
        #Les informations sur le chargement des fichiers
        return '</br>'.join(
           [ '%s ** %s'%(str(k),str(v))
             for (k, v) in out.items() ])
       
def test_senegal():
    d =DejaReaboHandler(sql_main ='MAIN_INSERT_SENEGAL.sql' ,
                        root ='SEN_A_PCCI\TMK',
                        sql_deja_rea =None)
    d.handle_senegal()
def test_afr():
    d =DejaReaboHandler(sql_main ='MAIN_INSERT_AFR.sql' ,
                        sql_deja_rea =None,
                        root ='COSA_A_PCCI\TMK'
                        )
    d.handle_afr()


if __name__=='__main__':
   
   import  time
   import threading
   t1=  threading.Thread(
               target = DejaReaboHandler(
               sql_main ='MAIN_INSERT_SENEGAL.sql' ,
               sql_deja_rea =None ,
               root ='SEN_A_PCCI\TMK').handle_senegal)
   t2=  threading.Thread(
         target  = DejaReaboHandler(
               sql_main ='MAIN_INSERT_AFR.sql' ,
               root ='COSA_A_PCCI\TMK',
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

    

        

