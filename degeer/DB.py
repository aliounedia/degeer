"""
Wrap a Database system
"""
import pyodbc
import time
class DB(object):
    def __init__(self):
        try:
            self.connection = pyodbc.connect(
            '''DRIVER={SQL Server};
               SERVER=X
               DATABASE=X;
               UID=X;
               PWD=X''',
               autocommit =True)
            self.cursor = self.connection.cursor()
            self.close  = self.cursor.close
            self.data   = []
        except pyodbc.OperationalError:
               raise
          
    def do_query(self, qs):
        print  'EXECUTING QUERY '
        self.cursor.execute(qs)
        print  'FINISH QUERY '

    def excute_many_for_senegal(self , query, map):
        '''
        SENEGAL 
        ====
        Insert Into Main(
        Main_Tel,Main_NumeroAbonne ,Main_NumeroContrat ,
        Main_NumeroCarte ,Main_Civ,Main_Nom,
        Main_Prenom,Main_Adr1,Main_Adr2,
        Main_Adr3,Main_Ville,Main_Pays,
        Main_TelDom,Main_TelBur,Main_GSM,
        Main_Tel4, Main_Tel5
        ,Main_Email,Main_Formule,Main_Options,
        Main_DebutAbonnement,
        Main_DateFinAbonnement,
        Main_Duree,Main_Distributeur,
        Main_TelDistributeur,Main_CoordonneesDistributeur,
        Main_Fichier,
        Main_Fichierclient,Main_Called,Main_Locked,
        Main_CodeFinAppel,Main_CodeRefus,main_datechargment)
        values(
        row[-1], row[0],   row[1], row[2],row[3],
        row[4] ,row[5],'', '','','','',
        '','', '','', '', '','', '', '', '','',
        '', '', '', row[-2],row[-4],  'N',0, '', '',row[-3])
        
        '''
        out  =[]
        for file ,rows in  map.items():
            for  row in rows:
                #print row
                if row[0].isdigit():
                  out.append(
                        [ row[-1],
                          row[0],'' ,
                          row[1],
                          row[2],
                          row[3],
                          row[4],
                          row[14],
                          row[15],
                          row[16],
                          row[17],
                          'SENEGAL',
                          row[10],
                          row[11],
                          row[8],
                          row[9],
                          row[12],
                          row[13],
                          row[5] ,'' ,'',
                          row[7],
                          row[6],
                          row[-2],
                          row[-4],
                          'N',
                          0,
                          '',
                          '',
                          row[-3]])
                print  '-'*40
        self.cursor.executemany(query, out)

    def excute_many_for_afr(self , query, map):
        '''
        AFR
        ====
        Insert Into Main(
        Main_Tel,Main_NumeroAbonne ,Main_NumeroContrat ,
        Main_NumeroCarte ,Main_Civ,Main_Nom,
        Main_Prenom,Main_Adr1,Main_Adr2,Main_Adr3,
        Main_Ville,Main_Pays,
        Main_TelDom,Main_TelBur,Main_GSM,Main_Tel4, Main_Tel5
        ,Main_Email,Main_Formule,Main_Options,
        Main_DebutAbonnement,
        Main_DateFinAbonnement,
        Main_Duree,Main_Distributeur,
        Main_TelDistributeur,Main_CoordonneesDistributeur,
        Main_Fichier,
        Main_Fichierclient,Main_Called,Main_Locked,
        Main_CodeFinAppel,Main_CodeRefus,main_datechargment)
        values(
        row[-1], row[0],   row[1], row[2],row[3], row[4] ,
        row[5], row[18], row[19],row[20],row[21],row[22],
        row[11],row[13], row[15],row[16], row[14],
        row[17], row[6],  row[7], row[8], row[9],row[10],
        row[23], row[24], row[25], row[-2],row[-4],
        'N',0, '', '',row[-3])
        '''
        out  =[]
        for file ,rows in  map.items():
            for  row in rows:
              #print row
              if row[0].isdigit():
                  out.append(
                        [ row[-1],
                          row[0],
                          row[1],
                          row[2],
                          row[3],
                          row[4],
                          row[5],
                          row[18],
                          row[19],
                          row[20],
                          row[21],
                          row[22],
                          row[11],
                          row[13],
                          row[15],
                          row[16],
                          row[14],
                          row[17],
                          row[6],
                          row[7],
                          row[8],
                          row[9],
                          row[10],
                          row[23],
                          row[25],
                          row[24],
                          row[-2],
                          row[-4],
                          'N',
                          0,
                          '',
                          '',
                          row[-3]])    
        self.cursor.executemany(query, out)

    def excute_many_for_afr2(self , query, map):
        '''
        AFR
        ====
        Insert Into Main(
        Main_Tel,Main_NumeroAbonne ,Main_NumeroContrat ,
        Main_NumeroCarte ,Main_Civ,Main_Nom,
        Main_Prenom,Main_Adr1,Main_Adr2,
        Main_Adr3,Main_Ville,Main_Pays,
        Main_TelDom,Main_TelBur,Main_GSM,
        Main_Tel4, Main_Tel5
        ,Main_Email,Main_Formule,Main_Options,
        Main_DebutAbonnement,
        Main_DateFinAbonnement,
        Main_Duree,Main_Distributeur,
        Main_TelDistributeur,Main_CoordonneesDistributeur,
        Main_Fichier,
        Main_Fichierclient,Main_Called,Main_Locked,
        Main_CodeFinAppel,Main_CodeRefus,main_datechargment)
        values(
        row[-1], row[0],   row[1], row[2],row[3],
        row[4] ,row[5],'', '','','','',
        '','', '','', '', '','', '', '', '','',
        '', '', '', row[-2],row[-4],  'N',0, '', '',row[-3])
        '''
        out  =[]
        for file ,rows in  map.items():
            for  row in rows:
              #print row
              if row[0].isdigit():
                  out.append(
                        [row[-1],
                         row[0],
                         row[1],
                         row[2],
                         row[3],
                         row[4],
                         row[5],
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         '',
                         row[-2],
                         row[-4],
                         'N',
                         0,
                         '',
                         '',
                         row[-3]])                       
        self.cursor.executemany(query, out)

          


