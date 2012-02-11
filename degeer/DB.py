"""
Wrap a Database 
"""
import pyodbc
import time
class DB(object):
      def __init__(self):
        try:
           self.connection = pyodbc.connect('''DRIVER={SQL Server};SERVER=hi_02;DATABASE=hi_02_db;
                                            UID=02_hi;PWD=02_hi''', autocommit =True)
           self.cursor = self.connection.cursor()
           self.close  = self.cursor.close
           self.data   = []
        except pyodbc.OperationalError:
               raise
              
      def do_query(self, qs):
          self.cursor.execute(qs)

      def excute_many_for_senegal(self , query, map):
          '''
          SENEGAL 
          '''
          out  =[]
          for file ,rows in  map.items():
             for  row in rows:
                  #print row
                  if row[0].isdigit():
                      out.append([ row[-1],row[0],'' ,row[1],row[2],row[3],row[4],row[14],row[15],row[16],row[17],'SENEGAL',
                                   row[10] ,row[11],row[8],row[9],row[12],row[13],row[5] ,'' ,'', row[7], row[6],row[-2],
                                   row[-4], 'N',0, '', '',row[-3]])
                      
                  print  '-'*40
          self.cursor.executemany(query, out)

      def excute_many_for_afr(self , query, map):
          '''
          AFR
          ====
          '''
          out  =[]
          for file ,rows in  map.items():
             for  row in rows:
                  #print row
                  if row[0].isdigit():
                      out.append([ row[-1], row[0],   row[1], row[2],row[3], row[4] ,row[5], row[18], row[19],row[20],row[21],row[22],
                                   row[11],row[13], row[15],row[16], row[14], row[17], row[6],  row[7], row[8], row[9],row[10],
                                   row[23], row[25],row[24], row[-2],row[-4],  'N',0, '', '',row[-3]])    
          self.cursor.executemany(query, out)
          
      def excute_many_for_afr2(self , query, map):
          '''
          AFR
          ====
          '''
          out  =[]
          for file ,rows in  map.items():
             for  row in rows:
                  #print row
                  if row[0].isdigit():
                      
                      out.append([  row[-1], row[0],   row[1], row[2],row[3], row[4] ,row[5],'', '','','','',
                                    '','', '','', '', '','', '', '', '','','', '', '', row[-2],row[-4],  'N',0, '', '',row[-3]])    
          self.cursor.executemany(query, out)

          


