import email
import sys
import base64
import string
import time
from datetime import date, datetime
try:
    import poplib 
except:
    raise 'This module need poplib'

# The Name of the directory to download Message retreived from
# The server.
COSA_A_PCCI  = "COSA_A_PCCI"
SEN_A_PCCI   = "SEN_A_PCCI"
# The Name of the directory to donwload Message retreived from
# The server via pop3 protocol
DEJA_REABO   = "DEJA_REABO"
TMK          = "TMK"
# -Whos is the list of  email domain name which should be download
# Only theses emails from theses domains list should be download
# Skep Orhers

Whos =['canal-plus', 'canalhorizons']
class  SMTP2Local(object):
    def __init__ (self,
                 smtp_host = '0.0.0.0',
                 smtp_user = 'hi',
                 smtp_pass ='ho' ,
                 root ='POP3_DIR'):
        self.M = poplib.POP3(smtp_host)
        self.M.user(smtp_user)
        self.M.pass_(smtp_pass)
        self.numMessages = len(
                self.M.list()[1])
        self.root  =root

    # Method to retreive file from smtp to local
    def extract_file(self):
        for i in range(self.numMessages)[::-1]:
            print "=" * 40
            # Get message 
            msg = self.M.retr(i+1)
            str = string.join(msg[1], "\n")
            mail = email.message_from_string(str)
            dt  = ' '.join(mail["Date"].split(' ')[:-2])
            if ',' in dt:
                dt = dt.split(',')[1].strip()
            dt = datetime.strptime(dt, "%d %b %Y")

            # Pour les moment check seulement les
            # emails provenant de ces domaines , et saute les autres
            if mail['From']  not in Whos:
                   continue

            # Nous avons depasse les messages du jours
            # Ne m'interesse que les messages du jour , car nous
            # faisons un chargment journalier
            # Si tu veux avoir plus de messages a toi de voir
            if dt.date()  < date.today():
                break

            # This message content a likned file perhaps
            if mail.is_multipart():
               print "From:", mail["From"]
               print "Subject:", mail["Subject"]
               print "Date:", mail["Date"]
               # check payload 
               for  p in mail.get_payload():
                    # print the content type of message
                    print  p.get_content_type()
                    
                    # if this payload has a filename ,then
                    # download this file into given directory
                    if p.get_filename():
                       # decode data form base64 encode 
                       data = base64.decodestring(p.get_payload())
                       self.create_file( name = p.get_filename(),
                                         data = data )
                       # Wait some to check another file 
                       time.sleep(2)
                  
        self.M.quit()

    # build a filename 
    def create_file(self, name, data):
        ROOT= self.__root(name)
        f = open('%s/%s'%(ROOT, name), 'w')
        f.write(data)
        f.close()

    # build root dir
    def __root(self, name):
       if   'AFR_AS' in name.upper():
             return   '%s/%s'%\
                 (COSA_A_PCCI ,TMK)
       elif 'AFR_STOPAS' in name.upper():
             return   '%s/%s'%\
                 (COSA_A_PCCI ,DEJA_REABO)
       elif  'SEN_AS' in name.upper():
             return   '%s/%s'%\
                 (SEN_A_PCCI ,TMK)
       elif 'SEN_STOPAS' in name.upper():
             return   '%s/%s'%\
                    (SEN_A_PCCI ,DEJA_REABO)

# loop  check message from server all time
# run - tourne en deamon et check tous les fichiers
# depuis le serveur.
# Je suis pris par le temps mais  il y'a un moyen plus
# simple et  elegant de faire
def run():
       while True:
          try:
              s= SMTP2Local()
              s.extract_file()
              time.sleep(60)
              print  'WAIT INTO EMAIL GETTER'
          except KeyboardInterrupt,e:
                 print  'STOPPING  EMAIL GETTER'
                 break
          except:
              print 'Probleme de connection avec le serveur'
              time.sleep(60)
          else:
              pass

# run or test
if __name__== '__main__':
   run()
       
   
            
