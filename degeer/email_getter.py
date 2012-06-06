
import poplib 
import email
import sys
import base64
import string
import time
from datetime import date, datetime
#
COSA_A_PCCI  = "X"
SEN_A_PCCI   = "X"
#
DEJA_REABO   = "X"
TMK          = "X"

class  SMTP2Local(object):
    def __init__ (self, smtp_host = 'X',
                        smtp_user = 'X',
                        smtp_pass = 'X' , root ='X'):
        self.M = poplib.POP3_SSL(smtp_host)
        self.M.user(smtp_user)
        self.M.pass_(smtp_pass)
        self.numMessages = len(self.M.list()[1])
        self.root  =root

    def extract_file(self):
        for i in range(self.numMessages)[::-1]:
            print "=" * 40
            msg = self.M.retr(i+1)
            str = string.join(msg[1], "\n")
            mail = email.message_from_string(str)

            dt  = ' '.join(mail["Date"].split(' ')[:-2])
            if ',' in dt:
                dt = dt.split(',')[1].strip()
            dt = datetime.strptime(dt, "%d %b %Y")
            if  'canal-plus' not in mail['From']\
                and 'canalhorizons' not in mail['From']:
                  continue

            # Nous avons depasse les messages du jours
            if dt.date()  < date.today():
                break
        
            if mail.is_multipart():
               print "From:", mail["From"]
               print "Subject:", mail["Subject"]
               print "Date:", mail["Date"]
               for  p in mail.get_payload():
                    print  p.get_content_type()
                    if p.get_filename():
                         data = base64.decodestring(p.get_payload())
                         self.create_file( name = p.get_filename(),
                                           data = data )
                         time.sleep(2)   
        self.M.quit()
        
    def create_file(self, name, data):
        ROOT= self.__root(name)
        f = open('%s/%s'%(ROOT, name), 'w')
        f.write(data)
        f.close()
        
    def __root(self, name):
       if   'AFR_AS' in name.upper():
             return   '%s/%s'%(COSA_A_PCCI ,TMK)
       elif 'AFR_STOPAS' in name.upper():
             return   '%s/%s'%(COSA_A_PCCI ,DEJA_REABO)
       elif  'SEN_AS' in name.upper():
             return   '%s/%s'%(SEN_A_PCCI ,TMK)
       elif 'SEN_STOPAS' in name.upper():
             return   '%s/%s'%(SEN_A_PCCI ,DEJA_REABO)
            
def run():
    while True:
      try:
        s= SMTP2Local()
        s.extract_file()
        time.sleep(60*10)
        print  'WAIT INTO EMAIL GETTER'
      except KeyboardInterrupt,e:
        print  'STOPPING  EMAIL GETTER'
        break
      except:
        print 'Probleme de connection avec le sercveur'
        raise
        time.sleep(60)
      else:
        pass
if __name__== '__main__':
   run()
       
   
            
