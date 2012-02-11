'''
SMPT wrapper
'''


__author__  ='Alioune Dia'
__version__ ='0.1'
__date__  ='2011-10-07'


import os
import sys
import smtplib
import mimetypes


from email import encoders
from email.message import Message
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import getopt

COMMASPACE = ', '


SMTP_PCCI_HOST ="0.0.0.0"
SMTP_PCCI_PORT =25
SMTP_PCCI_USER ="hi_02"
SMTP_PCCI_PWD  ="02_hi"

from datetime import datetime


recipients  = [
              'adia@pcci.sn',
              'dia.aloiunes@gmail.com']



def send(to =None, from_ ='adia@pcci.sn',
         att_path =None, att_file=None,subject =None,
         preambule ='Preambule', body =None, *agrs,**kw):
    '''Send mail to someone '''

    
    to  = (to or recipients)


    td = datetime.now()
    body = body or \
    """
    'Bonjour Yatma </br>
    Merci de deposer ces fichiers</br>
    L \'Informatique '
    """%str(td)

    subject = subject or\
    """
    FTP du %s
    """%str(td)

    part1 = MIMEText(body, 'html')
    
    outer = MIMEMultipart()
    outer['To']      = ','.join(to)
    outer['From']    =from_
    outer['Subject'] =subject
    outer.preambule  =preambule       
	
    #
    files, is_att = [] , False
    if att_file and att_path:
	raise ValueError(
            """
            send mail accepte soit att_file or att_path
            pas les deux!
            """)

    if att_file:
	files =[att_file]
    if att_path:
	for f in os.listdir(att_path):
	    f = os.path.join(att_path, f)
	    print  f
	    if os.path.isfile(f):
	        files.append(f)


    # The email should  contain files attachments
    if len(files)>0:
	is_att= True

    
    #if is_att
    if is_att:
        for f in files:
            # guess the file type, see ptyhon 's doc 
            ctype,  encoding = mimetypes.guess_type(f)
            print ctype, encoding
            # if ctype is None and encoding is not None
            if ctype is None and encoding is not None:
                    ctype  ='application/octet-stream'


            if ctype is None:
                     ctype  ='application/octet-stream'   
            maintype, subtype = ctype.split('/', 1)
            msg =  mime_class(maintype,subtype, f)
            outer.attach(msg)


    outer.attach(part1)
    # send message now
    print  >> sys.stdout ,'Sending message to %r'%(to,)
    s = smtplib.SMTP(SMTP_PCCI_HOST,SMTP_PCCI_PORT)
    s.ehlo()

    # le serveur de messagerie ne supporte pas
    # ttls pour le moment on 
    #s.starttls()
    s.ehlo()
    s.login(SMTP_PCCI_USER, SMTP_PCCI_PWD)
    s.sendmail(from_, to , outer.as_string())
    s.quit()
    print  >> sys.stdout ,'Terminate to send message'

def mime_class(maintype,subtype, f):
    ''' Return the mime type class'''
    if maintype == 'text':
        fp = open(f)
        # Note: we should handle calculating the charset
        msg = MIMEText(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == 'image':
        fp = open(f, 'rb')
        msg = MIMEImage(fp.read(), _subtype=subtype)
        fp.close()
    elif maintype == 'audio':
        fp = open(f, 'rb')
        msg = MIMEAudio(fp.read(), _subtype=subtype)
        fp.close()
    else:
        fp = open(f, 'rb')
        msg = MIMEBase(maintype, subtype)
        msg.set_payload(fp.read())
        fp.close()
        # Encode the payload using Base64
        encoders.encode_base64(msg)
    msg.add_header('Content-Disposition',
                   'attachment',
                   filename=f[20:])
    return msg

    
if __name__ =='__main__':

    # python  send.py -t dia.aliounes@gmail.com -f adia@pcci.sn  -p /home/
    to=from_=path = None 
    try:
        opts, args = getopt.getopt(sys.argv[1:],
        't:f:p:')
    except getopt.error:
          raise
          sys.exit(1)

    if len(opts)!=3:
        print  '3 options s il vous plait'
        sys.exit(1)
    print opts
    for o,a in opts:
        if o== '-t':
            to    = a
        if o== '-f':
            from_ = a
        if o== '-p':
            path  = a
    print >>sys.stdout,'Alioune test send mail'
    print to
    print '-'*10
    print from_
    print '-'*10
    print path
    send(to,from_, path)





    
