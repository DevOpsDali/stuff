import sys, os, boto3, botocore, shutil, urllib
from PyPDF2 import PdfFileWriter, PdfFileReader
from botocore.exceptions import ClientError
import zipfile

def zipdir(path, ziph):
    # ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

def lambda_handler(event, context):
	print(event)
	BUCKET_NAME = event['Records'][0]['s3']['bucket']['name']
	KEY = event['Records'][0]['s3']['object']['key']
	s3 = boto3.resource('s3')
	
	test = boto3.client('s3')
	
	print(test.list_objects(
		Bucket='bernsteinandassociates'
		))
	
	print(BUCKET_NAME)
	KEY = KEY.replace("+", " ")
	print(KEY)
	
	if os.path.exists("/tmp/pdf"): #Delete our working dir if it exists
		shutil.rmtree("/tmp/pdf")

	os.mkdir ( "/tmp/pdf" ) 
	
	if os.path.exists("/tmp/source"): #If a source file already exists delete it
		shutil.rmtree("/tmp/source")

	os.mkdir ( "/tmp/source" )
	
	if os.path.exists("/tmp/output"): #If a source file already exists delete it
		shutil.rmtree("/tmp/output")

	os.mkdir ( "/tmp/output" )

	try:
		s3.Bucket(BUCKET_NAME).download_file(KEY, '/tmp/source/workitem.zip') #Attempt to copy the zip from s3
	except botocore.exceptions.ClientError as e:
		if e.response['Error']['Code'] == "404":
			print("The object does not exist.")
			print(e.response)
		else:
			raise
	shutil.unpack_archive('/tmp/source/workitem.zip', '/tmp/source/') #Extract the contents
    
	for x in os.listdir('/tmp/source'):
		print (x)
    
	settings=[]    
    
	with open('/tmp/source/settings.txt') as f: #Read in the settings file
		content = f.readlines()
	content = [x.strip() for x in content] 
    
	#print (content)
    
	filename = list((filter(lambda x: x.endswith('.pdf'), os.listdir('/tmp/source/')))) #Dynamically get the name of the pdf we're splitting ( I THINK ?!)
	split_pdf("/tmp/source/" + filename[0], content[0], content[1])

	shutil.make_archive('/tmp/output/' + content[0], 'zip', '/tmp/pdf/')
	
	to_email = content[2]
	email_list = []
	email_list = to_email.split(';')
    
	for x in os.listdir('/tmp/output/'):
		print (x)
		s3.Bucket(BUCKET_NAME).upload_file('/tmp/output/'+ x, 'done/'+x)
	
	OUTPUT_LOCATION = 'https://s3-us-west-2.amazonaws.com/bernsteinandassociates/done/' + urllib.parse.quote(content[0]) + '.zip'

	SENDER = "PDF Splitter <corey@coreyr.com>"
	RECIPIENT = "corey@coreyr.com"
	AWS_REGION = "us-west-2"
	SUBJECT = "PDF Output - " + content[0]
	CHARSET = "UTF-8"
	# The email body for recipients with non-HTML email clients.
	BODY_TEXT = ("Finished splitting pdf. You can download the file here: " + OUTPUT_LOCATION )
	            
	BODY_HTML = """<html>
	<head></head>
	<body>
	  <h1>Split Complete</h1>
	  <p>You can download the output file here: """ + OUTPUT_LOCATION + """ </p>
	</body>
	</html>
	            """
	client = boto3.client('ses',region_name=AWS_REGION)
	try:
	    #Provide the contents of the email.
	    response = client.send_email(
	        Destination={
	            'ToAddresses': email_list,
	        },
	        Message={
	            'Body': {
	                'Html': {
	                    'Charset': CHARSET,
	                    'Data': BODY_HTML,
	                },
	                'Text': {
	                    'Charset': CHARSET,
	                    'Data': BODY_TEXT,
	                },
	            },
	            'Subject': {
	                'Charset': CHARSET,
	                'Data': SUBJECT,
	            },
	        },
	        Source=SENDER
	    )
	# Display an error if something goes wrong.	
	except ClientError as e:
	    print(e.response['Error']['Message'])
	else:
	    print("Email sent! Message ID:"),
	    print(response['ResponseMetadata']['RequestId'])
	return 'Hello from Lambda'

def split_pdf(sourcepdf, outputname, splitnum):
	inputpdf = PdfFileReader(open(sourcepdf, "rb"))
	#name = sys.argv[1].split(".")
	#outputname = sys.argv[2]
	#splitnum = sys.argv[3]
	if os.path.exists("/tmp/pdf"):
		shutil.rmtree("/tmp/pdf")

	os.mkdir ( "/tmp/pdf" )
	
	pdfcount = 1
	for i in range(0, inputpdf.numPages, int(splitnum)):
		output = PdfFileWriter()
		z = 0
		y = 1
		for y in range(0, int(splitnum), 1):
			z = i + y
			output.addPage(inputpdf.getPage(z))

		if os.path.exists('/tmp/source/attachments'):
			for filename in sorted(os.listdir('/tmp/source/attachments')):
				if not filename.startswith('.'):
					attachmentPDF = PdfFileReader(open('/tmp/source/attachments/'+ filename, 'rb'))
					for c in range(attachmentPDF.numPages):
						output.addPage(attachmentPDF.getPage(c))

		with open( "/tmp/pdf/" + outputname +" %02d.pdf" % pdfcount, "wb") as outputStream:
			output.write(outputStream)
		outputStream.close()
		pdfcount += 1
	return "Done"
