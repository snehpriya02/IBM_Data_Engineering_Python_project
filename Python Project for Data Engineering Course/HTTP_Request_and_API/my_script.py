#pip install requests
import requests
url = "https://www.ibm.com/"
r = requests.get(url)
#print(r.status_code)
#print(r.request.headers)  #request header
#print(r.request.body)  
#print(r.headers)    #response header
#print(r.encoding)
print(r.text[1:100])
