import urllib.request
try:
    code = urllib.request.urlopen("http://127.0.0.1:8000/", timeout=5).status
    print("HTTP", code)
except Exception as e:
    print("ERR", e)