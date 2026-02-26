session = requests.Session()
session.headers.update(headers)
response = session.get(url)
print(f"Status: {response.status_code}")
