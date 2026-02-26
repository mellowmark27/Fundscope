proxies = {
    'http': 'http://your_proxy:port',
    'https': 'http://your_proxy:port',
}
response = requests.get(url, headers=headers, proxies=proxies)
print(f"Status: {response.status_code}")
