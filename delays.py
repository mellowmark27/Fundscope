import time
for i in range(5):
    response = requests.get(url, headers=headers)
    print(f"Request {i+1}: {response.status_code}")
    time.sleep(2)  # 2-second delay
