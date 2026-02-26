from selenium import webdriver
from bs4 import BeautifulSoup

driver = webdriver.Chrome()
driver.get(url)
html = driver.page_source
soup = BeautifulSoup(html, 'html.parser')
# Your scraping logic here
driver.quit()
