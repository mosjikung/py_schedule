from selenium import webdriver

option = webdriver.ChromeOptions()
option.add_argument("-incognito")
#option.add_argument("--headless")
#option.add_argument("disable-gpu")

browser = webdriver.Chrome('C:/Users/Administrator/Downloads/chromedriver_win32/chromedriver.exe')

# https://docs.google.com/forms/d/e/1FAIpQLSeLaMbGiwKxLRVW35M6fJauo_Z5_cDyc0e_TaREwAYjI8MANg/viewform


# class="quantumWizTextinputPaperinputInput exportInput"

browser.get("https://docs.google.com/forms/d/e/1FAIpQLSeLaMbGiwKxLRVW35M6fJauo_Z5_cDyc0e_TaREwAYjI8MANg/viewform")



textboxes[0].send_keys("Hello World")