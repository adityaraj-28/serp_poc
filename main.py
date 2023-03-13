import pandas as pd
import requests
from bs4 import BeautifulSoup

SERP_DIV_CLASS = "yuRUbf"


def extract_domains_from_linkedin():
    soup = BeautifulSoup(unblocker_res.text, 'html.parser')
    tag = soup.find('a', {'data-tracking-control-name': 'about_website'})
    text = tag.text.strip()
    website_list = [text]
    return website_list


unblocker_dict = {
    'linkedin': extract_domains_from_linkedin
}


def extract_url_from_serp_res():
    soup = BeautifulSoup(response.text, 'html.parser')
    div_list = soup.find_all('div', {'class': SERP_DIV_CLASS})
    unblocker_url_list = []
    for div in div_list:
        anchor_tag = div.find('a')
        unblocker_url_list.append(anchor_tag['href'])
    return unblocker_url_list


if __name__ == '__main__':
    df = pd.read_csv('serp_sample.csv')
    columns = list(df.columns)
    # required columns company_id, entity, domain
    for index, row in df.iterrows():
        domain = row['domain']
        company_id = row['company_id']
        # association
        data_source = row['entity']
        google_query = row['google_query']

        serp_url = "https://www.google.com/search?q=" + google_query.replace(' ', '+')

        serp_proxies = {
            "https": "https://brd-customer-hl_387a0b46-zone-serp_zone:7j5tinf2e6il@zproxy.lum-superproxy.io:22225",
        }

        response = requests.get(serp_url, proxies=serp_proxies, verify=False)
        # it returns 10 results by default
        unblocker_urls = extract_url_from_serp_res()
        unblocker_proxies = {
            'https': 'http://brd-customer-hl_387a0b46-zone-unblocker_1:y1ibmxaapy29@zproxy.lum-superproxy.io:22225'
        }
        for unblocker_url in unblocker_urls:
            unblocker_res = requests.get(unblocker_url, proxies=unblocker_proxies, verify=False)
            unblocker_dict[data_source](unblocker_res)
        break
