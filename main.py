import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

SERP_DIV_CLASS = "yuRUbf"


def extract_websites_from_linkedin():
    soup = BeautifulSoup(unblocker_res.text, 'html.parser')
    tag = soup.find('a', {'data-tracking-control-name': 'about_website'})
    text = tag.text.strip()
    website_list = [text]
    return website_list


def serp_datasource_id_from_linkedin_url(url):
    if url[-1] == '/':
        url = url[:-1]
    path = urlparse(url).path
    _domain = path.split('/')[-1]
    return _domain


unblocker_dict = {
    'linkedin': extract_websites_from_linkedin
}

datasource_serp_id_extractor = {
    'linkedin': serp_datasource_id_from_linkedin_url
}


def extract_url_from_serp_res():
    soup = BeautifulSoup(response.text, 'html.parser')
    div_list = soup.find_all('div', {'class': SERP_DIV_CLASS})
    unblocker_url_list = []
    for div in div_list:
        anchor_tag = div.find('a')
        unblocker_url_list.append(anchor_tag['href'])
    return unblocker_url_list


def create_output_file_entry():
    extracted_domain = urlparse(website).netloc
    domain_matching = 'True' if domain == extracted_domain else 'False'
    _row = f'{company_id}, {data_source}, {domain}, {unblocker_url}, {serp_data_source_id}, {website}, {extracted_domain}, {domain_matching}\n'
    file_name_dict[data_source].write(_row)


if __name__ == '__main__':
    df = pd.read_csv('serp_sample.csv')
    associations = ['linkedin', 'glassdoor', 'pitchbook', 'playstore', 'appstore']
    file_name_dict = {}
    for association in associations:
        file_name_dict[association] = open(f'{association}.txt', 'w+')
    error_file = open('errors.txt', 'w+')
    session = requests.Session()
    # required columns company_id, entity, domain
    for index, row in df.iterrows():
        domain = row['domain']
        company_id = row['company_id']
        data_source = row['entity']
        google_query = row['google_query']
        try:
            serp_url = "https://www.google.com/search?q=" + google_query.replace(' ', '+')

            serp_proxies = {
                "https": "https://brd-customer-hl_387a0b46-zone-serp_zone:7j5tinf2e6il@zproxy.lum-superproxy.io:22225",
            }

            response = session.get(serp_url, proxies=serp_proxies, verify=False)
            # it returns 10 results by default
            unblocker_urls = extract_url_from_serp_res()
            unblocker_proxies = {
                'https': 'http://brd-customer-hl_387a0b46-zone-unblocker_1:y1ibmxaapy29@zproxy.lum-superproxy.io:22225'
            }
            for unblocker_url in unblocker_urls:
                unblocker_res = session.get(unblocker_url, proxies=unblocker_proxies, verify=False)
                serp_data_source_id = datasource_serp_id_extractor[data_source](unblocker_url)
                websites = unblocker_dict[data_source]()
                for website in websites:
                    create_output_file_entry()
            break
        except Exception as e:
            error_file.write(f'{company_id}, {company_id}, {data_source}, {google_query} : {str(e)}\n')

    session.close()
    error_file.close()
    for file in file_name_dict.values():
        file.close()