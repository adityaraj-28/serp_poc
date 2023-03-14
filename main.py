import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs
from time import time
import logging
import tldextract
import re

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

SERP_DIV_CLASS = "yuRUbf"


def extract_websites_from_linkedin():
    soup = BeautifulSoup(unblocker_res.text, 'html.parser')
    tag = soup.find('a', {'data-tracking-control-name': 'about_website'})
    text = tag.text.strip()
    website_list = [text]
    return website_list


def extract_websites_from_playstore():
    soup = BeautifulSoup(unblocker_res.text, 'html.parser')
    website_list = []
    for div in soup.find_all('div', {'class': 'pZ8Djf'}):
        if div.find('div', {'class': 'xFVDSb'}).text in ['Website', 'Email', 'Privacy policy']:
            website_list.append(div.find('div', {'class': 'pSEeg'}).text)
    return website_list


def extract_websites_from_pitchbook():
    soup = BeautifulSoup(unblocker_res.text, 'html.parser')
    a_tag = soup.find("a", {"class": "d-block-XL font-underline", "aria-label": "Website link"})
    return [a_tag["href"]]


def extract_websites_from_appstore():
    soup = BeautifulSoup(unblocker_res.text, 'html.parser')
    website_list = []
    for a in soup.find_all('a', {'class': 'link icon icon-after icon-external'}):
        if 'Developer Website' in a.text or 'App Support' in a.text or 'Privacy Policy' in a.text:
            website_list.append(a['href'])
    return website_list


def extract_websites_from_glassdoor():
    soup = BeautifulSoup(unblocker_res.text, 'html.parser')
    a_tag = soup.find('a', {'data-test': 'employer-website'})
    return [a_tag["href"]]


def remove_ending_slash_from_url(url):
    if url[-1] == '/':
        url = url[:-1]
    return url


def serp_datasource_id_from_linkedin_url(url):
    url = remove_ending_slash_from_url(url)
    path = urlparse(url).path
    data_source_ids = path.split('/')[-1]
    return data_source_ids


def serp_datasource_id_from_playstore_url(url):
    url = remove_ending_slash_from_url(url)
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    data_source_ids = query_params['id'][0]
    return data_source_ids


def serp_datasource_id_from_appstore_url(url):
    url = remove_ending_slash_from_url(url)
    path = urlparse(url).path
    data_source_ids = path.split('/')[-1]
    data_source_ids = str(data_source_ids).removeprefix('id')
    return data_source_ids


def serp_datasource_id_from_glassdoor_url(url):
    url = remove_ending_slash_from_url(url)
    url = url.removesuffix('.htm')
    path = urlparse(url).path
    last_part = str(path.split('/')[-1])
    match = re.search(r"EI_IE(\d+)\.", last_part)
    return match.group(1)


unblocker_dict = {
    'linkedin': extract_websites_from_linkedin,
    'playstore': extract_websites_from_playstore,
    'pitchbook': extract_websites_from_pitchbook,
    'appstore': extract_websites_from_appstore,
    'glassdoor': extract_websites_from_glassdoor
}

datasource_serp_id_extractor = {
    'linkedin': serp_datasource_id_from_linkedin_url,
    'playstore': serp_datasource_id_from_playstore_url,
    'pitchbook': serp_datasource_id_from_linkedin_url,
    'appstore': serp_datasource_id_from_appstore_url,
    'glassdoor': serp_datasource_id_from_glassdoor_url
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
    domain_matching = 'True' if domain == extracted_domain else 'False'
    _row = f'{company_id}, {data_source}, {domain}, {unblocker_url}, {serp_data_source_id}, {website}, {extracted_domain}, {domain_matching}\n'
    file_name_dict[data_source].write(_row)


def extract_domain(url):
    ext = tldextract.extract(url)
    return f'{ext.domain}.{ext.suffix}'


if __name__ == '__main__':
    df = pd.read_csv('serp_sample.csv')
    associations = ['linkedin', 'glassdoor', 'pitchbook', 'playstore', 'appstore']
    file_name_dict = {}
    for association in associations:
        file_name_dict[association] = open(f'{association}.txt', 'w+', 1)
    error_file = open('errors.txt', 'w+', 1)
    session = requests.Session()
    for index, row in df.iterrows():
        start_time = time()
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
                    extracted_domain = extract_domain(website)
                    create_output_file_entry()
            end_time = time()
            logging.info(f'{company_id}, {data_source}, Time taken: {end_time - start_time}')
        except Exception as e:
            logging.error(f'{company_id}, {domain}, {data_source}, {google_query} : {str(e)}')
            error_file.write(f'{company_id}, {domain}, {data_source}, {google_query} : {str(e)}\n')

    session.close()
    error_file.close()
    for file in file_name_dict.values():
        file.close()
