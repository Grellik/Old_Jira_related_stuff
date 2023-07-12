
from datetime import datetime, timedelta

import requests
import re
import pandas as pd
import pathlib

savePath = str(pathlib.Path(__file__).parent.resolve())

base_url = 'https://jira.goodt.me/rest/api/2'
headers = {'Content-type': 'application/json'}

log = 'g.danilov'
pas = 'qazxsw2!'
maxres = '1'


# log = input("login : ")
# pas = input("password : ")
# maxres = str(input("max results : "))

current_date = datetime.now()
date_format = '%Y-%m-%d'
cd_str = current_date.strftime(date_format)
cd_str_minus_day = (current_date - timedelta(days=3)).strftime(date_format)

base_jql = 'project = ABCHR AND sprint=631 and status != Cancelled'

testers = ["m.druzhinin@goodt.me", "f.shabalkin@goodt.me", "a.kolganov@goodt.me", "a.svalova@goodt.me", "e.voronina@goodt.me", "e.kazova@goodt.me"]

def correct_url(base_jql):
    URL = base_url + '/search?jql=' + base_jql + '&maxResults=' + maxres
    return URL


def json_returner(URL):
    get_req = requests.get(URL, auth=(log, pas))
    return get_req.json()


def extract_issue_from_json(json):
    return json['issues']

def list_self_links(d, testers):
    list_self = []
    values = []
    keys = []
    for i in d:
        list_self.append(i['self'])
        comm_url = i["self"] + "/comment"
        comm_req = requests.get(comm_url, auth=(log, pas))
        comm_data = comm_req.json()
        key = i["key"]
        
        for comment in comm_data["comments"]:
    
            value = None
            
            #print(comment["body"])
            #print(comment["author"]["name"])
            text = comment["body"]
            mail = comment["author"]["emailAddress"]
            if mail in testers:
    
                if "t: " in str(comment["body"]):
                    search_in = text.partition("t:")[2]
                    value = re.search(r'\d+', search_in).group()
    
                elif "t - " in str(comment["body"]):
                    search_in = text.partition("t -")[2]
                    value = re.search(r'\d+', search_in).group()
    
                elif "=" in str(comment["body"]):
                    search_in = text.partition("=")[2]
                    value = re.search(r'\d+', search_in).group()
    
            if value != None:
                values.append(value)
                keys.append(key)
    
    output = {
        "Ключ задачи" : keys,
        "Сложность" : values
    }
    df = pd.DataFrame(output, columns = ["Ключ задачи", "Сложность"])
    df.to_csv(savePath + "\difficulty.csv",index=False)

    print('Проверка размера списка ссылок селф на задачи', len(list_self), sep='/t')
    return list_self


#######################
# При запросе ["fields"] реквест не возвращает ["comment"] с содержимым по неизвестной причине

def extract_comment_from_issue(issue_json):
    comm_self = []
    for i in issue_json:
        print(i['self'])
        data = json_returner(i['self'])
        comm_self.append(data["fields"]["comment"])
    print(comm_self)

######################

class Comment:

    def __init__(self, issue_id, author, body, updatedDate):
        self.issue_id = issue_id
        self.author = author
        self.body = body
        self.updatedDate = updatedDate

    def get_id(self):
        return self.issue_id

    def get_author(self):
        return self.author

    def get_body(self):
        return self.body

    def get_updated(self):
        return self.updatedDate


full_jql_url = correct_url(base_jql)
links = list_self_links(extract_issue_from_json(json_returner(full_jql_url)), testers)
extract_comment_from_issue(extract_issue_from_json(json_returner(full_jql_url)))