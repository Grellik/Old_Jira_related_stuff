import pandas as pd
import pathlib
import requests

savePath = str(pathlib.Path(__file__).parent.resolve())

log = "g.danilov"
pas = "qazxsw2!"

boards = ["ABCHR"]


sprint = "ABCHR Sprint 133"

sprint_end_date = [2021, 5, 31]

url = "https://jira.goodt.me/rest/api/2/search?"
def jql_creator(board,sprint):
    part1_jql = "maxResults=500&jql= project = " + board

    part1_jql = part1_jql + " AND sprint = " + '"' + sprint + '"'

    part2_jql = " ORDER BY createdDate DESC"
    jql = part1_jql + part2_jql


    return jql
def url_creator(jql):
    URL = url + jql
    return URL

def requester(URL):
    r = requests.get(URL, auth = (log, pas))
    data = r.json()
    return data

def main(boards, sprint):
    for board in boards:
        issues = requester(url_creator(jql_creator(board, sprint)))
        keys = []
        statuses = []
        timing = []  
        for issue in issues["issues"]:
            keys.append(issue["key"])
            statuses.append(issue["fields"]["status"]["name"])
            res_timing = None
            if issue["fields"]["status"]["name"] == "Done" or issue["fields"]["status"]["name"] == "Готово":
                res_date = issue["fields"]["resolutiondate"]
                res_date = res_date.split("T")
                res_date = res_date[0].split("-")
                if int(res_date[0]) < sprint_end_date[0]:
                    res_timing = "done in time"
                elif int(res_date[0]) == sprint_end_date[0]:
                    if int(res_date[1]) < sprint_end_date[1]:
                        res_timing = "done in time"
                    elif int(res_date[1]) == sprint_end_date[1]:
                        if int(res_date[2]) <= sprint_end_date[2]:
                            res_timing = "done in time"
            else:
                res_timing = " - "
            if res_timing == None:
                res_timing = "done after sprint ended"
    
            timing.append(res_timing)
    
    
    return keys, statuses, timing

k, s, t = main(boards, sprint)


df = {
    "key" : k,
    "status" : s,
    "timing" : t
}

df = pd.DataFrame(df ,columns=["key", "status", "timing"])
df.to_csv(r"C:\Users\danil\Desktop\sreport.csv", index=False)