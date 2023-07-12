import pandas as pd
import pathlib
from jira import JIRA
import itertools
import re

savePath = str(pathlib.Path(__file__).parent.resolve())

log = "g.danilov"
pas = "qazxsw2!"

boards = ["ABCHR", "LMAO"]

jira_options = {'server': 'https://jira.goodt.me'}
jira = JIRA(options=jira_options, basic_auth=(log, pas))

testers = ["a.svalova", "v.voronina","d.cviliy", "a.rychkov", "e.vetoshkina", "v.yarovoy", "a.kosolapov"]


def kpi_for_tester(tester):
    keys = []
    problem_type = []
    problem_points = []
    jql = "project = ABC_HRPORTAL_ABCCONSULTING and createdDate >= '2021/10/01' and createdDate < '2021/12/17' AND assignee = {} ORDER BY createdDate , updated DESC".format(tester)
    for issue in jira.search_issues(jql, maxResults=999):
        local_key = issue.key
        link = '=HYPERLINK("https://jira.goodt.me/browse/{}";"{}")'.format(local_key,local_key)
        keys.append(link)
        problem_type.append(issue.fields.issuetype.name)
        try:
            points = int(issue.fields.customfield_12402)
        except TypeError:
            points = 0
        problem_points.append(points)
    
    df = {
        "key" : keys,
        "type" : problem_type,
        "points" : problem_points
    }
    
    df = pd.DataFrame(df ,columns=["key", "type", "points"])
    df.to_csv("{}\kpiq3_data_{}.csv".format(savePath,tester), index=False)
    
    summ_points = sum(problem_points)
    prob_checked = int(len(keys))
    
    sum_bugs = 0
    jql = "project = ABC_HRPORTAL_ABCCONSULTING and createdDate >= '2021/10/01' and createdDate < '2021/12/17' and creator = {} AND type = Bug".format(tester)
    for issue in jira.search_issues(jql, maxResults=999):
        sum_bugs += 1
    
    sum_bugs_not_cancelled = 0
    pers_crit = 0
    crit_dict ={
        "Lowest" : 1,
        "Low" : 2,
        "Medium" : 3,
        "High" : 4,
        "Highest" : 5
    }
    jql = "project = ABC_HRPORTAL_ABCCONSULTING and createdDate >= '2021/10/01' and createdDate < '2021/12/17' and creator = {} AND type = Bug AND status != Cancelled".format(tester)
    for issue in jira.search_issues(jql, maxResults=999):
        sum_bugs_not_cancelled += 1
        pers_crit += crit_dict[issue.fields.priority.name]
    
    final_result = """
    тестер {}, проверенно задач {}, сумма поинтсов по всем задачвм {} kpi3,
    все баги {} kpi1,  не отмененные баги {} kpi1, перс критичность {}
    """.format(tester, prob_checked, summ_points, sum_bugs, sum_bugs_not_cancelled, pers_crit)
    
    return final_result
    
def for_kpi2():
    jql = "project = ABC_HRPORTAL_ABCCONSULTING and createdDate >= '2021/10/01' and createdDate < '2021/12/17'  and type = Bug and status != Cancelled"
    crit_dict ={
        "Lowest" : 1,
        "Low" : 2,
        "Medium" : 3,
        "High" : 4,
        "Highest" : 5
    }
    sum_bugs = 0
    sum_crit = 0
    for issue in jira.search_issues(jql, maxResults=999):
        sum_bugs += 1
        sum_crit += crit_dict[issue.fields.priority.name]
        
    final_result = "все неотменненые бвги {}, их критичность {}".format(sum_bugs,sum_crit)
    
    return final_result
    
for tester in testers:
    print(kpi_for_tester(tester))

print(for_kpi2())