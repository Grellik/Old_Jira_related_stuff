import pandas as pd
import pathlib
from jira import JIRA
import itertools
import re

savePath = str(pathlib.Path(__file__).parent.resolve())

log = "g.danilov"
pas = "qazxsw2!"

boards = ["ABCHR", "VID", "AWFM"]
t_b = ["VID", "AWFM"]

testers = ["f.shabalkin", "ak", "m.druzhinin", "i.baranchikova", "g.ericyan", "a.svalova", "v.voronina"]

df = {
    "key" : [],
    "creation date" : [],
    "reporter" : [],
    "priority" : [],
    "status" : [],
    "points" : []
}

#You can access sprint by their ID, ex : "sprint = "ABCHR Sprint 142" can be replaced with "sprint = 646"
#sprintId = [646, 645, 643, 639, 638]  # <---- spints 142, 141, 140, 139, 138

project_start_date = "2021-07-26"
project_end_date = "2021-09-20"

jira_options = {'server': 'https://jira.goodt.me'}
jira = JIRA(options=jira_options, basic_auth=(log, pas))

url = "https://jira.goodt.me/rest/api/2/search?"

def jql_creator(board, kpi):
    part1_jql = "project = " + board + " AND resolutiondate >= " + project_start_date + " AND resolutiondate <= " + project_end_date
    if kpi != 3:
        part1_jql += " AND type = Bug"
    else:
        part1_jql += " AND resolution = Done"
    #if board == "ABCHR":
    #    part1_jql += ' AND (sprint = "ABCHR Sprint 138" or sprint = "ABCHR Sprint 139" or sprint = "ABCHR Sprint 140" or sprint = "ABCHR Sprint 141")'
    if kpi == 2:
        part1_jql = part1_jql + " AND status != Cancelled "
    part2_jql = " ORDER BY createdDate DESC"
    jql = part1_jql + part2_jql


    return jql

def data_collector(board, kpi):
    jql = jql_creator(board, kpi)
    keys = []
    cr_dates = []
    authors = []
    priorities = []
    statuses = []
    for issue in jira.search_issues(jql, maxResults=300):
        keys.append(issue.key)
        cd = str(issue.fields.created)
        cd = cd.split("T")
        cr_dates.append(cd[0])
        authors.append(issue.fields.reporter.name)
        priorities.append(issue.fields.priority.name)
        statuses.append(issue.fields.status.name)

    return keys, cr_dates, authors, priorities, statuses

def main(boards, df):
    kpi = 1
    points = []
    for b in boards:
        k, c, r, p, s = data_collector(b, kpi)
        df["key"] = itertools.chain(df["key"],k)
        df["key"] = list(df["key"])

        df["creation date"] = itertools.chain(df["creation date"],c)
        df["creation date"] = list(df["creation date"])

        df["reporter"] = itertools.chain(df["reporter"], r)
        df["reporter"] = list(df["reporter"])

        df["priority"] = itertools.chain(df["priority"],p)
        df["priority"] = list(df["priority"])

        df["status"] = itertools.chain(df["status"],s)
        df["status"] = list(df["status"])

        for key in k:
            value = None
            comments = jira.comments(key)
            if comments != []:
                for i in comments:
                    comment = jira.comment(key, i).body
                    if jira.comment(key, i).author.key in testers:
                        if "t: " in comment:
                            try:
                                search_in = comment.partition("t: ")[2]
                                value = re.search(r'\d+', search_in).group()
                            except AttributeError:
                                value = None
        
                        elif "t : " in comment:
                            try:
                                search_in = comment.partition("t : ")[2]
                                value = re.search(r'\d+', search_in).group()
                            except AttributeError:
                                value = None
                        elif "t - " in comment:
                            try:
                                search_in = comment.partition("t - ")[2]
                                value = re.search(r'\d+', search_in).group()
                            except AttributeError:
                                value = None
                        elif "t- " in comment:
                            try:
                                search_in = comment.partition("t- ")[2]
                                value = re.search(r'\d+', search_in).group()
                            except AttributeError:
                                value = None
            
                        elif " = " in comment and jira.comment(key, i).author.key in ["m.druzhinin", "a.svalova"]:
                            try:
                                search_in = comment.partition(" = ")[2]
                                value = re.search(r'\d+', search_in).group()
                            except AttributeError:
                                value = None
                                
            if value != None:
                if int(value) <= 25:
                    points.append(int(value))
                else:
                    points.append(" - ")
            else:
                points.append(" - ")

        df["points"] = points

    return df

df = main(boards, df)

df = pd.DataFrame(df ,columns=["key", "creation date", "reporter", "priority", "status", "points"])
df.to_csv(savePath + "\kpi_data.csv", index=False)