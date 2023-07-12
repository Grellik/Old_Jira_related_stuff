import requests
import pandas as pd
import re
import pathlib

savePath = str(pathlib.Path(__file__).parent.resolve())

log = "g.danilov"
pas = "qazxsw2!"

testers = ["v.voronina@goodt.me", "a.svalova@abcconsulting.ru", "g.ericyan@goodt.me"]

boards = ["AWFM", "VID", "ABCHR"]

project_start_date = "2021-07-26"
project_end_date = "2021-09-20"

all_kpi_list = []
total_kpi_list = []

url = "https://jira.goodt.me/rest/api/2/search?maxResults=300&"

def jql_creator(board, status, kpi):
    part1_jql = "project = " + board + " AND resolutiondate >= " + project_start_date + " AND resolutiondate <= " + project_end_date
    if kpi != 3:
        part1_jql += " AND type = Bug"
    else:
        part1_jql += " AND resolution = Done"
    if board == "ABCHR":
        part1_jql += ' AND (sprint = "ABCHR Sprint 138" or sprint = "ABCHR Sprint 139" or sprint = "ABCHR Sprint 140" or sprint = "ABCHR Sprint 141")'
    if status != None:
        part1_jql = part1_jql + " AND status " + status
    part2_jql = " ORDER BY createdDate DESC"
    jql = "jql=" + part1_jql + part2_jql


    return jql

def url_creator(jql):
    URL = url + jql
    return URL

def requester(URL):
    r = requests.get(URL, auth = (log, pas))
    data = r.json()
    urls = []
    for issue in data["issues"]:
        urls.append(issue["self"])
    print(urls)
    return data

def com_url_creator(data):
    comments_url_list = []
    for issue in data["issues"]:
        comment_url = issue["self"] + "/comment"
        comments_url_list.append(comment_url)
        
    return comments_url_list

def com_requester(comment_url):
    comm_req = requests.get(comment_url, auth=(log, pas))
    comm_data = comm_req.json()
    return comm_data

def kpi_1(tester, board, incl):
    kpi = 1
    status = None

    total_bugs = 0
    cancelled_bugs = 0

    total_bugs_data = requester(url_creator(jql_creator(board, status, kpi)))

    for issue in total_bugs_data["issues"]:
        if tester == issue["fields"]["reporter"]["emailAddress"]:
            total_bugs += 1
            if issue["fields"]["status"]["statusCategory"]["name"] == "Cancelled":
                cancelled_bugs += 1

    try :
        kpi_1_value = ((total_bugs - cancelled_bugs)/total_bugs)*incl
    except ZeroDivisionError:
        kpi_1_value = 0

    return kpi_1_value

def kpi_2(tester, board, incl, total_incl):
    kpi = 2
    status = " != Cancelled"

    crit_dict ={
        "Lowest" : 1,
        "Low" : 2,
        "Medium" : 3,
        "High" : 4,
        "Highest" : 5
    }

    total_crit = 0
    pers_crit = 0

    total_bugs_data = requester(url_creator(jql_creator(board, status, kpi)))
    
    for issue in total_bugs_data["issues"]:
        total_crit += crit_dict[issue["fields"]["priority"]["name"]]
        if tester == issue["fields"]["reporter"]["emailAddress"]:
            pers_crit += crit_dict[issue["fields"]["priority"]["name"]]
    
    try:
        fixed_crit = total_crit*(incl/total_incl)
        kpi_2_value = (pers_crit/fixed_crit)*incl
    except ZeroDivisionError:
        kpi_2_value = 0
    
    return kpi_2_value

def kpi_3(tester, board, incl, total_incl):
    kpi = 3
    
    status = None

    pers_points = 0
    total_points = 0

    com_urls = com_url_creator(requester(url_creator(jql_creator(board, status, kpi))))

    for com_url in com_urls:
        com_data = com_requester(com_url)
        for comment in com_data["comments"]:
            value = None
            mail = comment["author"]["emailAddress"]
            text = comment["body"]
            if "t: " in str(comment["body"]):
                try:
                    search_in = text.partition("t: ")[2]
                    value = re.search(r'\d+', search_in).group()
                except AttributeError:
                    value = None
    
            elif "t - " in str(comment["body"]):
                try:
                    search_in = text.partition("t - ")[2]
                    value = re.search(r'\d+', search_in).group()
                except AttributeError:
                    value = None

            elif " = " in str(comment["body"]):
                try:
                    search_in = text.partition(" = ")[2]
                    value = re.search(r'\d+', search_in).group()
                except AttributeError:
                    value = None

            if value != None:
                if int(value) > 20:
                    value = None
                else:
                    total_points += int(value)
                    if mail == tester:
                        pers_points += int(value)

    try:
        fixed_points = total_points*(incl/total_incl)
        kpi_3_value = (pers_points/fixed_points)*incl
    except ZeroDivisionError:
        kpi_3_value = 0

    return kpi_3_value

def total_kpi(kpi_1_value, kpi_2_value, kpi_3_value):
    all_kpi = [int(kpi_1_value), int(kpi_2_value), int(kpi_3_value)]
    ttc = 0
    summ = 0
    for i in range(0,2):
        if all_kpi[i] != 0:
            ttc += 1
        summ += all_kpi[i]

    if ttc != 0:
        summ /= ttc
    return summ

def incl_func():
    t1 = []
    t2 = []
    t3 = []
    for t in testers:
        for b in boards:
            includness = int(input("Вовлеченность "+ t + " в проект "+ b + " : "))
            if testers.index(t) == 0:
                t1.append(includness)
            elif testers.index(t) == 1:
                t2.append(includness)
            else:
                t3.append(includness)

    return t1, t2, t3

def filter_f(kpi1, kpi2, kpi3):
    kpi_p_pr = [kpi1, kpi2, kpi3]
    return kpi1, kpi2, kpi3, kpi_p_pr

def start():
    t1, t2, t3 = incl_func()
    for tester in testers:
        if testers.index(tester) == 0:
            pr_list = t1
        elif testers.index(tester) == 1:
            pr_list = t2
        else:
            pr_list = t3

        for board in boards:
            pr_num = boards.index(board)
            total_incl = t1[pr_num] + t2[pr_num] + t3[pr_num]

            kpi1, kpi2, kpi3, all_kpi = filter_f(kpi_1(tester, board, pr_list[pr_num]),kpi_2(tester, board, pr_list[pr_num], total_incl),kpi_3(tester, board, pr_list[pr_num], total_incl))
            score = total_kpi(kpi1, kpi2, kpi3)
            all_kpi_list.append(all_kpi)
            total_kpi_list.append(score)

            print("у ",tester, " итоговый KPI за проект ", board, " ", score)

    return all_kpi_list, total_kpi_list


#akl, tkl = start()
#tkl_chunks = [tkl[x:x+3] for x in range(0, len(tkl), 3)]
#
#output_file = {
#    "name" : testers,
#    "Итоговые KPI (AWFM, VID, ABCHR)" : tkl_chunks,
#    "kpi 1,2,3 AWFM" : [akl[0], akl[3], akl[6]],
#    "kpi 1,2,3 VID" : [akl[1], akl[4], akl[7]],
#    "kpi 1,2,3 ABCHR" : [akl[2], akl[5], akl[8]]
#}
#
#df = pd.DataFrame(output_file ,columns=["name", "Итоговые KPI (AWFM, VID, ABCHR)", "kpi 1,2,3 AWFM", "kpi 1,2,3 VID", "kpi 1,2,3 ABCHR"])
#df.to_csv(savePath + "\kpi.csv", index=False)

kpi_3("---","ABCHR",100, 300)