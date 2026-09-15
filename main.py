# -*- coding: utf-8 -*-
"""
全球足球赛事自动化工作流（纯历史数据统计推演，不含任何投注建议）
流程：API拉取 -> 数据清洗 -> 量化统计 -> 输出 schedule.json / match_analysis.json / log.txt
支持数据源：Football-Data.org（推荐，免费）与 API-Football（RapidAPI）
未配置 API Key 时自动进入 DEMO 模式（生成模拟数据，便于离线测试流程）。
"""
import json
import math
import os
import sys
import time
import random
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
LOG_PATH = os.path.join(BASE_DIR, "log.txt")
SCHEDULE_PATH = os.path.join(BASE_DIR, "schedule.json")
ANALYSIS_PATH = os.path.join(BASE_DIR, "match_analysis.json")

HTTP_TIMEOUT = 15          # 单次请求超时（秒）
MAX_RETRIES = 2            # 失败自动重试次数（共请求 1+2 次）
RATE_LIMIT_SLEEP = 6.5     # Football-Data.org 免费版 10次/分钟 限速
FORM_N = 10                # 近期战绩统计窗口

# 联赛代码 -> 中文名（Football-Data.org 免费版可用的主流联赛）
FD_LEAGUES = {"PL": "英超", "PD": "西甲", "SA": "意甲", "BL1": "德甲", "FL1": "法甲"}
# API-Football 联赛ID -> 中文名
AF_LEAGUES = {39: "英超", 140: "西甲", 135: "意甲", 78: "德甲", 61: "法甲"}

# 英文队名 -> 中文队名（覆盖五大联赛常见球队，未收录的保留英文）
TEAM_CN = {
    # 英超
    "Manchester United": "曼联", "Liverpool": "利物浦", "Arsenal": "阿森纳",
    "Manchester City": "曼城", "Chelsea": "切尔西", "Tottenham Hotspur": "热刺",
    "Newcastle United": "纽卡斯尔联", "Aston Villa": "阿斯顿维拉",
    "West Ham United": "西汉姆联", "Everton": "埃弗顿",
    "Brighton & Hove Albion": "布莱顿", "Brighton and Hove Albion": "布莱顿",
    "Crystal Palace": "水晶宫", "Fulham": "富勒姆", "Brentford": "布伦特福德",
    "Nottingham Forest": "诺丁汉森林", "Wolverhampton Wanderers": "狼队",
    "AFC Bournemouth": "伯恩茅斯", "Bournemouth": "伯恩茅斯",
    "Leeds United": "利兹联", "Burnley": "伯恩利", "Sunderland": "桑德兰",
    "Leicester City": "莱斯特城", "Southampton FC": "南安普顿", "Southampton": "南安普顿",
    "Ipswich Town": "伊普斯维奇", "Luton Town": "卢顿", "Sheffield United": "谢菲尔德联",
    # 西甲
    "Real Madrid": "皇马", "FC Barcelona": "巴萨", "Barcelona": "巴萨",
    "Atlético Madrid": "马竞", "Atletico Madrid": "马竞", "Club Atlético de Madrid": "马竞",
    "Sevilla FC": "塞维利亚", "Sevilla": "塞维利亚", "Villarreal CF": "比利亚雷亚尔",
    "Villarreal": "比利亚雷亚尔", "Real Sociedad": "皇家社会",
    "Real Betis": "皇家贝蒂斯", "Real Betis Balompié": "皇家贝蒂斯",
    "Athletic Club": "毕尔巴鄂竞技", "Valencia CF": "瓦伦西亚", "Valencia": "瓦伦西亚",
    "Girona FC": "赫罗纳", "Girona": "赫罗纳", "Celta Vigo": "塞尔塔",
    "RC Celta de Vigo": "塞尔塔", "CA Osasuna": "奥萨苏纳", "Osasuna": "奥萨苏纳",
    "Rayo Vallecano": "巴列卡诺", "RCD Mallorca": "马略卡", "Mallorca": "马略卡",
    "Getafe CF": "赫塔菲", "Getafe": "赫塔菲", "Deportivo Alavés": "阿拉维斯",
    "Alavés": "阿拉维斯", "Alaves": "阿拉维斯", "RCD Espanyol": "西班牙人",
    "Espanyol": "西班牙人", "UD Las Palmas": "拉斯帕尔马斯", "Las Palmas": "拉斯帕尔马斯",
    "CD Leganés": "莱加内斯", "Leganes": "莱加内斯", "Real Valladolid": "巴利亚多利德",
    "Levante UD": "莱万特", "Levante": "莱万特", "Elche CF": "埃尔切", "Elche": "埃尔切",
    "Real Oviedo": "奥维耶多", "Oviedo": "奥维耶多", "UD Almería": "阿尔梅里亚",
    # 意甲
    "Inter": "国际米兰", "Internazionale": "国际米兰", "FC Internazionale Milano": "国际米兰",
    "Juventus": "尤文图斯", "AC Milan": "AC米兰", "Napoli": "那不勒斯",
    "AS Roma": "罗马", "Roma": "罗马", "Lazio": "拉齐奥", "SS Lazio": "拉齐奥",
    "Atalanta": "亚特兰大", "Atalanta BC": "亚特兰大", "Fiorentina": "佛罗伦萨",
    "ACF Fiorentina": "佛罗伦萨", "Bologna": "博洛尼亚", "Bologna FC 1909": "博洛尼亚",
    "Torino": "都灵", "Torino FC": "都灵", "Udinese": "乌迪内斯", "Udinese Calcio": "乌迪内斯",
    "Genoa": "热那亚", "Genoa CFC": "热那亚", "Como 1907": "科莫", "Como": "科莫",
    "Parma": "帕尔马", "Parma Calcio 1913": "帕尔马", "Cagliari": "卡利亚里",
    "Cagliari Calcio": "卡利亚里", "Verona": "维罗纳", "Hellas Verona": "维罗纳",
    "Hellas Verona FC": "维罗纳", "Lecce": "莱切", "US Lecce": "莱切",
    "Empoli": "恩波利", "Empoli FC": "恩波利", "Monza": "蒙扎", "AC Monza": "蒙扎",
    "Venezia": "威尼斯", "Venezia FC": "威尼斯", "Pisa": "比萨", "Pisa SC": "比萨",
    "Cremonese": "克雷莫纳", "US Cremonese": "克雷莫纳", "Sassuolo": "萨索洛",
    "US Sassuolo Calcio": "萨索洛", "Sassuolo Calcio": "萨索洛",
    # 德甲
    "Bayern München": "拜仁慕尼黑", "FC Bayern München": "拜仁慕尼黑",
    "Bayern Munich": "拜仁慕尼黑", "Borussia Dortmund": "多特蒙德",
    "Bayer 04 Leverkusen": "勒沃库森", "Bayer Leverkusen": "勒沃库森",
    "RB Leipzig": "莱比锡红牛", "Eintracht Frankfurt": "法兰克福",
    "VfB Stuttgart": "斯图加特", "VfL Wolfsburg": "沃尔夫斯堡",
    "SC Freiburg": "弗赖堡", "Union Berlin": "柏林联合", "1. FC Union Berlin": "柏林联合",
    "Werder Bremen": "云达不莱梅", "SV Werder Bremen": "云达不莱梅",
    "FSV Mainz 05": "美因茨", "1. FSV Mainz 05": "美因茨",
    "Borussia Mönchengladbach": "门兴格拉德巴赫", "FC Augsburg": "奥格斯堡",
    "TSG Hoffenheim": "霍芬海姆", "TSG 1899 Hoffenheim": "霍芬海姆",
    "Heidenheim": "海登海姆", "1. FC Heidenheim 1846": "海登海姆",
    "FC St. Pauli": "圣保利", "Hamburger SV": "汉堡", "FC Köln": "科隆",
    "1. FC Köln": "科隆",
    # 法甲
    "Paris Saint Germain": "巴黎圣日耳曼", "Paris Saint-Germain": "巴黎圣日耳曼",
    "Paris SG": "巴黎圣日耳曼", "Marseille": "马赛", "Olympique de Marseille": "马赛",
    "Lyon": "里昂", "Olympique Lyonnais": "里昂", "Monaco": "摩纳哥", "AS Monaco": "摩纳哥",
    "Lille": "里尔", "LOSC Lille": "里尔", "Nice": "尼斯", "OGC Nice": "尼斯",
    "Lens": "朗斯", "RC Lens": "朗斯", "Rennes": "雷恩", "Stade Rennais FC": "雷恩",
    "Strasbourg": "斯特拉斯堡", "RC Strasbourg Alsace": "斯特拉斯堡",
    "Toulouse": "图卢兹", "Toulouse FC": "图卢兹", "Nantes": "南特", "FC Nantes": "南特",
    "Brest": "布雷斯特", "Stade Brestois 29": "布雷斯特", "Le Havre": "勒阿弗尔",
    "Le Havre AC": "勒阿弗尔", "Angers": "昂热", "Angers SCO": "昂热",
    "Auxerre": "欧塞尔", "AJ Auxerre": "欧塞尔", "Paris FC": "巴黎FC",
    "Metz": "梅斯", "FC Metz": "梅斯", "Lorient": "洛里昂", "FC Lorient": "洛里昂",
    # 其他常见队名变体
    "Napoli": "那不勒斯", "SSC Napoli": "那不勒斯", "Lille OSC": "里尔",
    "Stade Rennais FC 1901": "雷恩", "Racing Club de Lens": "朗斯",
    "Le Mans FC": "勒芒", "ES Troyes AC": "特鲁瓦", "Málaga CF": "马拉加",
    "RC Deportivo de La Coruña": "拉科鲁尼亚", "RC Deportivo La Coruña": "拉科鲁尼亚",
    "Real Racing Club de Santander": "桑坦德竞技", "RCD Espanyol de Barcelona": "西班牙人",
    "Real Madrid CF": "皇马", "Real Sociedad de Fútbol": "皇家社会",
    "Frosinone Calcio": "弗罗西诺内", "SC Paderborn 07": "帕德博恩",
    "FC Schalke 04": "沙尔克04", "SV 07 Elversberg": "埃尔沃斯堡",
    "Coventry City FC": "考文垂", "Coventry City": "考文垂",
    "Hull City AFC": "赫尔城", "Hull City": "赫尔城",
    "Rayo Vallecano de Madrid": "巴列卡诺", "AS Monaco FC": "摩纳哥",
    "Paris Saint-Germain FC": "巴黎圣日耳曼", "Manchester United FC": "曼联",
    "Liverpool FC": "利物浦", "Arsenal FC": "阿森纳", "Chelsea FC": "切尔西",
    "Manchester City FC": "曼城", "Everton FC": "埃弗顿", "Tottenham Hotspur FC": "热刺",
    "Newcastle United FC": "纽卡斯尔联", "Aston Villa FC": "阿斯顿维拉",
    "Brighton & Hove Albion FC": "布莱顿", "Crystal Palace FC": "水晶宫",
    "Brentford FC": "布伦特福德", "Nottingham Forest FC": "诺丁汉森林",
    "Leeds United FC": "利兹联", "Sunderland AFC": "桑德兰", "Ipswich Town FC": "伊普斯维奇",
    "Juventus FC": "尤文图斯", "West Bromwich Albion": "西布罗姆维奇",
}


def load_config():
    cfg = {
        "provider": os.environ.get("FOOTBALL_PROVIDER", "football-data"),
        "api_key": os.environ.get("FOOTBALL_API_KEY", ""),
        "fd_api_key": os.environ.get("FD_API_KEY", ""),
        "af_api_key": os.environ.get("AF_API_KEY", ""),
    }
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception as e:
            log_line("WARN 读取 config.json 失败: %s" % e)
    return cfg


def log_line(msg, log_lines=None):
    """同时打印并收集日志行"""
    line = "[%s] %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg)
    print(line)
    if log_lines is not None:
        log_lines.append(line)


def http_get_json(url, headers, retries=MAX_RETRIES, timeout=HTTP_TIMEOUT):
    """带重试的 HTTP GET，全部失败抛出异常"""
    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 * (attempt + 1))
    raise last_err


# ---------------- 步骤1：API 拉取 ----------------

def fetch_football_data(cfg, log_lines):
    """Football-Data.org：拉取各联赛未开赛 + 已完赛比赛"""
    key = cfg.get("fd_api_key") or cfg.get("api_key")
    headers = {"X-Auth-Token": key}
    upcoming, finished = [], []
    for code, cn in FD_LEAGUES.items():
        for status, bucket in (("SCHEDULED", upcoming), ("FINISHED", finished)):
            url = "https://api.football-data.org/v4/competitions/%s/matches?status=%s" % (code, status)
            try:
                data = http_get_json(url, headers)
                for m in data.get("matches", []):
                    bucket.append({
                        "id": str(m.get("id")),
                        "league": cn,
                        "utcDate": m.get("utcDate", ""),
                        "home": m.get("homeTeam", {}).get("name", ""),
                        "away": m.get("awayTeam", {}).get("name", ""),
                        "home_id": str(m.get("homeTeam", {}).get("id", "")),
                        "away_id": str(m.get("awayTeam", {}).get("id", "")),
                        "status": "finished" if status == "FINISHED" else "upcoming",
                        "hg": (m.get("score", {}).get("fullTime", {}) or {}).get("home"),
                        "ag": (m.get("score", {}).get("fullTime", {}) or {}).get("away"),
                    })
                log_line("INFO 抓取 %s %s 完成" % (cn, status))
            except Exception as e:
                log_line("ERROR 抓取 %s(%s) 失败: %s" % (cn, status, e), log_lines)
            time.sleep(RATE_LIMIT_SLEEP)
    return upcoming, finished


def fetch_api_football(cfg, log_lines):
    """API-Football (RapidAPI)：拉取各联赛本赛季未开赛 + 已完赛比赛"""
    key = cfg.get("af_api_key") or cfg.get("api_key")
    headers = {"x-rapidapi-key": key, "x-rapidapi-host": "api-football-v1.p.rapidapi.com"}
    season = date.today().year if date.today().month >= 7 else date.today().year - 1
    upcoming, finished = [], []
    for lid, cn in AF_LEAGUES.items():
        for status, bucket in (("NS", upcoming), ("FT", finished)):
            url = ("https://api-football-v1.p.rapidapi.com/v3/fixtures?league=%d&season=%d&status=%s"
                   % (lid, season, status))
            try:
                data = http_get_json(url, headers)
                for m in data.get("response", []):
                    fx, teams, goals = m.get("fixture", {}), m.get("teams", {}), m.get("goals", {})
                    bucket.append({
                        "id": str(fx.get("id")),
                        "league": cn,
                        "utcDate": fx.get("date", ""),
                        "home": teams.get("home", {}).get("name", ""),
                        "away": teams.get("away", {}).get("name", ""),
                        "home_id": str(teams.get("home", {}).get("id", "")),
                        "away_id": str(teams.get("away", {}).get("id", "")),
                        "status": "finished" if status == "FT" else "upcoming",
                        "hg": goals.get("home"),
                        "ag": goals.get("away"),
                    })
                log_line("INFO 抓取 %s %s 完成" % (cn, status))
            except Exception as e:
                log_line("ERROR 抓取 %s(%s) 失败: %s" % (cn, status, e), log_lines)
            time.sleep(RATE_LIMIT_SLEEP)
    return upcoming, finished


def fetch_demo(log_lines):
    """DEMO 模式：无 API Key 时生成确定性模拟数据，保证全流程可离线验证"""
    random.seed(20260914)
    leagues = ["英超", "西甲", "意甲", "德甲", "法甲"]
    teams = {
        "英超": ["曼联", "利物浦", "阿森纳", "曼城", "切尔西", "热刺"],
        "西甲": ["皇马", "巴萨", "马竞", "塞维利亚", "比利亚雷亚尔", "皇家社会"],
        "意甲": ["国际米兰", "尤文图斯", "AC米兰", "那不勒斯", "罗马", "拉齐奥"],
        "德甲": ["拜仁慕尼黑", "多特蒙德", "勒沃库森", "莱比锡", "法兰克福", "斯图加特"],
        "法甲": ["巴黎圣日耳曼", "马赛", "里昂", "摩纳哥", "里尔", "尼斯"],
    }
    today = date.today()
    upcoming, finished = [], []
    mid = 0
    for li, lg in enumerate(leagues):
        pool = teams[lg][:]
        random.shuffle(pool)
        # 已完赛：每联赛 6 队单循环 15 场（过去45天内），保证每队有足够近期战绩
        n_teams = len(pool)
        for i in range(n_teams):
            for j in range(i + 1, n_teams):
                mid += 1
                d = today - timedelta(days=1 + (mid % 45))
                hg, ag = random.randint(0, 4), random.randint(0, 3)
                finished.append({
                    "id": "m%03d" % mid, "league": lg,
                    "utcDate": "%sT%02d:00:00Z" % (d.isoformat(), 13 + (mid % 6)),
                    "home": pool[i], "away": pool[j],
                    "home_id": "%s-%d" % (lg, i), "away_id": "%s-%d" % (lg, j),
                    "status": "finished", "hg": hg, "ag": ag,
                })
        # 未开赛：每联赛 3 场（未来7天内）
        for i in range(0, 6, 2):
            mid += 1
            d = today + timedelta(days=mid % 5)
            upcoming.append({
                "id": "m%03d" % mid, "league": lg,
                "utcDate": "%sT%02d:00:00Z" % (d.isoformat(), 19 + (mid % 3)),
                "home": pool[i], "away": pool[i + 1],
                "home_id": "%s-%d" % (lg, i), "away_id": "%s-%d" % (lg, i + 1),
                "status": "upcoming", "hg": None, "ag": None,
            })
    log_line("INFO DEMO模式生成模拟比赛 %d 场（未开赛 %d / 已完赛 %d）"
             % (len(upcoming) + len(finished), len(upcoming), len(finished)), log_lines)
    return upcoming, finished


# ---------------- 步骤2：数据清洗 ----------------

def clean_data(upcoming, finished, log_lines):
    """去重、剔除无效比赛、统一日期时间格式、标准化名称"""
    seen, cleaned_up, cleaned_fin = set(), [], []

    def normalize_team(name):
        name = (name or "").strip()
        if name in TEAM_CN:
            return TEAM_CN[name]
        # 去掉常见后缀（FC/AFC/CF/SC 等）再查一次
        for suf in (" FC", " AFC", " CF", " SC"):
            if name.endswith(suf) and name[: -len(suf)] in TEAM_CN:
                return TEAM_CN[name[: -len(suf)]]
        return name  # 未收录的球队保留英文名

    def parse_dt(utc):
        try:
            dt = datetime.strptime(utc[:16], "%Y-%m-%dT%H:%M")
            return (dt + timedelta(hours=8)).strftime("%Y-%m-%d"), (dt + timedelta(hours=8)).strftime("%H:%M")
        except Exception:
            return utc[:10] or "1970-01-01", "00:00"

    def dedup_add(bucket, m):
        key = (m["league"], m["home"], m["away"], m["date"])
        if key in seen or not m["home"] or not m["away"]:
            return False
        seen.add(key)
        bucket.append(m)
        return True

    for m in finished:
        if m.get("hg") is None or m.get("ag") is None:
            continue  # 无比分的完赛记录视为无效
        d, t = parse_dt(m["utcDate"])
        cleaned_fin.append({
            "id": m["id"], "league": m["league"], "date": d, "time": t,
            "home": normalize_team(m["home"]), "away": normalize_team(m["away"]),
            "home_id": m.get("home_id", ""), "away_id": m.get("away_id", ""),
            "status": "finished", "score": "%s-%s" % (m["hg"], m["ag"]),
            "hg": m["hg"], "ag": m["ag"],
        })
    for m in upcoming:
        d, t = parse_dt(m["utcDate"])
        cleaned_up.append({
            "id": m["id"], "league": m["league"], "date": d, "time": t,
            "home": normalize_team(m["home"]), "away": normalize_team(m["away"]),
            "home_id": m.get("home_id", ""), "away_id": m.get("away_id", ""),
            "status": "upcoming", "score": "", "hg": None, "ag": None,
        })

    all_matches = sorted(cleaned_up + cleaned_fin, key=lambda x: (x["date"], x["time"]))
    log_line("INFO 清洗后有效比赛 %d 场（未开赛 %d / 已完赛 %d）"
             % (len(all_matches), len(cleaned_up), len(cleaned_fin)), log_lines)
    return all_matches


# ---------------- 统计工具（泊松模型） ----------------

def build_team_stats(finished_matches):
    """从已完赛比赛池计算每队近10场：场均进球、场均失球、胜/平/负"""
    by_team = {}
    for m in finished_matches:
        for side, tid, name, gf, ga in (
            ("home", m.get("home_id") or m["home"], m["home"], m["hg"], m["ag"]),
            ("away", m.get("away_id") or m["away"], m["away"], m["ag"], m["hg"]),
        ):
            by_team.setdefault(tid, []).append({
                "date": m["date"], "name": name, "gf": gf, "ga": ga,
                "result": "win" if gf > ga else ("draw" if gf == ga else "loss"),
            })
    stats = {}
    for tid, games in by_team.items():
        games.sort(key=lambda g: g["date"], reverse=True)
        recent = games[:FORM_N]
        n = len(recent)
        if n == 0:
            continue
        stats[tid] = {
            "name": recent[0]["name"],
            "n": n,
            "avg_gf": sum(g["gf"] for g in recent) / n,
            "avg_ga": sum(g["ga"] for g in recent) / n,
            "w": sum(1 for g in recent if g["result"] == "win"),
            "d": sum(1 for g in recent if g["result"] == "draw"),
            "l": sum(1 for g in recent if g["result"] == "loss"),
            "recent5": [g["result"] for g in recent[:5]],
        }
    return stats


def build_h2h(finished_matches):
    """历史交锋：(主队id, 客队id) 有序对 -> 场次列表"""
    h2h = {}
    for m in finished_matches:
        key = tuple(sorted([m.get("home_id") or m["home"], m.get("away_id") or m["away"]]))
        h2h.setdefault(key, []).append({"date": m["date"], "hg": m["hg"], "ag": m["ag"]})
    return h2h


def poisson_pmf(lam, k):
    return math.exp(-lam) * lam ** k / math.factorial(k)


# ---------------- 步骤3：量化统计分析（仅未开赛） ----------------

def analyze_match(match, team_stats, h2h):
    """泊松模型估算：预期进球、总进球分布、大小球、胜平负。数据不足返回 None。"""
    h = team_stats.get(match.get("home_id") or match["home"])
    a = team_stats.get(match.get("away_id") or match["away"])
    if not h or not a or h["n"] < 3 or a["n"] < 3:
        return None  # 数据缺失，跳过分析

    # 预期进球：主客攻防均值 + 主场优势修正，再与历史交锋场均进球融合
    lam_home = (h["avg_gf"] + a["avg_ga"]) / 2 * 1.10
    lam_away = (a["avg_gf"] + h["avg_ga"]) / 2 * 0.95
    pair = tuple(sorted([match.get("home_id") or match["home"], match.get("away_id") or match["away"]]))
    games = h2h.get(pair, [])
    if games:
        h2h_avg = sum(g["hg"] + g["ag"] for g in games) / len(games)
        blend = 0.7 * (lam_home + lam_away) + 0.3 * h2h_avg
        if lam_home + lam_away > 0:
            lam_home *= blend / (lam_home + lam_away)
            lam_away *= blend / (lam_home + lam_away)
    lam_home = max(0.2, min(lam_home, 4.0))
    lam_away = max(0.15, min(lam_away, 4.0))
    xg_total = lam_home + lam_away

    # 总进球分布（两队独立泊松卷积，网格 0-10）
    N = 10
    ph = [poisson_pmf(lam_home, k) for k in range(N + 1)]
    pa = [poisson_pmf(lam_away, k) for k in range(N + 1)]
    ptot = [0.0] * 11
    p_home_win = p_draw = p_away_win = 0.0
    for i in range(N + 1):
        for j in range(N + 1):
            p = ph[i] * pa[j]
            k = min(i + j, 10)
            ptot[k] += p
            if i > j:
                p_home_win += p
            elif i == j:
                p_draw += p
            else:
                p_away_win += p
    tail = 1.0 - sum(ptot[:10])
    ptot[10] += tail

    def pct(x):
        return "%.0f%%" % (round(x * 100))

    # 归一化保证合计 100%
    s = sum(ptot)
    dist = [x / s for x in ptot]
    p0, p1, p2, p3 = dist[0], dist[1], dist[2], dist[3]
    p4plus = 1 - (p0 + p1 + p2 + p3)
    over25 = p3 + p4plus + dist[4] * 0  # p3 已含 3 球，over2.5 = P(>=3)
    over25 = 1 - (p0 + p1 + p2)
    under25 = 1 - over25
    s2 = p_home_win + p_draw + p_away_win
    hw, dr, aw = p_home_win / s2, p_draw / s2, p_away_win / s2

    # 综合文字总结（纯历史统计描述）

    parts = ["%s近期表现：近%d场%d胜%d平%d负，场均进球%.2f、失球%.2f"
             % (match["home"], h["n"], h["w"], h["d"], h["l"], h["avg_gf"], h["avg_ga"]),
             "%s近期表现：近%d场%d胜%d平%d负，场均进球%.2f、失球%.2f"
             % (match["away"], a["n"], a["w"], a["d"], a["l"], a["avg_gf"], a["avg_ga"])]
    if games:
        parts.append("两队历史交锋%d场，场均总进球%.2f" % (len(games), sum(g["hg"] + g["ag"] for g in games) / len(games)))
    else:
        parts.append("两队近期无直接交锋记录")
    if over25 >= 0.5:
        parts.append("历史攻防数据显示总进球期望偏高")
    else:
        parts.append("历史攻防数据显示总进球期望偏低")
    summary = "；".join(parts) + "。足球存在偶然性，仅历史数据统计。"

    return {
        "id": match["id"],
        "league": match["league"],
        "home": match["home"],
        "away": match["away"],
        "date": match["date"],
        "time": match["time"],
        "xg": "%.2f" % xg_total,
        "prob_0": pct(p0),
        "prob_1": pct(p1),
        "prob_2": pct(p2),
        "prob_3": pct(p3),
        "prob_4plus": pct(p4plus),
        "over25": pct(over25),
        "under25": pct(under25),
        "home_win": pct(hw),
        "draw": pct(dr),
        "away_win": pct(aw),
        "summary": summary,
    }


# ---------------- 步骤4/5：输出 ----------------

def write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def ensure_html():
    """HTML 只在首次生成，之后不覆盖"""
    index_path = os.path.join(BASE_DIR, "index.html")
    detail_path = os.path.join(BASE_DIR, "detail.html")
    if os.path.exists(index_path) and os.path.exists(detail_path):
        return False
    here = os.path.dirname(os.path.abspath(__file__))
    src = os.path.join(here, "_html_templates")
    if os.path.isdir(src):
        import shutil
        if not os.path.exists(index_path):
            shutil.copyfile(os.path.join(src, "index.html"), index_path)
        if not os.path.exists(detail_path):
            shutil.copyfile(os.path.join(src, "detail.html"), detail_path)
        return True
    return False


def main():
    start = time.time()
    log_lines = []
    log_line("===== 工作流运行开始 =====", log_lines)
    cfg = load_config()
    has_key = bool(cfg.get("fd_api_key") or cfg.get("af_api_key") or cfg.get("api_key"))
    mode = "LIVE(%s)" % cfg["provider"] if has_key else "DEMO(未配置API Key)"
    log_line("INFO 运行模式: %s" % mode, log_lines)

    errors = []
    try:
        if not has_key:
            upcoming, finished = fetch_demo(log_lines)
        elif cfg["provider"] == "api-football":
            upcoming, finished = fetch_api_football(cfg, log_lines)
        else:
            upcoming, finished = fetch_football_data(cfg, log_lines)
    except Exception as e:
        errors.append("数据拉取阶段异常: %s" % e)
        log_line("ERROR 数据拉取阶段异常: %s（本次不中断整体工作流）" % e, log_lines)
        upcoming, finished = [], []

    # 步骤2 清洗
    all_matches = clean_data(upcoming, finished, log_lines)

    # 保护机制：已配置 Key 但实时数据拉取为空（如 Key 失效/网络故障），回退 DEMO 数据，保证页面不空白
    if has_key and len(all_matches) == 0:
        log_line("WARN 实时数据拉取为空，自动回退 DEMO 演示数据（请检查 config.json 中的 API Key）", log_lines)
        errors.append("实时数据拉取为空，已回退DEMO数据")
        upcoming, finished = fetch_demo(log_lines)
        all_matches = clean_data(upcoming, finished, log_lines)

    # 步骤3 统计分析（未开赛+近期完赛，完赛比赛保留赛前预测用于对比）
    ARCHIVE_PATH = os.path.join(BASE_DIR, "predictions_history.json")
    archive = {}
    if os.path.exists(ARCHIVE_PATH):
        try:
            with open(ARCHIVE_PATH, "r", encoding="utf-8") as f:
                archive = json.load(f)
        except Exception:
            archive = {}

    all_finished = [m for m in all_matches if m["status"] == "finished"]
    team_stats = build_team_stats(all_finished)
    h2h = build_h2h(all_finished)
    analysis, skipped = [], 0
    for m in all_matches:
        try:
            if m["id"] in archive:
                # 使用存档中该场真正的赛前预测（比赛开赛前生成，永不改写）
                r = dict(archive[m["id"]])
                # 预测数值保留存档，显示信息（队名翻译等）用当前数据刷新
                r.update({"league": m["league"], "home": m["home"], "away": m["away"],
                          "date": m["date"], "time": m["time"]})
            else:
                if m["status"] == "finished":
                    # 完赛且无存档：仅用该场比赛之前的完赛数据做回溯推演
                    pre = [x for x in all_finished if x["date"] < m["date"]]
                    r = analyze_match(m, build_team_stats(pre), build_h2h(pre))
                else:
                    r = analyze_match(m, team_stats, h2h)
                if r is not None:
                    archive[m["id"]] = r
            if r is None:
                skipped += 1
                log_line("INFO 跳过分析(数据缺失): %s vs %s" % (m["home"], m["away"]), log_lines)
                continue
            r = dict(r)
            r["status"] = m["status"]
            r["score"] = m["score"]
            analysis.append(r)
        except Exception as e:
            skipped += 1
            errors.append("分析失败 %s: %s" % (m["id"], e))
            log_line("ERROR 分析失败 %s(%s vs %s): %s" % (m["id"], m["home"], m["away"], e), log_lines)

    write_json(ARCHIVE_PATH, archive)
    log_line("INFO 预测存档 predictions_history.json 共 %d 条" % len(archive), log_lines)

    # 步骤4 输出 JSON（覆盖更新）
    schedule_out = [{"id": m["id"], "league": m["league"], "date": m["date"], "time": m["time"],
                     "home": m["home"], "away": m["away"], "status": m["status"], "score": m["score"]}
                    for m in all_matches]
    write_json(SCHEDULE_PATH, schedule_out)
    write_json(ANALYSIS_PATH, analysis)
    log_line("INFO 已覆盖更新 schedule.json（%d场）、match_analysis.json（%d场）"
             % (len(schedule_out), len(analysis)), log_lines)

    # 步骤5 HTML 仅首次生成
    if ensure_html():
        log_line("INFO 首次生成 index.html / detail.html", log_lines)

    # 汇总日志
    fetched = len(schedule_out)
    analyzed = len(analysis)
    failed = skipped + len(errors)
    log_line("----- 本次运行汇总 -----", log_lines)
    log_line("抓取比赛总数: %d" % fetched, log_lines)
    log_line("成功分析场次: %d" % analyzed, log_lines)
    log_line("失败/跳过场次: %d" % failed, log_lines)
    log_line("错误信息: %s" % ("；".join(errors) if errors else "无"), log_lines)
    log_line("运行耗时: %.1f 秒" % (time.time() - start), log_lines)
    log_line("===== 工作流运行结束 =====", log_lines)

    with open(LOG_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines) + "\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write("[%s] FATAL 全局异常: %s\n" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e))
        print("FATAL:", e)
        sys.exit(1)
