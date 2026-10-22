"""合并节点、生成固定订阅（Clash 配置 + v2ray 订阅）。"""

import base64
import hashlib
import json
import logging
import math
import re
import unicodedata
from collections import defaultdict
from urllib.parse import quote

import yaml

log = logging.getLogger("datiya.build")

# ---- 地区识别 ----
# 一张表打天下：ISO 代码 → (中文名, 其它写法)。
# 为什么不再用原来那 16 行小表：上游名字里的地区写法比它杂得多——实测线上 390 个
# 节点里出现了 47 个不同国家码（US 92 / RO 52 / NL 35 / SG 23 / DE 19 / FR 17 …），
# 旧表只认 16 个国家，四成节点被归进「其他」：分组塌成一个巨型「其他」，也没法用
# 地区给节点命名（名字开头会是「其他」，等于把唯一确定的信息丢了）。
#
# 三档匹配，越靠前越可信：
#   ① 国旗 emoji（区域指示符两两成对，几乎不会错）
#   ② 中文名 / 英文全名 走子串匹配（"新加坡"、"singapore"、"united states"）
#   ③ 短代码按**单词**匹配（"US"、"SG"），取名字里**最先出现**的那个
#      —— 上游习惯把地区写在开头；"RO - zhuhai.uk" 里的 uk 是域名的一部分，
#      按出现顺序取第一个才能得到 RO 而不是 GB。
REGION_TABLE = [
    ("HK", "香港", ("hongkong", "hong kong", "hkg", "港")),
    ("TW", "台湾", ("台灣", "taiwan", "tpe", "台北", "taipei")),
    ("MO", "澳门", ("澳門", "macau", "macao")),
    ("SG", "新加坡", ("狮城", "獅城", "singapore", "sgp")),
    ("JP", "日本", ("japan", "jpn", "东京", "東京", "大阪", "tokyo", "osaka")),
    ("KR", "韩国", ("韓國", "korea", "首尔", "首爾", "seoul", "busan")),
    ("CN", "中国", ("中国大陆", "china")),
    ("US", "美国", ("美國", "united states", "america", "usa", "洛杉矶", "硅谷",
                    "los angeles", "san jose", "seattle", "dallas", "new york")),
    ("CA", "加拿大", ("canada", "多伦多", "蒙特利尔", "toronto", "montreal")),
    ("GB", "英国", ("英國", "united kingdom", "britain", "england", "london", "伦敦", "倫敦")),
    ("DE", "德国", ("德國", "germany", "frankfurt", "法兰克福")),
    ("FR", "法国", ("法國", "france", "paris", "巴黎")),
    ("NL", "荷兰", ("荷蘭", "netherlands", "holland", "amsterdam", "阿姆斯特丹")),
    ("BE", "比利时", ("belgium",)),
    ("CH", "瑞士", ("switzerland", "zurich", "苏黎世")),
    ("AT", "奥地利", ("austria", "vienna", "维也纳")),
    ("IT", "意大利", ("義大利", "italy", "milan", "米兰")),
    ("ES", "西班牙", ("spain", "madrid", "马德里")),
    ("PT", "葡萄牙", ("portugal", "lisbon", "里斯本")),
    ("IE", "爱尔兰", ("愛爾蘭", "ireland", "dublin", "都柏林")),
    ("SE", "瑞典", ("sweden", "stockholm", "斯德哥尔摩")),
    ("NO", "挪威", ("norway", "oslo", "奥斯陆")),
    ("DK", "丹麦", ("丹麥", "denmark", "copenhagen", "哥本哈根")),
    ("FI", "芬兰", ("芬蘭", "finland", "helsinki", "赫尔辛基")),
    ("IS", "冰岛", ("冰島", "iceland")),
    ("PL", "波兰", ("波蘭", "poland", "warsaw", "华沙")),
    ("CZ", "捷克", ("czech", "prague", "布拉格")),
    ("SK", "斯洛伐克", ("slovakia",)),
    ("HU", "匈牙利", ("hungary", "budapest", "布达佩斯")),
    ("RO", "罗马尼亚", ("羅馬尼亞", "romania", "bucharest", "布加勒斯特")),
    ("BG", "保加利亚", ("保加利亞", "bulgaria", "sofia", "索非亚")),
    ("GR", "希腊", ("希臘", "greece", "athens", "雅典")),
    ("HR", "克罗地亚", ("克羅地亞", "croatia", "zagreb")),
    ("SI", "斯洛文尼亚", ("slovenia",)),
    ("RS", "塞尔维亚", ("塞爾維亞", "serbia", "belgrade", "贝尔格莱德")),
    ("BA", "波黑", ("bosnia",)),
    ("AL", "阿尔巴尼亚", ("阿爾巴尼亞", "albania")),
    ("MT", "马耳他", ("馬耳他", "malta")),
    ("LU", "卢森堡", ("盧森堡", "luxembourg")),
    ("EE", "爱沙尼亚", ("愛沙尼亞", "estonia", "tallinn", "塔林")),
    ("LV", "拉脱维亚", ("拉脫維亞", "latvia", "riga", "里加")),
    ("LT", "立陶宛", ("lithuania", "vilnius", "维尔纽斯")),
    ("UA", "乌克兰", ("烏克蘭", "ukraine", "kyiv", "基辅")),
    ("MD", "摩尔多瓦", ("moldova",)),
    ("BY", "白俄罗斯", ("belarus", "minsk", "明斯克")),
    ("RU", "俄罗斯", ("俄羅斯", "russia", "moscow", "莫斯科")),
    ("TR", "土耳其", ("turkey", "istanbul", "伊斯坦布尔")),
    ("IL", "以色列", ("israel", "tel aviv", "特拉维夫")),
    ("AE", "阿联酋", ("阿聯酋", "emirates", "uae", "dubai", "迪拜")),
    ("SA", "沙特", ("saudi", "riyadh")),
    ("QA", "卡塔尔", ("卡塔爾", "qatar", "doha")),
    ("KW", "科威特", ("kuwait",)),
    ("BH", "巴林", ("bahrain",)),
    ("JO", "约旦", ("約旦", "jordan", "amman")),
    ("LB", "黎巴嫩", ("lebanon", "beirut")),
    ("IQ", "伊拉克", ("iraq", "baghdad")),
    ("IR", "伊朗", ("iran", "tehran")),
    ("AM", "亚美尼亚", ("armenia", "yerevan")),
    ("AZ", "阿塞拜疆", ("azerbaijan", "baku")),
    ("GE", "格鲁吉亚", ("georgia", "tbilisi")),
    ("KZ", "哈萨克斯坦", ("kazakhstan", "almaty")),
    ("UZ", "乌兹别克斯坦", ("uzbekistan",)),
    ("KG", "吉尔吉斯斯坦", ("kyrgyzstan",)),
    ("MN", "蒙古", ("mongolia",)),
    ("PK", "巴基斯坦", ("pakistan", "karachi")),
    ("BD", "孟加拉", ("bangladesh", "dhaka")),
    ("NP", "尼泊尔", ("nepal", "kathmandu")),
    ("LK", "斯里兰卡", ("sri lanka", "colombo")),
    ("MM", "缅甸", ("緬甸", "myanmar", "yangon")),
    ("TH", "泰国", ("泰國", "thailand", "bangkok", "曼谷")),
    ("VN", "越南", ("vietnam", "hanoi", "河内", "胡志明")),
    ("LA", "老挝", ("laos", "vientiane")),
    ("KH", "柬埔寨", ("cambodia", "phnom penh")),
    ("MY", "马来西亚", ("馬來西亞", "malaysia", "kuala lumpur", "吉隆坡")),
    ("ID", "印度尼西亚", ("印尼", "indonesia", "jakarta", "雅加达")),
    ("PH", "菲律宾", ("菲律賓", "philippines", "manila", "马尼拉")),
    ("BN", "文莱", ("brunei",)),
    ("IN", "印度", ("india", "mumbai", "孟买")),
    ("AU", "澳大利亚", ("澳大利亞", "澳洲", "australia", "sydney", "悉尼")),
    ("NZ", "新西兰", ("新西蘭", "new zealand", "auckland", "奥克兰")),
    ("BR", "巴西", ("brazil", "sao paulo", "圣保罗")),
    ("AR", "阿根廷", ("argentina", "buenos aires")),
    ("CL", "智利", ("chile", "santiago")),
    ("PE", "秘鲁", ("秘魯", "peru", "lima")),
    ("CO", "哥伦比亚", ("colombia", "bogota")),
    ("VE", "委内瑞拉", ("venezuela",)),
    ("EC", "厄瓜多尔", ("ecuador", "quito")),
    ("UY", "乌拉圭", ("uruguay",)),
    ("PY", "巴拉圭", ("paraguay",)),
    ("BO", "玻利维亚", ("bolivia",)),
    ("MX", "墨西哥", ("mexico", "墨西哥城")),
    ("GT", "危地马拉", ("guatemala",)),
    ("CR", "哥斯达黎加", ("costa rica",)),
    ("PA", "巴拿马", ("巴拿馬", "panama")),
    ("DO", "多米尼加", ("dominican",)),
    ("PR", "波多黎各", ("puerto rico",)),
    ("JM", "牙买加", ("jamaica",)),
    ("TT", "特立尼达", ("trinidad",)),
    ("ZA", "南非", ("south africa", "johannesburg")),
    ("NG", "尼日利亚", ("nigeria", "lagos")),
    ("KE", "肯尼亚", ("kenya", "nairobi")),
    ("EG", "埃及", ("egypt", "cairo")),
    ("MA", "摩洛哥", ("morocco", "casablanca")),
    ("TN", "突尼斯", ("tunisia",)),
    ("DZ", "阿尔及利亚", ("algeria",)),
    ("GH", "加纳", ("ghana",)),
    ("ET", "埃塞俄比亚", ("ethiopia",)),
    ("TZ", "坦桑尼亚", ("tanzania",)),
    ("UG", "乌干达", ("uganda",)),
    ("MU", "毛里求斯", ("mauritius",)),
    ("CM", "喀麦隆", ("cameroon",)),
    ("SN", "塞内加尔", ("senegal",)),
    ("CI", "科特迪瓦", ("ivory coast",)),
    ("ZW", "津巴布韦", ("zimbabwe",)),
    ("AO", "安哥拉", ("angola",)),
    ("MZ", "莫桑比克", ("mozambique",)),
    ("BW", "博茨瓦纳", ("botswana",)),
    ("NA", "纳米比亚", ("namibia",)),
    ("CY", "塞浦路斯", ("cyprus", "nicosia")),
]

# 常见三字码 / 机场码：上游节点名里大量使用（RUS、NRT、LAX、SIN…），
# 只按**单词**匹配，且只用来剥「名字开头」的地区前缀，不动中间的部分。
ALPHA3 = {
    "hkg": "HK", "tpe": "TW", "mac": "MO", "sin": "SG", "sgp": "SG",
    "nrt": "JP", "hnd": "JP", "kix": "JP", "icn": "KR", "lax": "US",
    "sjc": "US", "sea": "US", "dfw": "US", "iad": "US", "nyc": "US",
    "usa": "US", "yyz": "CA", "lhr": "GB", "lon": "GB", "gbr": "GB",
    "fra": "DE", "deu": "DE", "cdg": "FR", "par": "FR", "ams": "NL",
    "nld": "NL", "bru": "BE", "zrh": "CH", "vie": "AT", "aut": "AT",
    "mxp": "IT", "ita": "IT", "mad": "ES", "esp": "ES", "lis": "PT",
    "prt": "PT", "dub": "IE", "irl": "IE", "arn": "SE", "swe": "SE",
    "osl": "NO", "nor": "NO", "cph": "DK", "dnk": "DK", "hel": "FI",
    "fin": "FI", "waw": "PL", "pol": "PL", "prg": "CZ", "cze": "CZ",
    "bud": "HU", "hun": "HU", "otp": "RO", "rou": "RO", "sof": "BG",
    "bgr": "BG", "ath": "GR", "grc": "GR", "zag": "HR", "hrv": "HR",
    "beg": "RS", "srb": "RS", "bih": "BA", "tia": "AL", "alb": "AL",
    "mla": "MT", "lux": "LU", "tal": "EE", "est": "EE", "rix": "LV",
    "lva": "LV", "vno": "LT", "ltu": "LT", "kyi": "UA", "ukr": "UA",
    "mda": "MD", "msq": "BY", "blr": "BY", "svo": "RU", "rus": "RU",
    "ist": "TR", "tur": "TR", "tlv": "IL", "isr": "IL", "dxb": "AE",
    "uae": "AE", "ruh": "SA", "sau": "SA", "kwt": "KW", "bhr": "BH",
    "jor": "JO", "lbn": "LB", "irq": "IQ", "irn": "IR", "arm": "AM",
    "aze": "AZ", "tbs": "GE", "kaz": "KZ", "uzb": "UZ", "kgz": "KG",
    "mng": "MN", "pak": "PK", "bgd": "BD", "npl": "NP", "lka": "LK",
    "mmr": "MM", "bkk": "TH", "tha": "TH", "han": "VN", "vnm": "VN",
    "lao": "LA", "khm": "KH", "kul": "MY", "mys": "MY", "cgk": "ID",
    "idn": "ID", "mnl": "PH", "phl": "PH", "brn": "BN", "bom": "IN",
    "del": "IN", "ind": "IN", "chn": "CN", "syd": "AU", "aus": "AU",
    "akl": "NZ", "nzl": "NZ", "gru": "BR", "bra": "BR", "arg": "AR",
    "chl": "CL", "lim": "PE", "bog": "CO", "col": "CO", "mex": "MX",
    "gtm": "GT", "cri": "CR", "pty": "PA", "dom": "DO", "sju": "PR",
    "zaf": "ZA", "nga": "NG", "ken": "KE", "cai": "EG", "egy": "EG",
    "mar": "MA", "tun": "TN", "dza": "DZ", "gha": "GH", "eth": "ET",
    "tza": "TZ", "uga": "UG", "mus": "MU", "cmr": "CM", "sen": "SN",
    "civ": "CI", "zwe": "ZW", "ago": "AO", "moz": "MZ", "bwa": "BW",
    "nam": "NA", "cyp": "CY", "svk": "SK", "svn": "SI", "isl": "IS",
}

# 节点名里表示「地区未知」的写法；它们同样是冗余的，命名时要剥掉
UNKNOWN_WORDS = ("un", "unknown", "other", "其他", "未知")

WORD_RE = re.compile(r"[A-Za-z0-9]+")


def _flag(code):
    """ISO 代码 → 国旗 emoji（区域指示符 = 'A' 的码位 + 字母序号）。"""
    return "".join(chr(0x1F1E6 + ord(ch) - 65) for ch in code)


FLAGS = {code: _flag(code) for code, _, _ in REGION_TABLE}
REGION_CN = {code: zh for code, zh, _ in REGION_TABLE}
_CODES = set(REGION_CN)

# 中文名与英文全名走子串匹配；≤4 字符的拉丁短码走单词匹配
# （否则 "est" 会命中 "fastest"、"nam" 会命中 "name"）
_ALIAS_SUB = []
_ALIAS_WORD = {}
for _code, _zh, _alts in REGION_TABLE:
    _ALIAS_SUB.append((_code, _zh))
    for _alt in _alts:
        if _alt.isascii() and len(_alt) <= 4:
            _ALIAS_WORD.setdefault(_alt.lower(), _code)
        else:
            _ALIAS_SUB.append((_code, _alt.lower()))

# 命名时要剥掉的中文写法（地区名/中文别名 + "其他/未知"）。按长度倒序替换，
# 避免短词先把长词切碎（"中国大陆" 先于 "中国"）。
_CJK_ALIASES = sorted(
    {zh for _, zh, _ in REGION_TABLE}
    | {alt for _, _, alts in REGION_TABLE for alt in alts if not alt.isascii()}
    | {"其他", "未知"},
    key=len,
    reverse=True,
)


def parse_proxies(text):
    """从一份 Clash 配置文本中提取 proxies 列表。"""
    try:
        data = yaml.safe_load(text)
        if isinstance(data, dict) and isinstance(data.get("proxies"), list):
            return [p for p in data["proxies"] if isinstance(p, dict) and p.get("server")]
    except Exception as exc:
        log.warning("YAML 解析失败，改用逐行解析: %s", exc)
    return _parse_inline(text)


def _parse_inline(text):
    """兜底解析：只处理 proxies 段中 `- {k: v, ...}` 形式的行。"""
    result = []
    in_block = False
    for line in text.splitlines():
        if re.match(r"^proxies\s*:", line):
            in_block = True
            continue
        if not in_block:
            continue
        if line and not line[0].isspace():
            break
        stripped = line.strip()
        if not stripped.startswith("- "):
            continue
        try:
            item = yaml.safe_load(stripped[2:])
        except Exception:
            continue
        if isinstance(item, dict) and item.get("server"):
            result.append(item)
    return result


def _ws_host(proxy):
    headers = (proxy.get("ws-opts") or {}).get("headers") or {}
    return headers.get("Host") or headers.get("host") or ""


def _credential(proxy, kind):
    """取节点的身份凭据：不同协议字段名不同，同一台机器的多份包装必须能对齐。

    明文代理必须用 `username:password` 组合：先按通用字段取 password 会把
    「同端口不同账号」当成同一节点（同一台 http 代理常挂多个账号）。
    """
    if kind in PLAINTEXT_TYPES:
        return f"{proxy.get('username') or ''}:{proxy.get('password') or ''}"
    for key in ("uuid", "password", "auth", "auth_str", "auth-str"):
        value = proxy.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def conn_key(proxy):
    """节点的**连接身份**（用于去重与跨构建追踪）。

    旧实现拿「除 name 外的全部字段」做 JSON key，上游只差 `udp: true`、
    `client-fingerprint`、`sub_tag` 这类附属字段就会被当成不同节点，于是同一台机器的
    同一端口在订阅里出现 5~6 份（实测 237 个节点只有 148 条线路）：既浪费测速与
    Globalping 额度，又让用户误以为「节点很多」，实际一死就是成片死。

    这里只保留决定「能不能连上、连到哪」的字段。
    """
    kind = str(proxy.get("type") or "").lower()
    server = str(proxy.get("server") or "").strip().lower()
    port = str(proxy.get("port") or "").strip()
    sni = proxy.get("sni") or proxy.get("servername") or _ws_host(proxy) or ""
    net = str(proxy.get("network") or "")
    path = (proxy.get("ws-opts") or {}).get("path") or (
        proxy.get("grpc-opts") or {}
    ).get("grpc-service-name") or ""
    return (kind, server, port, _credential(proxy, kind), str(sni), net, str(path))


def fingerprint(proxy):
    """连接身份的稳定短指纹，用作存活账本的键。"""
    return hashlib.sha1(
        "|".join(conn_key(proxy)).encode("utf-8", "replace")
    ).hexdigest()[:16]


_EMPTY = (None, "", [], {})


def dedupe(proxies):
    """按连接身份去重；参数相同的多份包装合并成一个节点（取字段最全的那份）。

    来源标记 `_srcs` 要**并集**而不是取第一个：跨源重复的节点知道「谁也给过它」，
    后面按源统计存活率时才不会把共享节点全算到第一个源头上。
    """
    order, best, merged_fields = [], {}, 0
    for proxy in proxies:
        key = conn_key(proxy)
        current = best.get(key)
        if current is None:
            best[key] = dict(proxy)
            best[key]["_srcs"] = list(proxy.get("_srcs") or [])
            order.append(key)
            continue
        mine = current.setdefault("_srcs", [])
        for name in proxy.get("_srcs") or []:
            if name not in mine:
                mine.append(name)
        for field, value in proxy.items():
            if field == "_srcs":
                continue
            if current.get(field) in _EMPTY and value not in _EMPTY:
                current[field] = value
                merged_fields += 1
    result = [best[key] for key in order]
    if merged_fields:
        log.info("去重时补全 %d 处缺失字段（同线路多份包装已合并）", merged_fields)
    return result


def _flag_code(text):
    """从国旗 emoji 取 ISO 代码；没有国旗返回 None。"""
    letters = [chr(ord(ch) - 0x1F1E6 + 65) for ch in text if 0x1F1E6 <= ord(ch) <= 0x1F1FF]
    if len(letters) >= 2 and "".join(letters[:2]) in _CODES:
        return "".join(letters[:2])
    return None


def region_code(name):
    """节点名 → ISO 国家码（认不出来返回 None）。"""
    text = str(name or "")
    code = _flag_code(text)
    if code:
        return code
    lowered = text.lower()
    for code, alias in _ALIAS_SUB:
        if alias and alias in lowered:
            return code
    # 短码只看前 4 个词、且要求**大写写法**（"RO - zhuhai.uk" 里的 uk、"Speed in
    # Tokyo" 里的 in 都是小写普通词，不该被当成地区）；首个词放宽到小写
    # （有些源整份配置就是小写）。
    for index, token in enumerate(WORD_RE.findall(text)[:4]):
        upper = token.upper()
        if upper in _CODES and (token.isupper() or index == 0):
            return upper
        if token.lower() in ALPHA3:
            return ALPHA3[token.lower()]
    return None


def classify(name):
    """根据节点名推测地区（返回中文名，认不出为「其他」）。"""
    return REGION_CN.get(region_code(name) or "", "其他")


def region_label(name):
    """节点名 → (国旗 emoji, 中文地区名)，用于给节点起名。"""
    code = region_code(name)
    if not code:
        return "❓", "其他"
    return FLAGS[code], REGION_CN[code]


# 交给 mihomo 内核前先清洗：内核不认识的协议会让内核整体启动失败，必须剔除；
# 其余内核支持的协议一律保留，交给后面的测速环节去判断（不预先按类型误杀）。
#
# 明文代理（http / socks5）**保留**。2026-10-21 用 mihomo 内核对线上 237 个节点做
# 「每节点一个入站端口 → 真实 CONNECT 出网」实测：
#     http  107/119 = 89.9%   socks5 3/3 = 100%
#     vless  23/108 = 21.3%   hysteria2 0/4   trojan 0/2   ss 0/1
# 即 http 恰恰是最能用的那一类。此前「抽样 25 个仅 1 个可用（4%）」的结论是错的：
# 那是用 requests 直接拿 http://host:port 当代理测出来的，没有复用内核的
# 传输/认证/伪装逻辑，对该类节点有系统性低估。真正该做的是统一交给内核验证。
PLAINTEXT_TYPES = {"http", "socks5"}
CORE_TYPES = {
    "ss", "vmess", "vless", "trojan", "hysteria2", "hysteria", "anytls",
    "http", "socks5",
}
CORE_REQUIRED = {
    "ss": ("cipher", "password"),
    "vmess": ("uuid",),
    "vless": ("uuid",),
    "trojan": ("password",),
    "anytls": ("password",),
}
# 凭据字段有别名，任一存在即可
CORE_ANY = {
    "hysteria2": ("password", "auth"),
    "hysteria": ("auth_str", "auth-str"),
}


def _valid_core(proxy, kind):
    if not isinstance(proxy.get("server"), str) or not proxy["server"].strip():
        return False
    try:
        if not 1 <= int(proxy["port"]) <= 65535:
            return False
    except (KeyError, TypeError, ValueError):
        return False
    if not all(proxy.get(f) not in (None, "") for f in CORE_REQUIRED.get(kind, ())):
        return False
    aliases = CORE_ANY.get(kind)
    if aliases and not any(proxy.get(f) not in (None, "") for f in aliases):
        return False
    return True


def filter_supported(proxies):
    """剔除内核不支持的协议，以及字段缺失/端口异常的节点。"""
    kept, dropped = [], {"不支持协议": 0, "字段异常": 0}
    for proxy in proxies:
        kind = str(proxy.get("type") or "").lower()
        if kind not in CORE_TYPES:
            dropped["不支持协议"] += 1
        elif not _valid_core(proxy, kind):
            dropped["字段异常"] += 1
        else:
            kept.append(proxy)
    detail = "，".join(f"{k} {v}" for k, v in dropped.items() if v)
    if detail:
        log.info("清洗掉 %d 个节点（%s）", len(proxies) - len(kept), detail)
    return kept


def _strip_controls(value):
    """递归清掉字符串里的控制字符（Unicode 类别 C*），其余原样保留。"""
    if isinstance(value, str):
        return "".join(ch for ch in value if unicodedata.category(ch)[0] != "C")
    if isinstance(value, dict):
        return {k: _strip_controls(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_strip_controls(v) for v in value]
    return value


def sanitize(proxies):
    """清掉节点字段里的控制字符，并规范化个别会让 mihomo 拒绝整份配置的字段。

    上游源里偶有字段被错误解码（例如 emoji 被按 latin-1 读成乱码字节），落进 YAML 时会变成
    \\x9F 这类 C1 控制字符转义。mihomo / FlClash 的内核解析时会直接报
    "yaml: control characters are not allowed" 并拒绝**整个**配置，表现为客户端节点列表空白。

    另外，vmess 节点若带空字符串 cipher（security），mihomo 会报
    "unsupported security type: \"\"" 并拒绝**整份**测速配置（几千个节点里一个坏节点
    就会让真实测速整体回退）。这里统一归一成内核支持的取值，无法识别的用 auto。
    """
    # mihomo vmess 支持的加密方式；其余取值（含空串）一律退回 auto（由内核自行协商）。
    VMESS_SECURITY = {"auto", "none", "zero", "aes-128-gcm", "chacha20-poly1305"}
    fixed = 0
    vmess_fixed = 0
    for proxy in proxies:
        if str(proxy.get("type")).lower() == "vmess":
            raw = proxy.get("cipher")
            security = str(raw).strip().lower() if raw not in (None, "") else ""
            if security not in VMESS_SECURITY:
                proxy["cipher"] = "auto"
                vmess_fixed += 1
        for key, value in list(proxy.items()):
            cleaned = _strip_controls(value)
            if cleaned != value:
                fixed += 1
                proxy[key] = cleaned
    if fixed:
        log.info("清洗掉 %d 处含控制字符的字段", fixed)
    if vmess_fixed:
        log.info("规范化 %d 个 vmess 节点的加密方式为 auto", vmess_fixed)
    return proxies


def _unique_names(proxies):
    """保证输出配置里的节点名唯一。

    注意：必须按「最终生成的名字」判重，而不是按基础名计数。否则当某个源站节点名
    本身就带 " #2" 时，会和另一个同基础名节点被改出的 "xxx #2" 撞车，导致配置里出现
    重名——mihomo 遇到重名会直接拒绝启动，客户端一个节点都加载不出来。
    """
    used = set()
    for proxy in proxies:
        base = str(proxy.get("name") or "node").strip() or "node"
        name = base
        n = 1
        while name in used:
            n += 1
            name = f"{base} #{n}"
        used.add(name)
        proxy["name"] = name
    return proxies


def rank_by_protocol(proxies, scores, high_ratio=0.75, cap=None, source_scores=None):
    """按协议「实测真实出网可用率」分区排序，让最能用的节点先占探测额度。

    依据 2026-10-21 在本机（国内家宽）用 mihomo 内核逐节点真实 CONNECT 的实测：
        socks5 100%    http 84~90%    vless 10~21%    ss/vmess/trojan/anytls/hy2 ≈ 0%
    协议差异极大，而 Globalping 的探测名额（`cap`，默认 240）是稀缺资源：
    让 0% 可用的协议去占名额，等于把额度浪费在必死节点上。

    高可用协议（≥80 分）最多占 `high_ratio × cap` 个名额，余量留给其他协议，
    避免订阅被单一协议垄断——同源节点往往成批失效，需要留点冗余。

    注意 `cap` 必须传「探测名额」而不是候选总数：候选可能有几千个，
    拿它算配额会让配额永远大于实际名额，等于配比彻底失效（踩过一次）。

    源倾斜（`source_scores`，0~100 表示该源实测存活率的相对值）：2026-10-22 按源实测
    daily_free_vpn 38%、Au1rxx-07 17.5%、ermaozi 15%、Au1rxx-06 10%、TopFreeProxy 0/40。
    协议只能解释一半方差 —— 「http 来自 daily_free_vpn」和「http 来自 TopFreeProxy」
    的命运完全不同。所以分区仍按**协议原始分**（不乘源因子：否则某个主力源临时 404
    就会把其他源的高可用节点一起打到低分区，白白饿死备份），分区内则按
    `协议分 × 源因子` 排优先级，额度不够时先砍掉低信誉源的节点。
    """
    if not scores:
        return proxies
    weight = {str(k).lower(): float(v) for k, v in scores.items()}
    src_weight = {str(k).lower(): float(v) / 100.0 for k, v in (source_scores or {}).items()}

    def score(p):
        return weight.get(str(p.get("type") or "").lower(), 0.0)

    def src_factor(p):
        if not src_weight:
            return 1.0
        names = [str(n).lower() for n in (p.get("_srcs") or [])]
        # 多个源都给过这个节点时按最好的那个算（它至少有一个源在供血）
        return max([src_weight.get(n, 0.5) for n in names] or [0.5])

    def priority(p):
        return -score(p) * src_factor(p)

    high = [p for p in proxies if score(p) >= 80]
    low = [p for p in proxies if score(p) < 80]
    # 分区内按「协议分 × 源因子」排（sort 稳定，同分保持上游的存活次数/网速序）
    high.sort(key=priority)
    low.sort(key=priority)
    quota = max(1, int((cap or len(proxies)) * float(high_ratio)))
    if len(high) <= quota:
        return high + low
    # 超出配额的高分节点排到最后，额度不够时优先被截掉
    return high[:quota] + low + high[quota:]



def quality_score(proxy, cfg=None):
    """延迟 + 网速的综合分（0~1），用于发布顺序。

    为什么不再「网速优先、延迟兜底」（原 `_sort_key`）：那样一个 800ms/100KB/s 的
    节点会排在 20ms/50KB/s 前面，而点开链接的第一感受是延迟（DNS + 握手 + 首字节），
    下载速度只在拉流/大文件时才感知得到。两维归一化后加权，才是「综合」。

    归一化方式：
      延迟 qd = min(1, delay_ref / delay)        倒数衰减，≤ ref 满分
      网速 qs = min(1, log1p(kbps)/log1p(ref))   对数压缩，几十到几千 KB/s 跨两个数量级，
                                                 线性会把小网速全压到 0 分附近
    缺测的维度给中性值而不是 0 分：能 CONNECT 但下载超时的节点（本机实测 223 个通过
    节点里只有 93 个能测出网速）不该被一票否决，也不该白拿满分。

    实测字段有两套来源，按优先级读：本机探测的 `_delay_ms` / `_speed_kbps`，
    以及白名单条目回落写入的 `_wl_delay` / `_wl_kbps`。
    """
    cfg = cfg or {}
    delay = float(proxy.get("_delay_ms") or proxy.get("_wl_delay") or 0)
    kbps = float(proxy.get("_speed_kbps") or proxy.get("_wl_kbps") or 0)
    d_ref = max(1.0, float(cfg.get("delay_ref_ms", 800)))
    s_ref = max(1.0, float(cfg.get("speed_ref_kbps", 150)))
    dw = max(0.0, float(cfg.get("delay_weight", 0.5)))
    sw = max(0.0, float(cfg.get("speed_weight", 0.5)))
    total = dw + sw
    if total <= 0:
        return 0.0

    qd = 0.45 if delay <= 0 else min(1.0, d_ref / delay)
    qs = 0.35 if kbps <= 0 else min(1.0, math.log1p(kbps) / math.log1p(s_ref))
    return (dw * qd + sw * qs) / total


def sort_by_quality(proxies, cfg=None):
    """按综合分从高到低排发布顺序（只改顺序，不淘汰任何节点）。

    同分时依次比较：存活轮次高者优先（免费节点「昨天好用今天死」很常见，
    分数相同时信稳定的那个）→ 延迟低者 → 原始顺序（保持上游的确定性）。
    """
    cfg = cfg or {}
    if not cfg.get("enabled", True):
        return proxies
    # 没有任何实测数据时（--no-check / 跳过检测）保持上游原顺序，别用一堆相同的
    # 中性分把它们按 streak 重排成看似有依据的顺序。
    measured = ("_delay_ms", "_wl_delay", "_speed_kbps", "_wl_kbps")
    if not any(any(key in p for key in measured) for p in proxies):
        return proxies
    rows = []
    for index, proxy in enumerate(proxies):
        delay = int(proxy.get("_delay_ms") or proxy.get("_wl_delay") or 0)
        rows.append(
            (
                -quality_score(proxy, cfg),
                -int(proxy.get("_streak") or 0),
                delay if delay > 0 else 10**9,
                index,
                proxy,
            )
        )
    rows.sort(key=lambda row: row[:4])
    return [row[4] for row in rows]


def rank_summary(proxies, cfg=None):
    """发布集的质量分布，写进 status.json 便于核对排序是否符合预期。"""
    cfg = cfg or {}
    scores = sorted(quality_score(p, cfg) for p in proxies)
    if not scores:
        return {}
    mid = scores[len(scores) // 2]
    return {
        "weights": {
            "delay": float(cfg.get("delay_weight", 0.5)),
            "speed": float(cfg.get("speed_weight", 0.5)),
        },
        "refs": {
            "delay_ms": float(cfg.get("delay_ref_ms", 800)),
            "speed_kbps": float(cfg.get("speed_ref_kbps", 150)),
        },
        "median_score": round(mid, 3),
        "best_score": round(scores[-1], 3),
        "worst_score": round(scores[0], 3),
    }


# ---- 节点命名：地区 | 网速 | 延迟 | 原名里的代表性标志 ----
#
# 为什么统一改名：上游名字是各家自己拼的，同一个订阅里能同时出现
#   🇸🇬SG_131|578KB/s / RO - zhuhai.uk 05 [be5ac291e62c920d] / vless-1879607313 /
#   UN【机场推荐：https://a9a.xyz】224 / 🏳[www.v2nodes.com] vless- #4
# 用户看不出「哪个快、哪个在哪、哪个还是刚才那个」。改成统一格式后，名字本身
# 就是一句话：地区 + 实测网速 + 实测延迟 + 能区分线路的原始标志。
_SPEED_TEXT_RE = re.compile(r"\d+(?:\.\d+)?\s*[KMGTkmgt]\s*[Bb]\s*/\s*[Ss]")
_DELAY_TEXT_RE = re.compile(r"\d+(?:\.\d+)?\s*ms(?![a-z])", re.I)
_URL_TEXT_RE = re.compile(r"(?:https?://|www\.)[^\s\]\)】]+", re.I)
# 括号内容基本都是推广位（【机场推荐：…】）或上游塞进来的连接指纹（[9f5d0bdf…]）
_BRACKET_TEXT_RE = re.compile(r"【[^】]*】|\[[^\]]*\]|\([^)]*\)|（[^）]*）")
_SEP_TEXT_RE = re.compile(r"[|/\\_+~,;:、·•]+")
_SPACE_TEXT_RE = re.compile(r"\s+")
_LEAD_SEP = re.compile(r"^[\s\-_—–|/+~,;:、·•#]+")
_TAIL_SEP = re.compile(r"[\s\-_—–|/+~,;:、·•]+$")
_UNKNOWN_TOKEN_RE = re.compile(r"\b(?:un|unknown|other)\b", re.I)


def _plain(text):
    """去掉 emoji 与纯符号字符（国旗、❓、🏳…），保留文字与数字。

    这些符号对命名没有信息量：地区已经由 region_label 显式写出来了，
    留着只是让名字更长。
    """
    return "".join(
        ch for ch in str(text or "")
        if unicodedata.category(ch) not in ("So", "Sk", "Cf", "Cc")
    )


def node_marker(proxy, limit=20):
    """从原节点名里抽出「代表性标志」：剥掉地区、网速、延迟、推广位后剩下的部分。

    实测的几种写法与结果：
        🇸🇬SG_131|578KB/s                → 131
        🇫🇷FR_185|1.0MB/s|S004-260618    → 185 S004-260618
        RO - zhuhai.uk 05 [hash]         → zhuhai.uk 05
        NL_speednode_0021                → speednode 0021
        UN【机场推荐：https://…】224      → 224
        🏳[www.v2nodes.com] vless- #4     → vless #4
    只剥**开头**的地区前缀：如果全串替换，`zhuhai.uk` 里的 uk、`US-LAX` 里的 LAX
    会被当成地区码吃掉，名字反而丢了用来区分线路的部分。
    剥完为空时退回连接指纹前 6 位，保证名字仍能落回一台具体的机器。
    """
    text = _plain(proxy.get("name"))
    text = _URL_TEXT_RE.sub(" ", text)
    text = _BRACKET_TEXT_RE.sub(" ", text)
    text = _SPEED_TEXT_RE.sub(" ", text)
    text = _DELAY_TEXT_RE.sub(" ", text)
    # 地区词（中文名与中文别名）在名字里任何位置都是冗余的，全串替换
    for alias in _CJK_ALIASES:
        text = text.replace(alias, " ")
    text = _SEP_TEXT_RE.sub(" ", text)
    text = _UNKNOWN_TOKEN_RE.sub(" ", text)
    text = re.sub(r"-(?=\s)", " ", text)      # "vless- #4" → "vless #4"，保留 "S004-260618"

    # 只剥一段开头的地区前缀（"US - xxx" / "SG_xxx" / "RUS 2"）：
    # "US-LAX-01" 里的 LAX 是机场码，属于有用的区分标志，不能连剥两段。
    match = WORD_RE.search(text)
    if match and not text[: match.start()].strip(" -_|/·"):
        token = match.group(0)
        if token.upper() in _CODES or token.lower() in ALPHA3:
            text = text[match.end():]

    text = _TAIL_SEP.sub("", _LEAD_SEP.sub("", _SPACE_TEXT_RE.sub(" ", text)))
    if not text:
        return fingerprint(proxy)[:6]
    if len(text) > limit:
        text = _TAIL_SEP.sub("", text[:limit])
    return text or fingerprint(proxy)[:6]


def _speed_text(kbps):
    """网速 → 名字里的可读写法（KB/s 或 MB/s）。"""
    if kbps <= 0:
        return "--"
    if kbps < 1024:
        return "%dKB/s" % int(kbps)
    return "%.1fMB/s" % (kbps / 1024.0)


def _delay_text(ms):
    return "%dms" % ms if ms > 0 else "--"


def display_name(proxy, cfg=None):
    """单个节点的展示名：地区 | 网速 | 延迟 | 原名代表性标志。"""
    cfg = cfg or {}
    flag, region = region_label(proxy.get("name"))
    delay = int(proxy.get("_delay_ms") or proxy.get("_wl_delay") or 0)
    kbps = float(proxy.get("_speed_kbps") or proxy.get("_wl_kbps") or 0)
    parts = [
        flag + region,
        _speed_text(kbps),
        _delay_text(delay),
        node_marker(proxy, limit=int(cfg.get("marker_max", 20) or 20)),
    ]
    # 分隔符不带空格：Clash 客户端普遍在 30~40 个字符处截断，省下的 6 个字符
    # 正好留给「代表性标志」——那才是区分线路的部分（实测名字中位 30 字符）。
    return "|".join(parts)


def rename(proxies, cfg=None):
    """把发布集的节点名统一成「地区 | 网速 | 延迟 | 代表性标志」并保证唯一。

    必须在测速之后调用：网速/延迟是名字的一部分，而这两个值只有测完才有。
    重名（同地区、同质量、同标志）时按 ` #n` 递增——mihomo 遇到重名会拒绝
    加载整份配置，客户端表现为节点列表空白。
    """
    cfg = cfg or {}
    if not cfg.get("enabled", True):
        return proxies
    used = set()
    for proxy in proxies:
        base = display_name(proxy, cfg)
        name, index = base, 1
        while name in used:
            index += 1
            name = "%s #%d" % (base, index)
        used.add(name)
        proxy["name"] = name
    return proxies


def diversify(proxies, per_line=2, trusted_streak=3, trusted_cap=4):
    """同一 `server:port` 最多保留 per_line 个凭据，提升订阅的线路多样性。

    公开源里同一台机器的「不同账号」常常成批出现（同一 IP:端口挂几十个 uuid），
    实测发布订阅里 237 个节点只对应 148 条线路，最夸张的一条线路占 6 份。
    这些多份凭据往往同时失效——留多份只是重复占位、挤占探测额度，
    且让用户误以为节点很多。存活达标的节点放宽到 trusted_cap，避免误伤真正稳的。
    """
    if not per_line or per_line <= 0:
        return proxies
    used = {}
    kept, dropped = [], 0
    for proxy in proxies:
        key = (str(proxy.get("server")), proxy.get("port"))
        count = used.get(key, 0)
        cap = trusted_cap if int(proxy.get("_streak") or 0) >= trusted_streak else per_line
        if count >= cap:
            dropped += 1
            continue
        used[key] = count + 1
        kept.append(proxy)
    if dropped:
        log.info("线路去冗：同一 server:port 最多保留 %d 个凭据（存活达标放宽到 %d），剔除 %d 个",
                 per_line, trusted_cap, dropped)
    return kept


def build_clash(proxies, clash_cfg, meta):
    """生成完整、可直接导入的 Clash 配置。"""
    # 防御：_speed_kbps / _streak 这类内部字段一旦写进配置，mihomo 会拒绝整份文件
    # （客户端表现为节点列表空白），这里再兜一层，不依赖调用方先剥干净。
    clean = [
        {k: v for k, v in proxy.items() if not k.startswith("_")} for proxy in proxies
    ]
    names = [p["name"] for p in clean]
    test_url = clash_cfg.get("test_url", "http://www.gstatic.com/generate_204")
    interval = int(clash_cfg.get("test_interval", 300))
    # url-test 默认 5 秒判超时，晚高峰国际链路 TTFB 常到 5~8 秒，会把其实能用的节点误判为 timeout。
    test_timeout = int(clash_cfg.get("test_timeout", 8000))

    by_region = defaultdict(list)
    for proxy in clean:
        by_region[classify(proxy["name"])].append(proxy["name"])

    # 地区认得的国家从 16 个扩到 100+ 之后，北美/欧洲的「一国一两个节点」会把
    # 分组列表刷成几十个几乎空的小组，反而更难选。少于 min_region_nodes 的地区
    # 并入「其他」，只让真正成规模的地区单独成组（按节点数从多到少排）。
    min_nodes = int(clash_cfg.get("min_region_nodes", 8) or 0)
    if min_nodes:
        merged = []
        for region, nodes in list(by_region.items()):
            if region != "其他" and len(nodes) < min_nodes:
                merged.extend(nodes)
                del by_region[region]
        if merged:
            by_region["其他"] = by_region.get("其他", []) + merged

    region_groups = [
        {
            "name": f"🌐 {region}",
            "type": "url-test",
            "url": test_url,
            "interval": interval,
            "tolerance": 50,
            "timeout": test_timeout,
            "proxies": nodes,
        }
        for region, nodes in sorted(
            by_region.items(), key=lambda kv: (kv[0] == "其他", -len(kv[1]), kv[0])
        )
    ]

    groups = [
        {
            "name": "🚀 节点选择",
            "type": "select",
            "proxies": ["♻️ 自动选择", "DIRECT"] + [g["name"] for g in region_groups] + names,
        },
        {
            "name": "♻️ 自动选择",
            "type": "url-test",
            "url": test_url,
            "interval": interval,
            "tolerance": 50,
            "timeout": test_timeout,
            "proxies": names,
        },
    ]
    groups.extend(region_groups)

    config = {
        "port": int(clash_cfg.get("http_port", 7890)),
        "socks-port": int(clash_cfg.get("socks_port", 7891)),
        "allow-lan": False,
        "mode": "rule",
        "log-level": "info",
        "external-controller": clash_cfg.get("external_controller", "127.0.0.1:9090"),
        "dns": _dns_config(),
        "proxies": clean,
        "proxy-groups": groups,
        "rules": _rules(),
    }
    # 主源停用（site.enabled=false）时不能再写「datiya 覆盖 0 天」——那会让读者以为
    # 站点还在供血，只是恰好抓不到。
    scope = (
        f"，datiya 覆盖 {meta['days']} 天"
        if meta.get("site_enabled", True)
        else "，仅附加源"
    )
    header = (
        "# 由 datiya-sub 自动生成，请勿手动修改\n"
        f"# 更新于 {meta['updated_at']} | 可用节点 {meta['alive']} 个"
        f"（候选 {meta['total']} 个{scope}）\n"
    )
    body = yaml.safe_dump(
        config,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
        width=4096,
    )
    return header + body


def _dns_config():
    return {
        "enable": True,
        "ipv6": False,
        "enhanced-mode": "fake-ip",
        "fake-ip-range": "198.18.0.1/16",
        "fake-ip-filter": ["*.lan", "*.local", "localhost.ptlogin2.qq.com"],
        "nameserver": ["223.5.5.5", "119.29.29.29"],
        "fallback": ["https://1.1.1.1/dns-query", "https://dns.google/dns-query"],
    }


def _rules():
    return [
        "IP-CIDR,127.0.0.0/8,DIRECT,no-resolve",
        "IP-CIDR,10.0.0.0/8,DIRECT,no-resolve",
        "IP-CIDR,172.16.0.0/12,DIRECT,no-resolve",
        "IP-CIDR,192.168.0.0/16,DIRECT,no-resolve",
        "IP-CIDR,100.64.0.0/10,DIRECT,no-resolve",
        "DOMAIN-SUFFIX,cn,DIRECT",
        "GEOIP,CN,DIRECT",
        "MATCH,🚀 节点选择",
    ]


SUPPORTED_LINK_TYPES = ("ss", "vmess", "trojan", "vless", "hysteria2", "anytls")


def build_links(proxies):
    """生成 v2rayN / v2rayNG / NekoBox 等客户端可用的分享链接。"""
    links = []
    for proxy in proxies:
        kind = str(proxy.get("type") or "").lower()
        if kind not in SUPPORTED_LINK_TYPES:
            continue
        try:
            links.append(_make_link(proxy))
        except Exception as exc:
            log.debug("节点 %s 转分享链接失败: %s", proxy.get("name"), exc)
    return links


def b64(data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _transport(proxy):
    """把 mihomo 的传输层参数翻译成分享链接的 query 片段。

    reality / gRPC 这类节点的参数字段名与分享链接完全不同，漏一个客户端就废：
    实测旧实现给 17 个 reality 节点生成的 vless 链接里 pbk/sid/fp 全部缺失，
    导入 v2rayN 必然连不上（reality 没有 publicKey 无法握手）。
    """
    query = []
    net = str(proxy.get("network") or "tcp")
    ws_opts = proxy.get("ws-opts") or {}
    grpc_opts = proxy.get("grpc-opts") or {}
    if net == "ws":
        host = _ws_host(proxy)
        if host:
            query.append(("host", host))
        path = ws_opts.get("path")
        if path:
            query.append(("path", path))
    elif net == "grpc":
        service = grpc_opts.get("grpc-service-name")
        if service:
            query.append(("serviceName", service))
    elif net in ("h2", "http"):
        hosts = (proxy.get("h2-opts") or {}).get("host") or []
        if hosts:
            query.append(("host", hosts[0] if isinstance(hosts, list) else hosts))
        path = (proxy.get("h2-opts") or {}).get("path")
        if path:
            query.append(("path", path))
    return query


def _make_link(proxy):
    kind = str(proxy.get("type") or "").lower()
    name = str(proxy.get("name"))
    server = str(proxy.get("server"))
    port = str(proxy.get("port"))
    tag = "#" + _quote(name)

    if kind == "ss":
        userinfo = b64(f"{proxy.get('cipher', 'aes-256-gcm')}:{proxy.get('password', '')}")
        query = []
        plugin = proxy.get("plugin")
        if plugin:
            opts = proxy.get("plugin-opts") or {}
            value = f"{plugin};{_plugin_opts(opts)}" if opts else str(plugin)
            query.append(("plugin", value))
        tail = f"?{_query(query)}" if query else ""
        return f"ss://{userinfo}@{server}:{port}{tail}{tag}"

    if kind == "vmess":
        net = str(proxy.get("network", "tcp"))
        service = (proxy.get("grpc-opts") or {}).get("grpc-service-name", "")
        payload = {
            "v": "2",
            "ps": name,
            "add": server,
            "port": port,
            "id": str(proxy.get("uuid", "")),
            "aid": str(proxy.get("alterId", 0)),
            "scy": str(proxy.get("cipher") or "auto"),
            "net": net,
            "type": "none",
            "host": _ws_host(proxy) or str(proxy.get("servername") or ""),
            "path": str((proxy.get("ws-opts") or {}).get("path") or service or ""),
            "tls": "tls" if proxy.get("tls") else "",
            "sni": str(proxy.get("servername") or proxy.get("sni") or ""),
            "fp": str(proxy.get("client-fingerprint") or ""),
        }
        return "vmess://" + base64.b64encode(
            json.dumps(payload, ensure_ascii=False).encode("utf-8")
        ).decode()

    if kind == "trojan":
        query = [("security", "tls"), ("type", str(proxy.get("network", "tcp")))]
        sni = proxy.get("sni") or proxy.get("servername")
        if sni:
            query.append(("sni", str(sni)))
        if proxy.get("skip-cert-verify"):
            query.append(("allowInsecure", "1"))
        query += _transport(proxy)
        return f"trojan://{_quote(str(proxy.get('password', '')))}@{server}:{port}?{_query(query)}{tag}"

    if kind == "vless":
        reality = proxy.get("reality-opts") or {}
        if reality:
            security = "reality"
        elif proxy.get("tls"):
            security = "tls"
        else:
            security = "none"
        query = [
            ("encryption", "none"),
            ("security", security),
            ("type", str(proxy.get("network", "tcp"))),
        ]
        sni = proxy.get("servername") or proxy.get("sni")
        if sni:
            query.append(("sni", str(sni)))
        if proxy.get("skip-cert-verify"):
            query.append(("allowInsecure", "1"))
        if proxy.get("flow"):
            query.append(("flow", str(proxy["flow"])))
        if reality:
            # reality 三件套：publicKey 必需，shortId/SpiderX 有则带上；
            # fingerprint 也必需（reality 握手靠它伪装浏览器）。
            if reality.get("public-key"):
                query.append(("pbk", str(reality["public-key"])))
            if reality.get("short-id"):
                query.append(("sid", str(reality["short-id"])))
            if reality.get("spider-x"):
                query.append(("spx", str(reality["spider-x"])))
            query.append(("fp", str(proxy.get("client-fingerprint") or "chrome")))
        elif proxy.get("client-fingerprint"):
            query.append(("fp", str(proxy["client-fingerprint"])))
        query += _transport(proxy)
        return f"vless://{proxy.get('uuid', '')}@{server}:{port}?{_query(query)}{tag}"

    if kind == "hysteria2":
        query = []
        sni = proxy.get("sni") or proxy.get("servername")
        if sni:
            query.append(("sni", str(sni)))
        if proxy.get("skip-cert-verify"):
            query.append(("insecure", "1"))
        obfs = proxy.get("obfs")
        if obfs:
            query.append(("obfs", str(obfs)))
            if proxy.get("obfs-password"):
                query.append(("obfs-password", str(proxy["obfs-password"])))
        password = proxy.get("password") or proxy.get("auth") or ""
        tail = f"?{_query(query)}" if query else ""
        return f"hysteria2://{_quote(str(password))}@{server}:{port}{tail}{tag}"

    if kind == "anytls":
        query = []
        sni = proxy.get("sni") or proxy.get("servername")
        if sni:
            query.append(("sni", str(sni)))
        if proxy.get("skip-cert-verify"):
            query.append(("insecure", "1"))
        if proxy.get("client-fingerprint"):
            query.append(("fp", str(proxy["client-fingerprint"])))
        tail = f"?{_query(query)}" if query else ""
        return f"anytls://{_quote(str(proxy.get('password', '')))}@{server}:{port}{tail}{tag}"

    raise ValueError(f"不支持的协议: {kind}")


def _plugin_opts(opts):
    """ss 插件参数拼成 SIP002 的 `plugin` 值（k=v 用分号连接）。"""
    pairs = []
    for key in ("mode", "host", "path", "tls"):
        value = opts.get(key)
        if value in (None, "", False):
            continue
        if value is True:
            pairs.append(str(key))
        else:
            pairs.append(f"{key}={value}")
    return ";".join(pairs)


def _query(pairs):
    return "&".join(f"{k}={_quote(str(v))}" for k, v in pairs)


def _quote(value):
    return quote(value, safe="")