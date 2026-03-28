import sys
import os
import re
import requests
import json
import pandas as pd
import copy
import urllib.parse
from urllib.parse import urlparse, urljoin, urlencode
from bs4 import BeautifulSoup
import webbrowser

from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QPushButton, QTableWidget, QTableWidgetItem,
                             QFileDialog, QLabel, QProgressBar, QMenu,
                             QTextEdit, QSplitter, QComboBox, QInputDialog, QDialog,
                             QLineEdit, QScrollArea, QCheckBox, QSpinBox, QMessageBox,
                             QColorDialog, QListWidget, QAbstractItemView)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QRunnable, QThreadPool, QObject
from PyQt6.QtGui import QColor

# --- 1. КОНФИГ ---
CONFIG_FILE = "config.json"

GLOBAL_EMPTY_MARKERS = [
    'ничего не найдено', 'not found', 'извините', 'no results', '0 результатов',
    '0 results', 'результатов нет', 'исключены из поиска', 'слишком часто используются',
    'либо слишком длинные, либо слишком короткие', 'слишком длинные', 'слишком короткие',
    'too common', 'ignored', 'были проигнорированы', 'слишком употребимыми', 'найдено записей: 0'
]


def wrap(words): return [{"word": w, "active": True} for w in words]


# Новая структура: всё привязано к категориям
DEFAULT_CONFIG = {
    "Default": {
        "THREADS": 20,
        "USE_PROXY": False,
        "PROXY_HOST": "",
        "PROXY_PORT": "",
        "PROXY_USER": "",
        "PROXY_PASS": "",
        "CONTEXT_WARNING_ACTIVE": False,
        "CONTEXT_WARNING_COLOR": "#FF8C00",
        "PRIORITY": ["SEO/PBN", "Scraper/Aggregator", "Adult", "Pharma", "Gambling", "Marketing Trash",
                     "Chinese Content", "Arabic Content", "Clean"],
        "EMPTY_MARKERS": GLOBAL_EMPTY_MARKERS.copy(),
        "CATEGORIES": {
            "Gambling": {
                "color": "#CC7A00",
                "stop_words": wrap(
                    ['казино', 'игровые автоматы', 'casino', 'vulkan', 'фонбет', '1xbet', 'freebet', '1vin',
                     'winline']),
                "context_words": {
                    "777": {"bad": wrap(['слот', 'jackpot', 'вулкан', 'играть', 'выигрыш']),
                            "good": wrap(['boeing', 'боинг'])},
                    "ставки": {"bad": wrap(['спорт', 'прогноз', 'букмекер']),
                               "good": wrap(['налог', 'ипотека', 'ремонт'])},
                    "слот": {"bad": wrap(['играть', 'джекпот']), "good": wrap(['память', 'плата'])},
                    "рулетка": {"bad": wrap(['зеро', 'ставка']), "good": wrap(['измерительная', 'строительная'])}
                }
            },
            "Adult": {
                "color": "#8B0000",
                "stop_words": wrap(
                    ['порно', 'секс', 'вебкам', 'porn', 'sex', 'xxx', 'bdsm', 'проститутки', 'webcam', 'stripchat']),
                "context_words": {
                    "знакомства": {"bad": wrap(['интим', 'секс']), "good": wrap(['объявления', 'городские'])}
                }
            },
            "Pharma": {
                "color": "#E65100",
                "stop_words": wrap(
                    ['наркотики', 'виагра', 'бад', 'drugs', 'pills', 'viagra', 'cialis', 'сиалис', 'бады']),
                "context_words": {}
            },
            "Scraper/Aggregator": {
                "color": "#6100AB",
                "stop_words": wrap(
                    ['website worth', 'domain value', 'estimated worth', 'site cost', 'websiteworth',
                     'most visited web pages', 'world\'s most']),
                "context_words": {}
            },
            "Marketing Trash": {
                "color": "#FBC02D",
                "stop_words": wrap(['make money online', 'passive income', 'list building', 'earn money']),
                "context_words": {}
            },
            "SEO/PBN": {
                "color": "#6100AB",
                "stop_words": wrap(
                    ['guest post', 'write for us', 'submit article', 'sponsored post', 'link directory',
                     'seo services']),
                "context_words": {
                    "seo": {"bad": wrap(['submit', 'directory', 'rank', 'post']), "good": wrap(['optimization'])}
                }
            },
            "Chinese Content": {"color": "#FF69B4", "stop_words": [], "context_words": {}},
            "Arabic Content": {"color": "#FF69B4", "stop_words": [], "context_words": {}},
            "Clean": {"color": "#004400", "stop_words": [], "context_words": {}}
        }
    }
}

# --- СТИЛЬ ТЕМНОЙ ТЕМЫ ---
DARK_STYLE = """
QWidget {
    background-color: #2b2b2b;
    color: #d4d4d4;
}
QTableWidget {
    background-color: #1e1e1e;
    color: #d4d4d4;
    gridline-color: #444444;
}
QHeaderView::section {
    background-color: #3c3c3c;
    color: white;
    border: 1px solid #2b2b2b;
}
QPushButton {
    background-color: #3c3c3c;
    color: white;
    border: 1px solid #555;
}
QPushButton:hover {
    background-color: #4c4c4c;
}
QPushButton:disabled {
    background-color: #2b2b2b;
    color: #777;
}
QLineEdit, QComboBox, QSpinBox, QTextEdit, QListWidget {
    background-color: #1e1e1e;
    color: #d4d4d4;
    border: 1px solid #555;
}
QProgressBar {
    border: 1px solid #555;
    background-color: #1e1e1e;
    text-align: center;
    color: white;
}
QProgressBar::chunk {
    background-color: #0D47A1;
}
"""


def load_settings():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Миграция старых конфигов
                for p_name, p_data in data.items():
                    if "CATEGORIES" not in p_data:
                        p_data["CATEGORIES"] = copy.deepcopy(DEFAULT_CONFIG["Default"]["CATEGORIES"])
                    if "CONTEXT_WARNING_ACTIVE" not in p_data:
                        p_data["CONTEXT_WARNING_ACTIVE"] = False
                    if "CONTEXT_WARNING_COLOR" not in p_data:
                        p_data["CONTEXT_WARNING_COLOR"] = "#FF8C00"
                    if "PRIORITY" not in p_data:
                        p_data["PRIORITY"] = ["SEO/PBN", "Adult", "Pharma", "Gambling", "Scraper/Aggregator",
                                              "Marketing Trash", "Chinese Content", "Arabic Content", "Clean"]
                        for c in p_data["CATEGORIES"]:
                            if c not in p_data["PRIORITY"]:
                                p_data["PRIORITY"].append(c)
                    if "EMPTY_MARKERS" not in p_data:
                        p_data["EMPTY_MARKERS"] = GLOBAL_EMPTY_MARKERS.copy()
                return data
        except:
            return copy.deepcopy(DEFAULT_CONFIG)
    return copy.deepcopy(DEFAULT_CONFIG)


def save_settings(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


def clean_page_garbage(soup):
    for tag in soup.find_all(['input', 'textarea', 'button', 'nav', 'footer', 'header']):
        tag.decompose()
    return soup


def calculate_trust_score(soup, domain_name, text_full):
    trust_score = 0
    trust_signals = []
    contact_links = soup.find_all('a', href=re.compile(r'contact|about|team|advertising', re.I))
    if contact_links: trust_score += 1; trust_signals.append("Has Contact/About")
    author_signals = soup.find_all(['author', 'byline', 'rel="author"'])
    author_text = text_full[:1000]
    has_author = bool(author_signals) or 'written by' in author_text or 'автор:' in author_text
    fake_authors = ['admin', 'editor', 'guest', 'author']
    if has_author and not any(fa in author_text.lower() for fa in fake_authors):
        trust_score += 1;
        trust_signals.append("Has Real Author")
    time_tags = soup.find_all('time')
    if time_tags or re.findall(r'\d{1,2}\.\d{1,2}\.\d{2,4}', text_full[:1000]):
        trust_score += 1;
        trust_signals.append("Has Date")
    footer = soup.find('footer')
    if footer:
        links = footer.find_all('a', href=True)
        if len(links) < 10: trust_score += 1; trust_signals.append(f"Low Footer Links ({len(links)})")
    else:
        trust_score += 1;
        trust_signals.append("No Footer")
    seo_keywords = ['seo', 'rank', 'directory', 'linkdirectory', 'toplink', 'bestlink', 'websiteworth', 'marketing',
                    'guide']
    if not any(kw in domain_name for kw in seo_keywords):
        trust_score += 1;
        trust_signals.append("Clean Domain Name")
    social = soup.find_all('a',
                           href=re.compile(r'vk\.com|telegram|youtube\.com|twitter\.com|facebook\.com|instagram\.com',
                                           re.I))
    if social: trust_score += 1; trust_signals.append("Has Social Links")
    if soup.find('form'): trust_score += 1; trust_signals.append("Has Contact Form")
    ai_cliches = ['in today\'s digital', 'game-changer', 'unlock the power', 'ultimate guide', 'cutting-edge']
    ai_matches = sum(1 for c in ai_cliches if c in text_full.lower())
    if ai_matches == 0: trust_score += 1; trust_signals.append("No AI Cliches")
    return trust_score, trust_signals


def detect_pbn_signals(soup, domain_name, path, text_full):
    spam_score = 0
    spam_signals = []
    pbn_kw = ['seo', 'rank', 'directory', 'linkdirectory', 'websiteworth', 'marketing']
    for kw in pbn_kw:
        if kw in domain_name: spam_score += 2; spam_signals.append(f"Domain: {kw}"); break
    if path.count('/') >= 2 and len(path) > 60: spam_score += 1; spam_signals.append("Long SEO URL")
    h1 = soup.find('h1')
    if h1:
        h1_t = h1.get_text(strip=True).lower()
        if any(p in h1_t for p in ['unlock the power', 'ultimate guide', 'best way to']):
            spam_score += 2;
            spam_signals.append("H1 Spam Pattern")
    ai_matches = sum(1 for c in ['game-changer', 'digital landscape'] if c in text_full.lower())
    if ai_matches >= 2: spam_score += 2; spam_signals.append("AI Style Content")
    return spam_score, spam_signals


def check_aggressive_aggregator(soup, url):
    current_domain = urlparse(url).netloc.lower()
    content_area = soup.find('body')
    if not content_area: return False, ""
    all_links = content_area.find_all('a', href=True)
    external_links = [l for l in all_links if urlparse(l.get('href', '')).netloc.lower() not in ('', current_domain)]
    num_ext = len(external_links)
    text_content = content_area.get_text(separator=' ', strip=True)
    words = text_content.split()
    num_words = len(words)
    if num_ext > 25:
        ratio = num_words / num_ext
        if ratio < 3.5: return True, f"Aggressive Ext-Links (Ratio: {ratio:.1f}, Ext: {num_ext})"
    h1 = soup.find('h1')
    if h1:
        h1_text = h1.get_text().lower()
        if any(p in h1_text for p in ['most visited web pages', 'world\'s most', 'directory of']):
            return True, f"Spam Header Pattern: {h1_text[:30]}"
    return False, ""


def scan_logic(html_content, url, profile_config, is_search=False, search_term=None):
    soup = BeautifulSoup(html_content, 'html.parser')

    for noise in soup.find_all(['nav', 'footer', 'header', 'script', 'style', 'aside', 'form', 'title', 'meta']):
        noise.decompose()

    if is_search and search_term:
        raw_q = search_term.split('=')[-1] if '=' in search_term else search_term
        decoded_query = urllib.parse.unquote(raw_q).lower().strip()
        to_erase = [decoded_query]
        if ' ' in decoded_query:
            to_erase.extend([w for w in decoded_query.split() if len(w) > 3])

        for term in to_erase:
            for element in soup.find_all(string=re.compile(re.escape(term), re.I)):
                parent = element.parent
                if parent and parent.name not in ['body', 'html']:
                    parent.decompose()

        text_full = soup.get_text(separator=' ', strip=True).lower()
        for term in to_erase:
            text_full = re.sub(rf"{re.escape(term)}", " [CLEANED] ", text_full)
    else:
        text_full = soup.get_text(separator=' ', strip=True).lower()

    empty_markers = profile_config.get("EMPTY_MARKERS", GLOBAL_EMPTY_MARKERS)
    is_search_url = any(x in url.lower() for x in ['search', 'query', 'poisk', 's=']) or is_search
    search_empty = is_search_url and (
            not text_full.strip() or
            any(m in text_full for m in empty_markers) or
            len(text_full) < 150
    )

    if search_empty:
        return set(), set(), "[Search: No Results or Cleared Header]"

    if is_search and search_term:
        text_full = text_full.replace(search_term.lower(), "")

    found_cats, found_words, snippets = set(), set(), []
    parsed = urlparse(url)
    domain, path = parsed.netloc.lower(), parsed.path.lower()

    trust_score, _ = calculate_trust_score(soup, domain, text_full)
    spam_score, spam_signals = detect_pbn_signals(soup, domain, path, text_full)

    is_agg, agg_msg = check_aggressive_aggregator(soup, url)
    if is_agg:
        found_cats.add("Scraper/Aggregator")
        snippets.append(f"[System]: {agg_msg}")

    chinese_chars = re.findall(r'[\u4e00-\u9fff]', text_full)
    has_chinese = len(chinese_chars) > 10
    arabic_chars = re.findall(r'[\u0600-\u06FF]', text_full)
    has_arabic = len(arabic_chars) > 10

    categories = profile_config.get("CATEGORIES", {})
    cw_active = profile_config.get("CONTEXT_WARNING_ACTIVE", False)

    for cat_name, cat_data in categories.items():
        if trust_score >= 5 and cat_name == 'Scraper/Aggregator': continue

        # 1. Stop Words
        for item in cat_data.get("stop_words", []):
            if not item.get("active"): continue
            w = item["word"].lower()
            if re.search(rf"\b{re.escape(w)}\b", text_full, re.UNICODE):
                found_cats.add(cat_name)
                found_words.add(w)
                idx = text_full.find(w)
                snippets.append(f"[{w}]: ...{text_full[max(0, idx - 60):idx + 60]}...")

        # 2. Context Words
        for trigger_word, ctx in cat_data.get("context_words", {}).items():
            w = trigger_word.lower()
            if w in found_words:
                continue
            if re.search(rf"\b{re.escape(w)}\b", text_full, re.UNICODE):
                # Проверка Алиби (Good)
                alibi_found = False
                for good_item in ctx.get('good', []):
                    if isinstance(good_item, dict):
                        if not good_item.get("active", True): continue
                        gw = good_item["word"].lower()
                    else:
                        gw = str(good_item).lower()

                    if re.search(rf"\b{re.escape(gw)}\b", text_full, re.UNICODE):
                        alibi_found = True
                        break
                if alibi_found:
                    continue

                # Проверка Улик (Bad)
                bad_found = None
                for bad_item in ctx.get('bad', []):
                    if isinstance(bad_item, dict):
                        if not bad_item.get("active"): continue
                        bw = bad_item["word"].lower()
                    else:
                        bw = str(bad_item).lower()

                    if re.search(rf"\b{re.escape(bw)}\b", text_full, re.UNICODE):
                        bad_found = bw
                        break

                if bad_found:
                    found_cats.add(cat_name)
                    found_words.add(w)
                    idx = text_full.find(w)
                    snippets.append(f"[{w} + {bad_found}]: ...{text_full[max(0, idx - 60):idx + 60]}...")
                else:
                    if cw_active:
                        found_cats.add("Context Warning")
                        found_words.add(w)
                        idx = text_full.find(w)
                        snippets.append(f"[{w} (No Alibi)]: ...{text_full[max(0, idx - 60):idx + 60]}...")

    if spam_score >= 4:
        found_cats.add("SEO/PBN")
        found_words.add(f"Score: {spam_score}")

    if has_chinese and not found_cats:
        found_cats.add("Chinese Content")
        found_words.add(f"Chars: {len(chinese_chars)}")

    if has_arabic and not found_cats:
        found_cats.add("Arabic Content")
        found_words.add(f"Chars: {len(arabic_chars)}")

    res_snip = f"[Trust: {trust_score}/5] [PBN: {spam_score}/4] " + (" | ".join(snippets))
    return found_cats, found_words, res_snip


class WorkerSignals(QObject):
    result = pyqtSignal(int, dict)


class CheckTask(QRunnable):
    def __init__(self, index, url, profile_config, search_queries, proxies=None):
        super().__init__()
        self.index = index
        self.url = url
        self.profile_config = profile_config
        self.search_queries = search_queries
        self.proxies = proxies
        self.signals = WorkerSignals()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
        }

    def prepare_search_query(self, query):
        if len(query) < 4:
            return f"{query}*"
        return query

    def extract_search_forms(self, soup, base_url):
        search_requests = []
        for form in soup.find_all('form'):
            action = form.get('action', '')
            method = form.get('method', 'get').lower()

            inputs = form.find_all('input')
            search_input = None
            params = {}

            for i in inputs:
                i_type = i.get('type', 'text').lower()
                i_name = i.get('name')
                if not i_name: continue

                if i_type == 'hidden':
                    params[i_name] = i.get('value', '')
                elif i_type in ['text', 'search'] and not search_input:
                    if re.search(r'search|query|q|text|term|find|poisk|word', i_name + str(i.get('placeholder', '')),
                                 re.I):
                        search_input = i_name
                    elif not search_input:
                        search_input = i_name

            if search_input:
                full_url = urljoin(base_url, action)
                search_requests.append({
                    'url': full_url,
                    'input_name': search_input,
                    'params': params,
                    'method': method
                })
        return search_requests

    def run(self):
        f_url = self.url if self.url.startswith('http') else 'http://' + self.url
        res = {"URL": self.url, "Result": "Clean", "Words": "-", "Snip": "-", "Checked": f_url}

        try:
            r = requests.get(f_url, headers=self.headers, timeout=12, allow_redirects=True, proxies=self.proxies)
            final_url = r.url
            soup = BeautifulSoup(r.content, 'html.parser')
            is_redirected = urlparse(f_url).netloc.lower() != urlparse(final_url).netloc.lower()

            c, w, s = scan_logic(r.content, final_url, self.profile_config)

            if not c:
                search_targets = []
                dynamic_forms = self.extract_search_forms(soup, final_url)

                safe_queries = [self.prepare_search_query(q) for q in self.search_queries]

                for df in dynamic_forms:
                    for q in safe_queries:
                        params = df['params'].copy()
                        params[df['input_name']] = q
                        qs = urlencode(params)
                        target_url = f"{df['url']}?{qs}" if '?' not in df['url'] else f"{df['url']}?{qs}"
                        search_targets.append(target_url)

                base = f"{urlparse(final_url).scheme}://{urlparse(final_url).netloc}"
                fallback_templates = ["/?s={q}", "/search?q={q}", "/search/?text={q}", "/poisk?searchword={q}"]
                for q in safe_queries:
                    for t in fallback_templates:
                        search_targets.append(urljoin(base, t.format(q=q)))

                search_targets = list(dict.fromkeys(search_targets))

                for target_link in search_targets:
                    try:
                        sr = requests.get(target_link, headers=self.headers, timeout=8, proxies=self.proxies)
                        if sr.status_code == 200:
                            sc, sw, ss = scan_logic(sr.content, sr.url, self.profile_config, True, target_link)
                            if sc:
                                c, w, s, res["Checked"] = sc, sw, ss, sr.url
                                break
                    except:
                        continue

            if c:
                res["Result"] = ("[Redirect] " if is_redirected else "") + ", ".join(c)
                res["Words"] = ", ".join(w)
                res["Snip"] = s
            elif is_redirected:
                res["Result"] = "[Redirect] Clean"
                res["Checked"] = final_url
        except Exception as e:
            res["Result"] = "Error"
            res["Snip"] = str(e)
        self.signals.result.emit(self.index, res)


class AnalysisWorker(QThread):
    progress = pyqtSignal(int, dict)
    finished = pyqtSignal(bool)

    def __init__(self, tasks, profile_config, max_threads=20, proxies=None):
        super().__init__()
        self.tasks = tasks
        self.profile_config = profile_config
        self.proxies = proxies
        self.is_running = True
        self.pool = QThreadPool()
        self.pool.setMaxThreadCount(max_threads)

    def stop(self):
        self.is_running = False
        self.pool.clear()

    def run(self):
        search_queries = []
        categories = self.profile_config.get("CATEGORIES", {})
        for cat in ['Gambling', 'Adult']:
            if cat in categories:
                search_queries.extend([w['word'] for w in categories[cat].get('stop_words', []) if w.get('active')])
        search_queries = list(dict.fromkeys(search_queries))[:10]

        for idx, url in self.tasks:
            if not self.is_running: break
            task = CheckTask(idx, url, self.profile_config, search_queries, self.proxies)
            task.signals.result.connect(self.handle_result)
            self.pool.start(task)
        self.pool.waitForDone()
        self.finished.emit(self.is_running)

    def handle_result(self, i, res):
        if self.is_running: self.progress.emit(i, res)


class WordEditorDialog(QDialog):
    def __init__(self, settings, profile_name, parent=None):
        super().__init__(parent)
        self.settings, self.profile = settings, profile_name
        self.setWindowTitle(f"Настройка: {profile_name}")
        self.resize(750, 800)
        layout = QVBoxLayout(self)

        # --- НАСТРОЙКА ПОТОКОВ ---
        thread_lay = QHBoxLayout()
        thread_lay.addWidget(QLabel("Количество потоков:"))
        self.thread_spin = QSpinBox()
        self.thread_spin.setRange(1, 100)
        self.thread_spin.setValue(self.settings[self.profile].get("THREADS", 20))
        self.thread_spin.valueChanged.connect(self.update_threads)
        thread_lay.addWidget(self.thread_spin)
        thread_lay.addStretch()
        layout.addLayout(thread_lay)

        # --- НАСТРОЙКА ПРОКСИ ---
        proxy_lay = QVBoxLayout()
        self.proxy_cb = QCheckBox("Использовать прокси")
        self.proxy_cb.setChecked(self.settings[self.profile].get("USE_PROXY", False))
        self.proxy_cb.toggled.connect(self.update_proxy)
        proxy_lay.addWidget(self.proxy_cb)

        proxy_inputs_lay = QHBoxLayout()
        self.proxy_host = QLineEdit()
        self.proxy_host.setPlaceholderText("IP / Host")
        self.proxy_host.setText(self.settings[self.profile].get("PROXY_HOST", ""))
        self.proxy_host.textChanged.connect(self.update_proxy)

        self.proxy_port = QLineEdit()
        self.proxy_port.setPlaceholderText("Port")
        self.proxy_port.setText(self.settings[self.profile].get("PROXY_PORT", ""))
        self.proxy_port.textChanged.connect(self.update_proxy)

        self.proxy_user = QLineEdit()
        self.proxy_user.setPlaceholderText("Логин (опционально)")
        self.proxy_user.setText(self.settings[self.profile].get("PROXY_USER", ""))
        self.proxy_user.textChanged.connect(self.update_proxy)

        self.proxy_pass = QLineEdit()
        self.proxy_pass.setPlaceholderText("Пароль (опционально)")
        self.proxy_pass.setEchoMode(QLineEdit.EchoMode.Password)
        self.proxy_pass.setText(self.settings[self.profile].get("PROXY_PASS", ""))
        self.proxy_pass.textChanged.connect(self.update_proxy)

        proxy_inputs_lay.addWidget(self.proxy_host)
        proxy_inputs_lay.addWidget(self.proxy_port)
        proxy_inputs_lay.addWidget(self.proxy_user)
        proxy_inputs_lay.addWidget(self.proxy_pass)

        proxy_lay.addLayout(proxy_inputs_lay)
        layout.addLayout(proxy_lay)

        self.update_proxy_fields_state(self.proxy_cb.isChecked())

        # --- CONTEXT WARNING ---
        cw_lay = QHBoxLayout()
        self.cw_cb = QCheckBox("Context Warning (без алиби)")
        self.cw_cb.setChecked(self.settings[self.profile].get("CONTEXT_WARNING_ACTIVE", False))
        self.cw_cb.toggled.connect(self.update_cw)
        self.btn_cw_color = QPushButton("🎨 Цвет")
        self.btn_cw_color.clicked.connect(self.change_cw_color)
        cw_color = self.settings[self.profile].get("CONTEXT_WARNING_COLOR", "#FF8C00")
        self.btn_cw_color.setStyleSheet(f"background-color: {cw_color}; color: white; border: 1px solid #555;")

        cw_lay.addWidget(self.cw_cb)
        cw_lay.addWidget(self.btn_cw_color)
        cw_lay.addStretch()
        layout.addLayout(cw_lay)
        layout.addWidget(QLabel("<hr>"))

        # --- ПРИОРИТЕТ КАТЕГОРИЙ (Drag & Drop) ---
        prio_lay = QVBoxLayout()
        prio_lay.addWidget(QLabel("<b>Приоритет покраски (Drag & Drop):</b>"))
        self.prio_list = QListWidget()
        self.prio_list.setFixedHeight(120)
        self.prio_list.setDragDropMode(QAbstractItemView.DragDropMode.InternalMove)
        self.prio_list.model().rowsMoved.connect(self.save_priority)
        prio_lay.addWidget(self.prio_list)
        layout.addLayout(prio_lay)
        self.refresh_priority()
        layout.addWidget(QLabel("<hr>"))

        # --- КАТЕГОРИИ И ИЕРАРХИЯ ---
        # 1. Категория
        cat_row = QHBoxLayout()
        self.cat_sel = QComboBox()
        self.cat_sel.currentTextChanged.connect(self.on_category_changed)

        self.btn_cat_color = QPushButton("🎨 Цвет")
        self.btn_cat_color.clicked.connect(self.change_category_color)

        btn_add_cat = QPushButton("+");
        btn_add_cat.setFixedWidth(30);
        btn_add_cat.clicked.connect(self.add_cat)
        btn_del_cat = QPushButton("-");
        btn_del_cat.setFixedWidth(30);
        btn_del_cat.clicked.connect(self.remove_cat)

        cat_row.addWidget(QLabel("1) Категория:"))
        cat_row.addWidget(self.cat_sel)
        cat_row.addWidget(self.btn_cat_color)
        cat_row.addWidget(btn_add_cat)
        cat_row.addWidget(btn_del_cat)
        layout.addLayout(cat_row)

        # 2. Тип словаря
        type_row = QHBoxLayout()
        self.type_sel = QComboBox()
        self.type_sel.addItems(["STOP_WORDS", "CONTEXT_WORDS", "EMPTY_MARKERS"])
        self.type_sel.currentTextChanged.connect(self.on_type_changed)
        type_row.addWidget(QLabel("2) Тип словаря:"))
        type_row.addWidget(self.type_sel)
        type_row.addStretch()
        layout.addLayout(type_row)

        # 3. Контекстное слово (Триггер)
        self.trigger_widget = QWidget()
        trig_row = QHBoxLayout(self.trigger_widget)
        trig_row.setContentsMargins(0, 0, 0, 0)
        self.trigger_sel = QComboBox()
        self.trigger_sel.currentTextChanged.connect(self.refresh_lists)
        btn_add_trig = QPushButton("+");
        btn_add_trig.setFixedWidth(30);
        btn_add_trig.clicked.connect(self.add_trigger)
        btn_del_trig = QPushButton("-");
        btn_del_trig.setFixedWidth(30);
        btn_del_trig.clicked.connect(self.remove_trigger)
        trig_row.addWidget(QLabel("3) Контекстное слово:"))
        trig_row.addWidget(self.trigger_sel)
        trig_row.addWidget(btn_add_trig)
        trig_row.addWidget(btn_del_trig)
        layout.addWidget(self.trigger_widget)

        # 4. Списки слов (Улики и Алиби)
        lists_lay = QHBoxLayout()

        # Улики (Bad / Stop words)
        bad_group = QWidget()
        bad_v = QVBoxLayout(bad_group)
        bad_v.setContentsMargins(0, 0, 0, 0)
        self.bad_label = QLabel("Улики (Bad)")
        self.bad_label.setStyleSheet("font-weight: bold; color: #ff6666;")
        bad_v.addWidget(self.bad_label)

        self.bad_scroll = QScrollArea();
        self.bad_scroll.setWidgetResizable(True)
        self.bad_cont = QWidget();
        self.bad_lay = QVBoxLayout(self.bad_cont);
        self.bad_lay.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.bad_scroll.setWidget(self.bad_cont)
        bad_v.addWidget(self.bad_scroll)

        bad_add_lay = QHBoxLayout()
        self.bad_in = QLineEdit()
        bad_add_btn = QPushButton("Добавить")
        bad_add_btn.clicked.connect(lambda: self.add_word("bad"))
        bad_add_lay.addWidget(self.bad_in);
        bad_add_lay.addWidget(bad_add_btn)
        bad_v.addLayout(bad_add_lay)
        lists_lay.addWidget(bad_group)

        # Алиби (Good)
        self.good_group = QWidget()
        good_v = QVBoxLayout(self.good_group)
        good_v.setContentsMargins(0, 0, 0, 0)
        self.good_label = QLabel("Алиби (Good)")
        self.good_label.setStyleSheet("font-weight: bold; color: #66cc66;")
        good_v.addWidget(self.good_label)

        self.good_scroll = QScrollArea();
        self.good_scroll.setWidgetResizable(True)
        self.good_cont = QWidget();
        self.good_lay = QVBoxLayout(self.good_cont);
        self.good_lay.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.good_scroll.setWidget(self.good_cont)
        good_v.addWidget(self.good_scroll)

        good_add_lay = QHBoxLayout()
        self.good_in = QLineEdit()
        good_add_btn = QPushButton("Добавить")
        good_add_btn.clicked.connect(lambda: self.add_word("good"))
        good_add_lay.addWidget(self.good_in);
        good_add_lay.addWidget(good_add_btn)
        good_v.addLayout(good_add_lay)
        lists_lay.addWidget(self.good_group)

        layout.addLayout(lists_lay)

        # Инициализация UI
        self.refresh_categories()

    def update_proxy_fields_state(self, state):
        self.proxy_host.setEnabled(state)
        self.proxy_port.setEnabled(state)
        self.proxy_user.setEnabled(state)
        self.proxy_pass.setEnabled(state)

    def update_proxy(self):
        state = self.proxy_cb.isChecked()
        self.update_proxy_fields_state(state)
        self.settings[self.profile]["USE_PROXY"] = state
        self.settings[self.profile]["PROXY_HOST"] = self.proxy_host.text()
        self.settings[self.profile]["PROXY_PORT"] = self.proxy_port.text()
        self.settings[self.profile]["PROXY_USER"] = self.proxy_user.text()
        self.settings[self.profile]["PROXY_PASS"] = self.proxy_pass.text()
        save_settings(self.settings)

    def update_threads(self, val):
        self.settings[self.profile]["THREADS"] = val
        save_settings(self.settings)

    def update_cw(self):
        self.settings[self.profile]["CONTEXT_WARNING_ACTIVE"] = self.cw_cb.isChecked()
        save_settings(self.settings)

    def change_cw_color(self):
        current_color = self.settings[self.profile].get("CONTEXT_WARNING_COLOR", "#FF8C00")
        color = QColorDialog.getColor(QColor(current_color), self, "Цвет для Context Warning")
        if color.isValid():
            self.settings[self.profile]["CONTEXT_WARNING_COLOR"] = color.name()
            save_settings(self.settings)
            self.btn_cw_color.setStyleSheet(f"background-color: {color.name()}; color: white; border: 1px solid #555;")

    def refresh_priority(self):
        self.prio_list.clear()
        for p in self.settings[self.profile].get("PRIORITY", []):
            self.prio_list.addItem(p)

    def save_priority(self):
        self.settings[self.profile]["PRIORITY"] = [self.prio_list.item(i).text() for i in range(self.prio_list.count())]
        save_settings(self.settings)

    def refresh_categories(self):
        self.cat_sel.blockSignals(True)
        self.cat_sel.clear()
        cats = self.settings[self.profile].get("CATEGORIES", {})
        self.cat_sel.addItems(cats.keys())
        self.cat_sel.blockSignals(False)
        self.on_category_changed()

    def on_category_changed(self):
        cat = self.cat_sel.currentText()
        if not cat:
            self.btn_cat_color.setEnabled(False)
            self.btn_cat_color.setStyleSheet("")
            return

        self.btn_cat_color.setEnabled(True)
        c_color = self.settings[self.profile]["CATEGORIES"][cat].get("color", "#4c4c4c")
        self.btn_cat_color.setStyleSheet(f"background-color: {c_color}; color: white; border: 1px solid #555;")
        self.on_type_changed()

    def change_category_color(self):
        c = self.cat_sel.currentText()
        if not c: return
        current_color = self.settings[self.profile]["CATEGORIES"][c].get("color", "#4c4c4c")
        color = QColorDialog.getColor(QColor(current_color), self, f"Выберите цвет для {c}")
        if color.isValid():
            self.settings[self.profile]["CATEGORIES"][c]["color"] = color.name()
            save_settings(self.settings)
            self.btn_cat_color.setStyleSheet(f"background-color: {color.name()}; color: white; border: 1px solid #555;")

    def add_cat(self):
        n, ok = QInputDialog.getText(self, "Категория", "Имя новой категории:")
        if ok and n:
            n = n.strip()
            if not n: return
            if "CATEGORIES" not in self.settings[self.profile]:
                self.settings[self.profile]["CATEGORIES"] = {}
            if n not in self.settings[self.profile]["CATEGORIES"]:
                self.settings[self.profile]["CATEGORIES"][n] = {
                    "color": "#4c4c4c", "stop_words": [], "context_words": {}
                }
                if "PRIORITY" not in self.settings[self.profile]:
                    self.settings[self.profile]["PRIORITY"] = []
                self.settings[self.profile]["PRIORITY"].append(n)
                save_settings(self.settings)
                self.refresh_categories()
                self.refresh_priority()
                self.cat_sel.setCurrentText(n)

    def remove_cat(self):
        c = self.cat_sel.currentText()
        if not c: return
        reply = QMessageBox.question(
            self, 'Удаление', f'Удалить всю категорию "{c}"?\nЭто действие нельзя отменить.',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            del self.settings[self.profile]["CATEGORIES"][c]
            if c in self.settings[self.profile].get("PRIORITY", []):
                self.settings[self.profile]["PRIORITY"].remove(c)
            save_settings(self.settings)
            self.refresh_categories()
            self.refresh_priority()

    def on_type_changed(self):
        dtype = self.type_sel.currentText()
        if dtype == "EMPTY_MARKERS":
            self.cat_sel.setEnabled(False)
            self.btn_cat_color.setEnabled(False)
            self.trigger_widget.hide()
            self.good_group.hide()
            self.bad_label.setText("Маркеры пустого поиска (EMPTY_MARKERS)")
            self.refresh_lists()
        elif dtype == "STOP_WORDS":
            self.cat_sel.setEnabled(True)
            self.btn_cat_color.setEnabled(True)
            self.trigger_widget.hide()
            self.good_group.hide()
            self.bad_label.setText("Слова (STOP_WORDS)")
            self.refresh_lists()
        else:
            self.cat_sel.setEnabled(True)
            self.btn_cat_color.setEnabled(True)
            self.trigger_widget.show()
            self.good_group.show()
            self.bad_label.setText("Улики (Bad)")

            self.trigger_sel.blockSignals(True)
            self.trigger_sel.clear()
            cat = self.cat_sel.currentText()
            if cat:
                trigs = self.settings[self.profile]["CATEGORIES"][cat].get("context_words", {}).keys()
                self.trigger_sel.addItems(trigs)
            self.trigger_sel.blockSignals(False)
            self.refresh_lists()

    def add_trigger(self):
        cat = self.cat_sel.currentText()
        if not cat: return
        n, ok = QInputDialog.getText(self, "Контекстное слово", "Триггер:")
        if ok and n:
            n = n.strip()
            if not n: return
            if "context_words" not in self.settings[self.profile]["CATEGORIES"][cat]:
                self.settings[self.profile]["CATEGORIES"][cat]["context_words"] = {}
            if n not in self.settings[self.profile]["CATEGORIES"][cat]["context_words"]:
                self.settings[self.profile]["CATEGORIES"][cat]["context_words"][n] = {"bad": [], "good": []}
                save_settings(self.settings)
                self.on_type_changed()
                self.trigger_sel.setCurrentText(n)

    def remove_trigger(self):
        cat = self.cat_sel.currentText()
        trig = self.trigger_sel.currentText()
        if not cat or not trig: return
        reply = QMessageBox.question(
            self, 'Удаление', f'Удалить контекстное слово "{trig}"?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No
        )
        if reply == QMessageBox.StandardButton.Yes:
            del self.settings[self.profile]["CATEGORIES"][cat]["context_words"][trig]
            save_settings(self.settings)
            self.on_type_changed()

    def _clear_layout(self, layout):
        while layout.count():
            w = layout.takeAt(0).widget()
            if w: w.deleteLater()

    def refresh_lists(self):
        self._clear_layout(self.bad_lay)
        self._clear_layout(self.good_lay)

        dtype = self.type_sel.currentText()

        if dtype == "EMPTY_MARKERS":
            markers = self.settings[self.profile].get("EMPTY_MARKERS", [])
            for m in markers:
                self._add_row_to_layout(self.bad_lay, {"word": m}, "empty")
            return

        cat = self.cat_sel.currentText()
        if not cat: return

        cat_data = self.settings[self.profile]["CATEGORIES"][cat]

        if dtype == "STOP_WORDS":
            bad_items = cat_data.get("stop_words", [])
            good_items = []
        else:
            trig = self.trigger_sel.currentText()
            if not trig: return
            bad_items = cat_data.get("context_words", {}).get(trig, {}).get("bad", [])
            good_items = cat_data.get("context_words", {}).get(trig, {}).get("good", [])

        # Заполняем Bad / Stop
        for item in bad_items:
            self._add_row_to_layout(self.bad_lay, item, "bad")

        # Заполняем Good
        if dtype == "CONTEXT_WORDS":
            for item in good_items:
                self._add_row_to_layout(self.good_lay, item, "good")

    def _add_row_to_layout(self, layout, item, list_type):
        row = QWidget()
        h = QHBoxLayout(row)

        if list_type == "empty":
            lbl = QLabel(item["word"])
            h.addWidget(lbl)
            h.addStretch()
            btn = QPushButton("×")
            btn.setFixedSize(20, 20)
            btn.clicked.connect(lambda s, i=item, lt=list_type: self.rem_word(i, lt))
            h.addWidget(btn)
        else:
            cb = QCheckBox(item["word"])
            cb.setChecked(item.get("active", True))
            cb.toggled.connect(lambda s, i=item: self.upd_word(i, s))
            btn = QPushButton("×")
            btn.setFixedSize(20, 20)
            btn.clicked.connect(lambda s, i=item, lt=list_type: self.rem_word(i, lt))
            h.addWidget(cb)
            h.addStretch()
            h.addWidget(btn)

        layout.addWidget(row)

    def upd_word(self, item, state):
        item["active"] = state
        save_settings(self.settings)

    def rem_word(self, item, list_type):
        dtype = self.type_sel.currentText()
        if dtype == "EMPTY_MARKERS":
            if item["word"] in self.settings[self.profile].get("EMPTY_MARKERS", []):
                self.settings[self.profile]["EMPTY_MARKERS"].remove(item["word"])
        else:
            cat = self.cat_sel.currentText()
            if dtype == "STOP_WORDS":
                self.settings[self.profile]["CATEGORIES"][cat]["stop_words"].remove(item)
            else:
                trig = self.trigger_sel.currentText()
                self.settings[self.profile]["CATEGORIES"][cat]["context_words"][trig][list_type].remove(item)

        save_settings(self.settings)
        self.refresh_lists()

    def add_word(self, list_type):
        input_field = self.bad_in if list_type in ("bad", "empty") else self.good_in
        t = input_field.text().strip()
        if not t: return

        dtype = self.type_sel.currentText()

        if dtype == "EMPTY_MARKERS":
            if "EMPTY_MARKERS" not in self.settings[self.profile]:
                self.settings[self.profile]["EMPTY_MARKERS"] = []
            if t not in self.settings[self.profile]["EMPTY_MARKERS"]:
                self.settings[self.profile]["EMPTY_MARKERS"].append(t)
        else:
            cat = self.cat_sel.currentText()
            new_item = {"word": t, "active": True}

            if dtype == "STOP_WORDS":
                if "stop_words" not in self.settings[self.profile]["CATEGORIES"][cat]:
                    self.settings[self.profile]["CATEGORIES"][cat]["stop_words"] = []
                self.settings[self.profile]["CATEGORIES"][cat]["stop_words"].append(new_item)
            else:
                trig = self.trigger_sel.currentText()
                if not trig: return
                if list_type not in self.settings[self.profile]["CATEGORIES"][cat]["context_words"][trig]:
                    self.settings[self.profile]["CATEGORIES"][cat]["context_words"][trig][list_type] = []
                self.settings[self.profile]["CATEGORIES"][cat]["context_words"][trig][list_type].append(new_item)

        save_settings(self.settings)
        input_field.clear()
        self.refresh_lists()


class LinkDetectiveGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings, self.current_profile = load_settings(), "Default"
        self.is_dark_theme = False
        self.setWindowTitle("LINK DETECTIVE V1.5")
        self.resize(1350, 850)
        self.loaded_urls = []
        self.completed_count = 0
        self.redirect_color = QColor("#0D47A1")
        self.init_ui()

    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(0)
        main_layout.setContentsMargins(0, 0, 0, 0)
        self.top_p = QWidget()
        self.top_p.setFixedHeight(60)
        top_v = QVBoxLayout(self.top_p)
        r1 = QHBoxLayout()
        self.combo_p = QComboBox()
        self.combo_p.addItems(self.settings.keys())
        r1.addWidget(QLabel("Профиль:"))
        r1.addWidget(self.combo_p)

        self.btn_add_profile = QPushButton("+", clicked=self.add_profile)
        self.btn_add_profile.setFixedWidth(30)
        self.btn_del_profile = QPushButton("-", clicked=self.del_profile)
        self.btn_del_profile.setFixedWidth(30)

        r1.addWidget(self.btn_add_profile)
        r1.addWidget(self.btn_del_profile)

        # Кнопки импорта/экспорта профиля
        self.btn_export_profile = QPushButton("⬇ Экспорт", clicked=self.export_profile)
        self.btn_import_profile = QPushButton("⬆ Импорт", clicked=self.import_profile)
        r1.addWidget(self.btn_export_profile)
        r1.addWidget(self.btn_import_profile)

        r1.addWidget(QPushButton("⚙", clicked=self.open_editor))
        r1.addStretch()

        self.btn_theme = QPushButton("🌙 Темная тема", clicked=self.toggle_theme)
        r1.addWidget(self.btn_theme)

        top_v.addLayout(r1)
        r2 = QHBoxLayout()
        r2.addWidget(QPushButton("Загрузить список", clicked=self.load_file))

        self.btn_majestic = QPushButton("Majestic Clipboard", clicked=self.load_majestic_clipboard)
        r2.addWidget(self.btn_majestic)

        r2.addWidget(QPushButton("Очистить", clicked=self.clear_list))
        self.lbl_c = QLabel("Доменов: 0")
        r2.addStretch()
        r2.addWidget(self.lbl_c)
        top_v.addLayout(r2)
        main_layout.addWidget(self.top_p)
        self.splitter = QSplitter(Qt.Orientation.Horizontal)

        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(["URL", "Результат", "Слова", "Сниппет", "Проверка", "TF", "CF", "TR"])
        self.table.itemSelectionChanged.connect(self.update_snip)
        self.splitter.addWidget(self.table)
        self.snip_view = QTextEdit()
        self.snip_view.setReadOnly(True)
        self.snip_view.setStyleSheet("background:#1e1e1e; color:#d4d4d4;")
        self.splitter.addWidget(self.snip_view)
        main_layout.addWidget(self.splitter)
        act = QHBoxLayout()
        self.btn_go = QPushButton("START INVESTIGATION", clicked=lambda: self.run_inv(True))
        self.btn_stop = QPushButton("STOP", clicked=self.handle_stop)
        self.btn_stop.setEnabled(False)
        self.btn_exp = QPushButton("EXPORT", clicked=self.export)
        self.btn_exp.setEnabled(False)
        act.addWidget(self.btn_go, 4)
        act.addWidget(self.btn_stop, 2)
        act.addWidget(self.btn_exp, 4)
        main_layout.addLayout(act)
        self.pbar = QProgressBar()
        self.pbar.setFixedHeight(12)
        main_layout.addWidget(self.pbar)
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self.show_menu)

    def add_profile(self):
        name, ok = QInputDialog.getText(self, "Новый профиль", "Имя профиля:")
        if ok and name:
            name = name.strip()
            if not name or name in self.settings:
                QMessageBox.warning(self, "Ошибка", "Некорректное имя или профиль уже существует.")
                return

            reply = QMessageBox.question(
                self, "Создание профиля",
                "Скопировать настройки текущего профиля?\n(Нажмите 'No' для создания пустого профиля по умолчанию)",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )

            if reply == QMessageBox.StandardButton.Yes:
                self.settings[name] = copy.deepcopy(self.settings[self.combo_p.currentText()])
            else:
                self.settings[name] = copy.deepcopy(DEFAULT_CONFIG["Default"])

            save_settings(self.settings)
            self.combo_p.addItem(name)
            self.combo_p.setCurrentText(name)

    def del_profile(self):
        name = self.combo_p.currentText()
        if len(self.settings) <= 1:
            QMessageBox.warning(self, "Ошибка", "Нельзя удалить единственный профиль.")
            return

        reply = QMessageBox.question(self, "Удаление", f"Удалить профиль '{name}'?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            del self.settings[name]
            save_settings(self.settings)
            self.combo_p.removeItem(self.combo_p.currentIndex())

    def export_profile(self):
        prof_name = self.combo_p.currentText()
        p, _ = QFileDialog.getSaveFileName(self, "Экспорт профиля", f"{prof_name}.json", "JSON Files (*.json)")
        if p:
            try:
                with open(p, 'w', encoding='utf-8') as f:
                    json.dump(self.settings[prof_name], f, ensure_ascii=False, indent=4)
                QMessageBox.information(self, "Успех", f"Профиль '{prof_name}' успешно экспортирован!")
            except Exception as e:
                QMessageBox.warning(self, "Ошибка", f"Не удалось экспортировать профиль:\n{str(e)}")

    def import_profile(self):
        p, _ = QFileDialog.getOpenFileName(self, "Импорт профиля", "", "JSON Files (*.json)")
        if p:
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    imported_data = json.load(f)

                if "CATEGORIES" not in imported_data:
                    raise ValueError("Файл не содержит валидных настроек профиля (отсутствует ключ CATEGORIES).")

                base_name = os.path.splitext(os.path.basename(p))[0]
                name, ok = QInputDialog.getText(self, "Имя профиля", "Введите имя для импортируемого профиля:",
                                                text=base_name)
                if ok and name:
                    name = name.strip()
                    if not name: return

                    if name in self.settings:
                        reply = QMessageBox.question(
                            self, "Перезаписать?",
                            f"Профиль с именем '{name}' уже существует.\nВы хотите перезаписать его?",
                            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                        )
                        if reply != QMessageBox.StandardButton.Yes:
                            return

                    # Применяем миграции для импортированного профиля (на случай старых версий)
                    if "PRIORITY" not in imported_data:
                        imported_data["PRIORITY"] = list(imported_data["CATEGORIES"].keys())
                    if "EMPTY_MARKERS" not in imported_data:
                        imported_data["EMPTY_MARKERS"] = GLOBAL_EMPTY_MARKERS.copy()
                    if "CONTEXT_WARNING_ACTIVE" not in imported_data:
                        imported_data["CONTEXT_WARNING_ACTIVE"] = False
                    if "CONTEXT_WARNING_COLOR" not in imported_data:
                        imported_data["CONTEXT_WARNING_COLOR"] = "#FF8C00"

                    self.settings[name] = imported_data
                    save_settings(self.settings)

                    if self.combo_p.findText(name) == -1:
                        self.combo_p.addItem(name)
                    self.combo_p.setCurrentText(name)
                    QMessageBox.information(self, "Успех", "Профиль успешно импортирован!")
            except Exception as e:
                QMessageBox.warning(self, "Ошибка", f"Не удалось импортировать профиль:\n{str(e)}")

    def toggle_theme(self):
        self.is_dark_theme = not self.is_dark_theme
        if self.is_dark_theme:
            self.setStyleSheet(DARK_STYLE)
            self.btn_theme.setText("☀️ Светлая тема")
        else:
            self.setStyleSheet("")
            self.btn_theme.setText("🌙 Темная тема")

    def show_menu(self, pos):
        item = self.table.itemAt(pos)
        if not item or item.column() not in (0, 4): return
        menu = QMenu(self)
        menu.addAction("Копировать", lambda: QApplication.clipboard().setText(item.text()))
        menu.addAction("Открыть",
                       lambda: webbrowser.open(item.text() if 'http' in item.text() else 'http://' + item.text()))
        menu.exec(self.table.viewport().mapToGlobal(pos))

    def open_editor(self):
        WordEditorDialog(self.settings, self.combo_p.currentText(), self).exec()

    def handle_stop(self):
        if self.btn_stop.text() == "STOP":
            if hasattr(self, 'worker') and self.worker.isRunning():
                self.btn_stop.setText("Остановка...")
                self.btn_stop.setEnabled(False)
                self.worker.stop()
        elif self.btn_stop.text() == "RESUME":
            self.run_inv(restart=False)

    def run_inv(self, restart=True):
        if not self.loaded_urls: return
        tasks_to_run = []
        if restart:
            self.table.setRowCount(len(self.loaded_urls))
            self.pbar.setMaximum(len(self.loaded_urls))
            self.pbar.setValue(0)
            self.completed_count = 0
            for r in range(self.table.rowCount()):
                for c in range(1, 5): self.table.setItem(r, c, QTableWidgetItem("-"))
            tasks_to_run = list(enumerate(self.loaded_urls))
        else:
            for r in range(self.table.rowCount()):
                item = self.table.item(r, 1)
                if item and item.text() == "-": tasks_to_run.append((r, self.loaded_urls[r]))
        if not tasks_to_run: return
        self.btn_go.setEnabled(False)
        self.btn_stop.setEnabled(True)
        self.btn_exp.setEnabled(False)
        self.btn_stop.setText("STOP")
        p = self.settings[self.combo_p.currentText()]

        thread_count = p.get("THREADS", 20)

        proxies = None
        if p.get("USE_PROXY", False) and p.get("PROXY_HOST") and p.get("PROXY_PORT"):
            host = p["PROXY_HOST"]
            port = p["PROXY_PORT"]
            user = p.get("PROXY_USER", "")
            pwd = p.get("PROXY_PASS", "")
            if user and pwd:
                proxy_url = f"http://{user}:{pwd}@{host}:{port}"
            else:
                proxy_url = f"http://{host}:{port}"
            proxies = {"http": proxy_url, "https": proxy_url}

        self.worker = AnalysisWorker(tasks_to_run, p, thread_count, proxies)
        self.worker.progress.connect(self.fill)
        self.worker.finished.connect(self.on_fin)
        self.worker.start()

    def fill(self, i, d):
        if i >= self.table.rowCount(): return
        for ci, k in enumerate(["URL", "Result", "Words", "Snip", "Checked"]):
            it = QTableWidgetItem(str(d[k]))
            if k == "Result":
                res_text = str(d[k])
                color_applied = False

                prof_settings = self.settings[self.combo_p.currentText()]

                # Сначала проверяем Context Warning
                if "Context Warning" in res_text and prof_settings.get("CONTEXT_WARNING_ACTIVE", False):
                    it.setBackground(QColor(prof_settings.get("CONTEXT_WARNING_COLOR", "#FF8C00")))
                    color_applied = True
                else:
                    # Затем проверяем категории с учетом приоритета (Drag & Drop)
                    prio_list = prof_settings.get("PRIORITY", [])
                    found_cats_in_res = [cat for cat in prof_settings.get("CATEGORIES", {}).keys() if
                                         cat in res_text and cat != "Clean"]

                    # Функция для сортировки найденных категорий по их индексу в PRIORITY
                    def get_prio(cat_name):
                        return prio_list.index(cat_name) if cat_name in prio_list else 999

                    found_cats_in_res.sort(key=get_prio)

                    if found_cats_in_res:
                        top_priority_cat = found_cats_in_res[0]
                        it.setBackground(QColor(prof_settings["CATEGORIES"][top_priority_cat].get("color", "#4c4c4c")))
                        color_applied = True

                if not color_applied:
                    if "[Redirect]" in res_text:
                        it.setBackground(self.redirect_color)
                    elif "Clean" in res_text:
                        it.setBackground(
                            QColor(prof_settings.get("CATEGORIES", {}).get("Clean", {}).get("color", "#004400")))
            self.table.setItem(i, ci, it)
        self.completed_count += 1
        self.pbar.setValue(self.completed_count)

    def on_fin(self, completed_naturally):
        self.btn_go.setEnabled(True)
        self.btn_exp.setEnabled(True)
        self.btn_stop.setEnabled(True)
        if completed_naturally:
            self.btn_stop.setText("DONE")
            self.btn_stop.setEnabled(False)
        else:
            self.btn_stop.setText("RESUME")

    def export(self):
        p, _ = QFileDialog.getSaveFileName(self, "Save", "report.xlsx", "Excel (*.xlsx);;CSV (*.csv)")
        if p:
            rows = []
            for r in range(self.table.rowCount()):
                row_data = []
                for c in range(8):
                    item = self.table.item(r, c)
                    row_data.append(item.text() if item else "")
                rows.append(row_data)

            df = pd.DataFrame(rows, columns=["URL", "Result", "Words", "Snip", "Checked", "TF", "CF", "TR"])

            if p.endswith('.xlsx'):
                try:
                    df.to_excel(p, index=False)
                except ImportError:
                    QMessageBox.warning(self, "Ошибка",
                                        "Для сохранения в формате Excel необходимо установить библиотеку openpyxl.\n\nВведите в терминале:\npip install openpyxl")
            else:
                df.to_csv(p, index=False, sep=';', encoding='utf-8-sig')

    def update_snip(self):
        sel = self.table.selectedItems()
        if sel: self.snip_view.setText(self.table.item(sel[0].row(), 3).text().replace(" | ", "\n\n"))

    def stop_and_disconnect_worker(self):
        if hasattr(self, 'worker') and self.worker is not None:
            self.worker.stop()
            try:
                self.worker.progress.disconnect()
            except TypeError:
                pass
            try:
                self.worker.finished.disconnect()
            except TypeError:
                pass
            self.worker = None

    def load_file(self):
        self.stop_and_disconnect_worker()
        p, _ = QFileDialog.getOpenFileName(self, "Open", "", "*.txt *.xlsx")
        if p:
            self.loaded_urls = [l.strip() for l in open(p, 'r', encoding='utf-8')] if p.endswith(
                '.txt') else pd.read_excel(p).iloc[:, 0].astype(str).tolist()
            self.lbl_c.setText(f"Доменов: {len(self.loaded_urls)}")
            self.table.setRowCount(len(self.loaded_urls))
            self.pbar.setValue(0)
            self.btn_go.setEnabled(True)
            self.btn_stop.setText("STOP")
            self.btn_stop.setEnabled(False)

            for i, u in enumerate(self.loaded_urls):
                self.table.setItem(i, 0, QTableWidgetItem(u))
                for c in range(1, 8):
                    self.table.setItem(i, c, QTableWidgetItem("-"))

    def load_majestic_clipboard(self):
        clipboard_text = QApplication.clipboard().text()
        if not clipboard_text:
            self.lbl_c.setText("Ошибка: Буфер обмена пуст")
            return

        lines = clipboard_text.strip().split('\n')
        if not lines:
            return

        parsed_data = []

        url_pattern = re.compile(r'https?://[^\s\t]+')

        for line in lines:
            if "Source URL" in line or line.startswith('#'):
                continue

            found_urls = url_pattern.findall(line)
            if not found_urls:
                continue

            target_url = found_urls[0].strip()

            tf, cf = 0, 0

            if '\t' in line:
                cols = [c.strip() for c in line.split('\t')]
            else:
                cols = [c.strip() for c in re.split(r'\s{2,}', line)]

            url_idx = -1
            for i, c in enumerate(cols):
                if target_url in c:
                    url_idx = i
                    break

            if url_idx != -1:
                try:
                    if url_idx + 4 < len(cols):
                        tf_str = cols[url_idx + 3]
                        cf_str = cols[url_idx + 4]

                        if tf_str.isdigit() and cf_str.isdigit():
                            tf = int(tf_str)
                            cf = int(cf_str)
                        else:
                            raise ValueError
                    else:
                        raise ValueError
                except ValueError:
                    numbers_after_url = []
                    for c in cols[url_idx + 1:]:
                        clean_c = c.replace(',', '').strip()
                        if clean_c.isdigit():
                            numbers_after_url.append(int(clean_c))

                    if len(numbers_after_url) >= 2:
                        tf = numbers_after_url[0]
                        cf = numbers_after_url[1]
                    elif len(numbers_after_url) == 1:
                        tf = numbers_after_url[0]

            tr = round(tf / cf, 2) if cf > 0 else 0.0

            parsed_data.append({
                'url': target_url,
                'tf': str(tf),
                'cf': str(cf),
                'tr': str(tr)
            })

        if parsed_data:
            self.stop_and_disconnect_worker()
            self.loaded_urls = [d['url'] for d in parsed_data]
            self.lbl_c.setText(f"Доменов: {len(self.loaded_urls)}")
            self.table.setRowCount(len(self.loaded_urls))
            self.pbar.setValue(0)
            self.btn_go.setEnabled(True)
            self.btn_stop.setText("STOP")
            self.btn_stop.setEnabled(False)

            for i, d in enumerate(parsed_data):
                self.table.setItem(i, 0, QTableWidgetItem(d['url']))
                for c in range(1, 5):
                    self.table.setItem(i, c, QTableWidgetItem("-"))
                self.table.setItem(i, 5, QTableWidgetItem(d['tf']))
                self.table.setItem(i, 6, QTableWidgetItem(d['cf']))
                self.table.setItem(i, 7, QTableWidgetItem(d['tr']))
        else:
            self.lbl_c.setText("Доменов: 0 (Данные не распознаны)")

    def clear_list(self):
        self.stop_and_disconnect_worker()
        self.table.setRowCount(0)
        self.loaded_urls = []
        self.lbl_c.setText("Доменов: 0")
        self.pbar.setValue(0)
        self.btn_stop.setText("STOP")
        self.btn_stop.setEnabled(False)
        self.btn_go.setEnabled(True)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = LinkDetectiveGUI()
    w.show()
    sys.exit(app.exec())