import sys
import os
import re
import requests
import json
import pandas as pd
import urllib.parse
from urllib.parse import urlparse, urljoin, urlencode
from bs4 import BeautifulSoup
import webbrowser

from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QPushButton, QTableWidget, QTableWidgetItem,
                             QFileDialog, QLabel, QProgressBar, QMenu,
                             QTextEdit, QSplitter, QComboBox, QInputDialog, QDialog,
                             QLineEdit, QScrollArea, QCheckBox, QSpinBox)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QRunnable, QThreadPool, QObject
from PyQt6.QtGui import QColor

# --- 1. КОНФИГ ---
CONFIG_FILE = "config.json"


def wrap(words): return [{"word": w, "active": True} for w in words]


DEFAULT_CONFIG = {
    "Default": {
        "THREADS": 20,
        "STOP_WORDS": {
            'Gambling': wrap(
                ['казино', 'игровые автоматы', 'casino', 'vulkan', 'фонбет', '1xbet', 'freebet', '1vin', 'winline']),
            'Adult': wrap(
                ['порно', 'секс', 'вебкам', 'porn', 'sex', 'xxx', 'bdsm', 'проститутки', 'webcam', 'stripchat']),
            'Pharma': wrap(['наркотики', 'виагра', 'бад', 'drugs', 'pills', 'viagra', 'cialis', 'сиалис', 'бады']),
            'Scraper/Aggregator': wrap(
                ['website worth', 'domain value', 'estimated worth', 'site cost', 'websiteworth',
                 'most visited web pages', 'world\'s most']),
            'Marketing Trash': wrap(['make money online', 'passive income', 'list building', 'earn money']),
            'SEO/PBN': wrap(
                ['guest post', 'write for us', 'submit article', 'sponsored post', 'link directory', 'seo services'])
        },
        "CONTEXT_WORDS": {
            '777': {'bad': wrap(['слот', 'jackpot', 'вулкан', 'играть', 'выигрыш']), 'good': ['boeing', 'боинг']},
            'ставки': {'bad': wrap(['спорт', 'прогноз', 'букмекер']), 'good': ['налог', 'ипотека', 'ремонт']},
            'слот': {'bad': wrap(['играть', 'джекпот']), 'good': ['память', 'плата']},
            'рулетка': {'bad': wrap(['зеро', 'ставка']), 'good': ['измерительная', 'строительная']},
            'скачать': {'bad': wrap(['бесплатно', 'торрент', 'mp3', 'фильм', 'смотреть онлайн']),
                        'good': ['прайс', 'инструкция']},
            'знакомства': {'bad': wrap(['интим', 'секс']), 'good': ['объявления', 'городские']},
            'seo': {'bad': wrap(['submit', 'directory', 'rank', 'post']), 'good': ['optimization']}
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
    padding: 5px 15px;
}
QPushButton:hover {
    background-color: #4c4c4c;
}
QPushButton:disabled {
    background-color: #2b2b2b;
    color: #777;
}
QLineEdit, QComboBox, QSpinBox, QTextEdit {
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

# Расширенные маркеры пустых поисков и форумных исключений
EMPTY_MARKERS = [
    'ничего не найдено', 'not found', 'извините', 'no results', '0 результатов',
    '0 results', 'результатов нет', 'исключены из поиска', 'слишком часто используются',
    'либо слишком длинные, либо слишком короткие', 'слишком длинные', 'слишком короткие',
    'too common', 'ignored', 'были проигнорированы', 'слишком употребимыми', 'найдено записей: 0'
]


def load_settings():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return DEFAULT_CONFIG
    return DEFAULT_CONFIG


def save_settings(config):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


# --- 2. ЛОГИКА АНАЛИЗА ---
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


def scan_logic(html_content, url, stop_words, context_words, is_search=False, search_term=None):
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

    is_search_url = any(x in url.lower() for x in ['search', 'query', 'poisk', 's=']) or is_search
    search_empty = is_search_url and (
            not text_full.strip() or
            any(m in text_full for m in EMPTY_MARKERS) or
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

    for cat, items in stop_words.items():
        if trust_score >= 5 and cat == 'Scraper/Aggregator': continue
        for item in items:
            if not item.get("active"): continue
            w = item["word"].lower()
            if re.search(rf"\b{re.escape(w)}\b", text_full, re.UNICODE):
                if w in context_words:
                    ctx = context_words[w]
                    alibi_found = any(re.search(rf"\b{re.escape(good.lower())}\b", text_full, re.UNICODE)
                                      for good in ctx.get('good', []))
                    if alibi_found:
                        continue
                found_cats.add(cat)
                found_words.add(w)
                idx = text_full.find(w)
                snippets.append(f"[{w}]: ...{text_full[max(0, idx - 60):idx + 60]}...")

    for trigger_word, ctx in context_words.items():
        w = trigger_word.lower()
        if w in found_words:
            continue
        if re.search(rf"\b{re.escape(w)}\b", text_full, re.UNICODE):
            alibi_found = any(re.search(rf"\b{re.escape(good.lower())}\b", text_full, re.UNICODE)
                              for good in ctx.get('good', []))
            if alibi_found:
                continue
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
                found_cats.add("Context Warning")
                found_words.add(w)
                idx = text_full.find(w)
                snippets.append(f"[{w} + {bad_found}]: ...{text_full[max(0, idx - 60):idx + 60]}...")
            else:
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
    def __init__(self, index, url, stop_words, context_words, search_queries):
        super().__init__()
        self.index = index
        self.url = url
        self.stop_words = stop_words
        self.context_words = context_words
        self.search_queries = search_queries
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
            r = requests.get(f_url, headers=self.headers, timeout=12, allow_redirects=True)
            final_url = r.url
            soup = BeautifulSoup(r.content, 'html.parser')
            is_redirected = urlparse(f_url).netloc.lower() != urlparse(final_url).netloc.lower()

            c, w, s = scan_logic(r.content, final_url, self.stop_words, self.context_words)

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
                        sr = requests.get(target_link, headers=self.headers, timeout=8)
                        if sr.status_code == 200:
                            sc, sw, ss = scan_logic(sr.content, sr.url, self.stop_words, self.context_words, True,
                                                    target_link)
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

    def __init__(self, tasks, stop_words, context_words, max_threads=20):
        super().__init__()
        self.tasks = tasks
        self.stop_words = stop_words
        self.context_words = context_words
        self.is_running = True
        self.pool = QThreadPool()
        self.pool.setMaxThreadCount(max_threads)

    def stop(self):
        self.is_running = False
        self.pool.clear()

    def run(self):
        search_queries = []
        for cat in ['Gambling', 'Adult']:
            if cat in self.stop_words:
                search_queries.extend([w['word'] for w in self.stop_words[cat] if w.get('active')])
        search_queries = list(dict.fromkeys(search_queries))[:10]
        for idx, url in self.tasks:
            if not self.is_running: break
            task = CheckTask(idx, url, self.stop_words, self.context_words, search_queries)
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
        self.resize(550, 600)
        layout = QVBoxLayout(self)

        # --- ДОБАВЛЕНА НАСТРОЙКА ПОТОКОВ ---
        thread_lay = QHBoxLayout()
        thread_lay.addWidget(QLabel("Количество потоков:"))
        self.thread_spin = QSpinBox()
        self.thread_spin.setRange(1, 100)
        self.thread_spin.setValue(self.settings[self.profile].get("THREADS", 20))
        self.thread_spin.valueChanged.connect(self.update_threads)
        thread_lay.addWidget(self.thread_spin)
        thread_lay.addStretch()
        layout.addLayout(thread_lay)
        # -----------------------------------

        self.mode_sel = QComboBox()
        self.mode_sel.addItems(["STOP_WORDS", "CONTEXT_WORDS"])
        self.mode_sel.currentTextChanged.connect(self.refresh_cats)
        layout.addWidget(QLabel("Тип словаря:"))
        layout.addWidget(self.mode_sel)
        cat_row = QHBoxLayout()
        self.cat_sel = QComboBox()
        self.cat_sel.currentTextChanged.connect(self.refresh_list)
        btn_add = QPushButton("+")
        btn_add.setFixedWidth(30)
        btn_add.clicked.connect(self.add_cat)
        cat_row.addWidget(self.cat_sel)
        cat_row.addWidget(btn_add)
        layout.addWidget(QLabel("Категория:"))
        layout.addLayout(cat_row)
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.cont = QWidget()
        self.list_lay = QVBoxLayout(self.cont)
        self.list_lay.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.scroll.setWidget(self.cont)
        layout.addWidget(self.scroll)
        add_box = QHBoxLayout()
        self.new_in = QLineEdit()
        btn_w = QPushButton("Добавить")
        btn_w.clicked.connect(self.add_word)
        add_box.addWidget(self.new_in)
        add_box.addWidget(btn_w)
        layout.addLayout(add_box)
        self.refresh_cats()

    def update_threads(self, val):
        self.settings[self.profile]["THREADS"] = val
        save_settings(self.settings)

    def refresh_cats(self):
        m = self.mode_sel.currentText()
        self.cat_sel.clear()
        if m in self.settings[self.profile]: self.cat_sel.addItems(self.settings[self.profile][m].keys())

    def add_cat(self):
        m, (n, ok) = self.mode_sel.currentText(), QInputDialog.getText(self, "Категория", "Имя:")
        if ok and n:
            self.settings[self.profile][m][n] = [] if m == "STOP_WORDS" else {"bad": [], "good": []}
            save_settings(self.settings)
            self.refresh_cats()

    def refresh_list(self):
        while self.list_lay.count():
            w = self.list_lay.takeAt(0).widget()
            if w: w.deleteLater()
        m, c = self.mode_sel.currentText(), self.cat_sel.currentText()
        if not c: return
        data = self.settings[self.profile][m][c]
        for item in (data if isinstance(data, list) else data['bad']):
            row = QWidget()
            h = QHBoxLayout(row)
            cb = QCheckBox(item["word"])
            cb.setChecked(item.get("active", True))
            cb.toggled.connect(lambda s, i=item: self.upd(i, s))
            btn = QPushButton("×")
            btn.setFixedSize(20, 20)
            btn.clicked.connect(lambda s, i=item: self.rem(i))
            h.addWidget(cb)
            h.addStretch()
            h.addWidget(btn)
            self.list_lay.addWidget(row)

    def upd(self, i, s):
        i["active"] = s
        save_settings(self.settings)

    def rem(self, i):
        m, c = self.mode_sel.currentText(), self.cat_sel.currentText()
        lst = self.settings[self.profile][m][c]
        (lst if isinstance(lst, list) else lst['bad']).remove(i)
        save_settings(self.settings)
        self.refresh_list()

    def add_word(self):
        t = self.new_in.text().strip()
        if t:
            m, c = self.mode_sel.currentText(), self.cat_sel.currentText()
            lst = self.settings[self.profile][m][c]
            (lst if isinstance(lst, list) else lst['bad']).append({"word": t, "active": True})
            save_settings(self.settings)
            self.new_in.clear()
            self.refresh_list()


class LinkDetectiveGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.settings, self.current_profile = load_settings(), "Default"
        self.is_dark_theme = False  # Флаг состояния темы
        self.setWindowTitle("LINK DETECTIVE V1.4")
        self.resize(1350, 850)
        self.loaded_urls = []
        self.completed_count = 0
        self.colors = {
            "Adult": QColor("#8B0000"), "Gambling": QColor("#CC7A00"), "SEO/PBN": QColor("#4B0082"),
            "Pharma": QColor("#E65100"), "Marketing Trash": QColor("#FBC02D"), "Clean": QColor("#004400"),
            "Chinese Content": QColor("#FF69B4"), "Arabic Content": QColor("#FF69B4"),
            "Scraper/Aggregator": QColor("#6A1B9A")
        }
        self.redirect_color = QColor("#0D47A1")

        self.color_priority = ["Scraper/Aggregator", "Adult", "Gambling", "Pharma", "SEO/PBN", "Marketing Trash",
                               "Chinese Content", "Arabic Content"]

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
        r1.addWidget(QPushButton("⚙", clicked=self.open_editor))
        r1.addStretch()

        # --- КНОПКА ПЕРЕКЛЮЧЕНИЯ ТЕМЫ ---
        self.btn_theme = QPushButton("🌙 Темная тема", clicked=self.toggle_theme)
        r1.addWidget(self.btn_theme)
        # ---------------------------------

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

        # Забираем количество потоков из настроек (по умолчанию 20, если их там нет)
        thread_count = p.get("THREADS", 20)

        self.worker = AnalysisWorker(tasks_to_run, p["STOP_WORDS"], p["CONTEXT_WORDS"], thread_count)
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
                for cat in self.color_priority:
                    if cat in res_text:
                        it.setBackground(self.colors[cat])
                        color_applied = True
                        break

                if not color_applied:
                    if "[Redirect]" in res_text:
                        it.setBackground(self.redirect_color)
                    elif "Clean" in res_text:
                        it.setBackground(self.colors["Clean"])
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
        p, _ = QFileDialog.getSaveFileName(self, "Save", "report.csv", "CSV (*.csv)")
        if p:
            rows = []
            for r in range(self.table.rowCount()):
                row_data = []
                for c in range(8):
                    item = self.table.item(r, c)
                    row_data.append(item.text() if item else "")
                rows.append(row_data)

            pd.DataFrame(rows, columns=["URL", "Result", "Words", "Snip", "Checked", "TF", "CF", "TR"]).to_csv(
                p, index=False, sep=';', encoding='utf-8-sig')

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

    # --- УЛУЧШЕННЫЙ ПАРСЕР БУФЕРА MAJESTIC ---
    def load_majestic_clipboard(self):
        clipboard_text = QApplication.clipboard().text()
        if not clipboard_text:
            self.lbl_c.setText("Ошибка: Буфер обмена пуст")
            return

        lines = clipboard_text.strip().split('\n')
        if not lines:
            return

        parsed_data = []

        # Регулярное выражение для поиска URL
        url_pattern = re.compile(r'https?://[^\s\t]+')

        for line in lines:
            # Пропускаем строку заголовков и технические строки
            if "Source URL" in line or line.startswith('#'):
                continue

            # 1. Извлекаем все URL из строки
            found_urls = url_pattern.findall(line)
            if not found_urls:
                continue

            # Берем первый найденный URL (обычно это Source URL)
            target_url = found_urls[0].strip()

            # 2. Правильно извлекаем TF и CF
            tf, cf = 0, 0

            # Разбиваем строку на колонки. Majestic использует табуляцию.
            # Если табуляции нет, используем двойные пробелы как разделитель.
            if '\t' in line:
                cols = [c.strip() for c in line.split('\t')]
            else:
                cols = [c.strip() for c in re.split(r'\s{2,}', line)]

            # Находим индекс колонки с нашим целевым URL
            url_idx = -1
            for i, c in enumerate(cols):
                if target_url in c:
                    url_idx = i
                    break

            if url_idx != -1:
                # В стандартной выгрузке Majestic колонки идут так:
                # [url_idx]   : Source URL
                # [url_idx+1] : Source Anchor Text
                # [url_idx+2] : Link Type
                # [url_idx+3] : Source Trust Flow
                # [url_idx+4] : Source Citation Flow

                try:
                    # Пробуем взять данные по стандартному смещению
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
                    # Резервный метод: если структура смещена, ищем первые цифровые колонки СТРОГО ПОСЛЕ URL
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

            # Считаем Trust Ratio
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