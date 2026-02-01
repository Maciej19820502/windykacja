import csv
import io
import json
import logging
import os
import re
import smtplib
import urllib.request
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from apscheduler.schedulers.background import BackgroundScheduler
from flask import (Flask, flash, jsonify, redirect, render_template, request,
                   send_file, url_for)

from models import Config, ImportHistory, Invoice, Kontrahent, Korespondencja, SzablonKomunikacji, db


# --- NBP exchange rates cache ---
_nbp_cache = {'rates': {}, 'date': None}


def get_nbp_rates():
    """Fetch current exchange rates from NBP API (Table A). Returns dict {code: mid_rate}."""
    today = date.today().isoformat()
    if _nbp_cache['date'] == today and _nbp_cache['rates']:
        return _nbp_cache['rates']

    rates = {'PLN': 1.0}
    try:
        url = 'https://api.nbp.pl/api/exchangerates/tables/A/?format=json'
        req = urllib.request.Request(url, headers={'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        for row in data[0]['rates']:
            rates[row['code']] = row['mid']
        _nbp_cache['rates'] = rates
        _nbp_cache['date'] = data[0]['effectiveDate']
    except Exception:
        # Fallback rates if NBP API is unavailable
        rates.update({'EUR': 4.30, 'USD': 4.00, 'GBP': 5.10, 'CHF': 4.50, 'CZK': 0.17})
        _nbp_cache['rates'] = rates
        _nbp_cache['date'] = 'offline'
    return rates


def kwota_pln(inv, rates):
    """Convert invoice amount to PLN using NBP rates."""
    if inv.waluta == 'PLN' or not inv.waluta:
        return inv.kwota
    rate = rates.get(inv.waluta, 1.0)
    return round(inv.kwota * rate, 2)


def parse_date(s):
    """Parse date string in multiple formats: YYYY-MM-DD, DD.MM.YYYY, DD-MM-YYYY, DD/MM/YYYY."""
    s = s.strip()
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d-%m-%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f'Nierozpoznany format daty: {s}')


def parse_ksef_xml(file_content):
    """Parse KSeF XML invoice (FA(2) or FA(3) schema). Returns dict with invoice data."""
    NAMESPACES = [
        'http://crd.gov.pl/wzor/2023/06/29/12648/',   # FA(2)
        'http://crd.gov.pl/wzor/2025/06/25/13775/',    # FA(3)
    ]

    root = ET.fromstring(file_content)
    tag = root.tag

    ns = None
    for candidate in NAMESPACES:
        if candidate in tag:
            ns = candidate
            break

    if ns is None:
        raise ValueError('Nieobsługiwany schemat XML — oczekiwano FA(2) lub FA(3)')

    def find(path):
        return root.find(path, {'ns': ns})

    def find_text(path):
        el = find(path)
        return el.text.strip() if el is not None and el.text else None

    nazwa = find_text('.//ns:Podmiot1/ns:DaneIdentyfikacyjne/ns:Nazwa')
    nip = find_text('.//ns:Podmiot1/ns:DaneIdentyfikacyjne/ns:NIP')
    nr_faktury = find_text('.//ns:Fa/ns:P_2')
    kwota_str = find_text('.//ns:Fa/ns:P_15')
    waluta = find_text('.//ns:Fa/ns:KodWaluty') or 'PLN'
    data_wystawienia_str = find_text('.//ns:Fa/ns:P_1')
    termin_str = find_text('.//ns:Fa/ns:Platnosc/ns:TerminyPlatnosci/ns:TerminPlatnosci')

    if not nr_faktury:
        raise ValueError('Brak numeru faktury (P_2) w pliku XML')
    if not kwota_str:
        raise ValueError('Brak kwoty brutto (P_15) w pliku XML')
    if not data_wystawienia_str:
        raise ValueError('Brak daty wystawienia (P_1) w pliku XML')

    kwota = float(kwota_str)
    data_wystawienia = parse_date(data_wystawienia_str)

    if termin_str:
        termin_platnosci = parse_date(termin_str)
    else:
        termin_platnosci = data_wystawienia + timedelta(days=14)

    return {
        'kontrahent': nazwa or '',
        'nip': nip or '',
        'nr_faktury': nr_faktury,
        'kwota': kwota,
        'waluta': waluta,
        'data_wystawienia': data_wystawienia,
        'termin_platnosci': termin_platnosci,
        'data_platnosci': None,
    }


def fetch_company_by_nip(nip):
    """Fetch company data from MF Biala Lista VAT API, fallback to GUS REGON.
    Returns dict with nazwa, adres, status_vat or None.
    """
    nip = nip.strip().replace('-', '')
    today = date.today().isoformat()
    url = f'https://wl-api.mf.gov.pl/api/search/nip/{nip}?date={today}'
    try:
        req = urllib.request.Request(url, headers={'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        subject = data.get('result', {}).get('subject')
        if subject:
            return {
                'nazwa': subject.get('name', ''),
                'adres': subject.get('workingAddress') or subject.get('residenceAddress') or '',
                'status_vat': subject.get('statusVat', ''),
            }
    except Exception:
        pass

    # Fallback: GUS REGON API
    gus_result = fetch_company_by_nip_gus(nip)
    if gus_result:
        return gus_result

    return None


def _gus_soap_request(url, action, body_xml, sid=None):
    """Send SOAP request to GUS REGON API. Returns parsed XML root or None."""
    envelope = f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
               xmlns:ns="http://CIS/BIR/PUBL/2014/07"
               xmlns:dat="http://CIS/BIR/PUBL/2014/07/DataContract">
  <soap:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
    <wsa:To>{url}</wsa:To>
    <wsa:Action>{action}</wsa:Action>
  </soap:Header>
  <soap:Body>{body_xml}</soap:Body>
</soap:Envelope>'''
    req = urllib.request.Request(url, data=envelope.encode('utf-8'), method='POST')
    req.add_header('Content-Type', 'application/soap+xml; charset=utf-8')
    if sid:
        req.add_header('sid', sid)
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read().decode('utf-8')
    match = re.search(r'(<s:Envelope.*?</s:Envelope>)', raw, re.DOTALL)
    return ET.fromstring(match.group(1)) if match else None


_gus_sid_cache = {'sid': None, 'url': None}

GUS_SANDBOX_URL = 'https://wyszukiwarkaregontest.stat.gov.pl/wsBIR/UslugaBIRzewnPubl.svc'
GUS_PROD_URL = 'https://wyszukiwarkaregon.stat.gov.pl/wsBIR/UslugaBIRzewnPubl.svc'
GUS_SANDBOX_KEY = 'abcde12345abcde12345'


def _gus_login(api_url, api_key):
    """Login to GUS API and cache SID."""
    action = 'http://CIS/BIR/PUBL/2014/07/IUslugaBIRzewnPubl/Zaloguj'
    body = f'<ns:Zaloguj><ns:pKluczUzytkownika>{api_key}</ns:pKluczUzytkownika></ns:Zaloguj>'
    root = _gus_soap_request(api_url, action, body)
    if root is None:
        return None
    ns = {'ns': 'http://CIS/BIR/PUBL/2014/07'}
    el = root.find('.//ns:ZalogujResult', ns)
    sid = el.text if el is not None and el.text else None
    if sid:
        _gus_sid_cache['sid'] = sid
        _gus_sid_cache['url'] = api_url
    return sid


def fetch_company_by_nip_gus(nip):
    """Fetch company data from GUS REGON API (BIR1) by NIP.
    Uses production key from Config if available, otherwise sandbox.
    Returns dict with nazwa, adres, status_vat or None.
    """
    nip = nip.strip().replace('-', '')
    gus_key = Config.get('gus_api_key', '').strip()
    if gus_key:
        api_url = GUS_PROD_URL
        api_key = gus_key
    else:
        api_url = GUS_SANDBOX_URL
        api_key = GUS_SANDBOX_KEY

    try:
        # Reuse cached SID or login
        sid = _gus_sid_cache.get('sid') if _gus_sid_cache.get('url') == api_url else None
        if not sid:
            sid = _gus_login(api_url, api_key)
        if not sid:
            return None

        action = 'http://CIS/BIR/PUBL/2014/07/IUslugaBIRzewnPubl/DaneSzukajPodmioty'
        body = f'''<ns:DaneSzukajPodmioty>
          <ns:pParametryWyszukiwania><dat:Nip>{nip}</dat:Nip></ns:pParametryWyszukiwania>
        </ns:DaneSzukajPodmioty>'''
        root = _gus_soap_request(api_url, action, body, sid=sid)
        if root is None:
            return None

        ns = {'ns': 'http://CIS/BIR/PUBL/2014/07'}
        result_el = root.find('.//ns:DaneSzukajPodmiotyResult', ns)
        if result_el is None or not result_el.text:
            # Session expired — retry once with fresh login
            sid = _gus_login(api_url, api_key)
            if not sid:
                return None
            root = _gus_soap_request(api_url, action, body, sid=sid)
            if root is None:
                return None
            result_el = root.find('.//ns:DaneSzukajPodmiotyResult', ns)
            if result_el is None or not result_el.text:
                return None

        inner = ET.fromstring(result_el.text)
        dane = inner.find('.//dane')
        if dane is None:
            return None

        nazwa = dane.findtext('Nazwa') or ''
        miejscowosc = dane.findtext('Miejscowosc') or ''
        kod = dane.findtext('KodPocztowy') or ''
        ulica = dane.findtext('Ulica') or ''
        nr = dane.findtext('NrNieruchomosci') or ''
        lokal = dane.findtext('NrLokalu') or ''

        adres_parts = []
        if ulica:
            addr = ulica
            if nr:
                addr += f' {nr}'
            if lokal:
                addr += f'/{lokal}'
            adres_parts.append(addr)
        if kod or miejscowosc:
            adres_parts.append(f'{kod} {miejscowosc}'.strip())
        adres = ', '.join(adres_parts)

        if not nazwa:
            return None

        return {
            'nazwa': nazwa,
            'adres': adres,
            'status_vat': '',
        }
    except Exception:
        return None


def get_or_create_kontrahent(nip):
    """Find existing Kontrahent by NIP or create new one using MF API data."""
    nip = nip.strip().replace('-', '')
    if not nip:
        return None

    kontrahent = Kontrahent.query.filter_by(nip=nip).first()
    if kontrahent:
        return kontrahent

    # Fetch from API
    company_data = fetch_company_by_nip(nip)
    kontrahent = Kontrahent(
        nip=nip,
        nazwa=company_data['nazwa'] if company_data else None,
        adres=company_data['adres'] if company_data else None,
        status_vat=company_data['status_vat'] if company_data else None,
        data_sprawdzenia=datetime.now(timezone.utc) if company_data else None,
    )
    db.session.add(kontrahent)
    db.session.flush()
    return kontrahent


app = Flask(__name__)
app.config['SECRET_KEY'] = 'windykacja-secret-key-2025'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///windykacja.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

ETAPY_INFO = {
    1: {
        'nazwa': 'Przypomnienie o zbliżającym się terminie płatności',
        'opis': 'Komunikacja wysyłana przed terminem płatności lub w określonym dniu przeterminowania. Etap prewencyjny.',
        'ikona': 'bi-bell',
        'kolor': 'info',
    },
    2: {
        'nazwa': 'Przypomnienie o płatności',
        'opis': 'Komunikacja wstępna, polubowna. O przejściu do etapu decyduje najstarszy rozrachunek.',
        'ikona': 'bi-envelope-paper',
        'kolor': 'primary',
    },
    3: {
        'nazwa': 'Monit płatności',
        'opis': 'Komunikacja konkretna i rzeczowa, przypominająca o zaległości. Dotyczy najstarszego rozrachunku z tabelą zbiorczą.',
        'ikona': 'bi-exclamation-circle',
        'kolor': 'warning',
    },
    4: {
        'nazwa': 'Wezwanie do zapłaty',
        'opis': 'Komunikacja stanowcza i zdecydowana. Dotyczy najstarszego rozrachunku z tabelą zbiorczą zobowiązań.',
        'ikona': 'bi-exclamation-triangle',
        'kolor': 'danger',
    },
    5: {
        'nazwa': 'Wezwanie ostateczne (przedsądowe)',
        'opis': 'Komunikacja stanowcza, zdecydowana, w formalnym biznesowym tonie. Ostatni etap przed działaniami prawnymi.',
        'ikona': 'bi-shield-exclamation',
        'kolor': 'dark',
    },
}

DOMYSLNE_SZABLONY = {
    # --- ETAP 1 : Przypomnienie przed terminem (per faktura) ---
    (1, 'LEKKA', 'email'): {
        'tytul': 'Informacja o zbliżającym się terminie płatności faktury {nr_faktury}',
        'tresc': 'Szanowni Państwo,\n\nuprzejmie informujemy, że zbliża się termin płatności faktury nr {nr_faktury} na kwotę {kwota} {waluta}, wystawionej dnia {data_wystawienia}, z terminem płatności {termin_platnosci}.\n\nProsimy o terminowe uregulowanie należności.\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}',
    },
    (1, 'LEKKA', 'sms'): {
        'tytul': '',
        'tresc': 'Przypominamy o zbliżającym się terminie płatności faktury {nr_faktury} na kwotę {kwota} {waluta} (termin: {termin_platnosci}). {firma_nazwa}',
    },
    (1, 'STANDARDOWA', 'email'): {
        'tytul': 'Przypomnienie o terminie płatności faktury {nr_faktury}',
        'tresc': 'Szanowni Państwo,\n\nprzypominamy, że termin płatności faktury nr {nr_faktury} na kwotę {kwota} {waluta} upływa dnia {termin_platnosci}.\n\nFaktura została wystawiona dnia {data_wystawienia}. Prosimy o terminowe uregulowanie płatności na wskazany rachunek bankowy.\n\nW przypadku dokonania wpłaty prosimy o potraktowanie niniejszej wiadomości jako bezprzedmiotowej.\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}',
    },
    (1, 'STANDARDOWA', 'sms'): {
        'tytul': '',
        'tresc': 'Przypominamy: termin płatności faktury {nr_faktury} ({kwota} {waluta}) upływa {termin_platnosci}. Prosimy o terminową wpłatę. {firma_nazwa}',
    },
    (1, 'OSTRA', 'email'): {
        'tytul': 'PILNE: Termin płatności faktury {nr_faktury}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym przypominamy o konieczności uregulowania faktury nr {nr_faktury} na kwotę {kwota} {waluta}. Termin płatności upływa dnia {termin_platnosci}.\n\nBrak terminowej wpłaty może skutkować naliczeniem odsetek ustawowych oraz podjęciem dalszych działań windykacyjnych.\n\nProsimy o niezwłoczne uregulowanie należności.\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}',
    },
    (1, 'OSTRA', 'sms'): {
        'tytul': '',
        'tresc': 'PILNE: Termin płatności FV {nr_faktury} ({kwota} {waluta}) upływa {termin_platnosci}. Brak wpłaty = odsetki i windykacja. {firma_nazwa}',
    },
    (1, 'BRAK', 'email'): {'tytul': '', 'tresc': ''},
    (1, 'BRAK', 'sms'): {'tytul': '', 'tresc': ''},

    # --- ETAP 2 : Przypomnienie o płatności (polubowne) ---
    (2, 'LEKKA', 'email'): {
        'tytul': 'Uprzejme przypomnienie o płatności – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nzwracamy się z uprzejmą prośbą o uregulowanie zaległej płatności wynikającej z faktury nr {nr_faktury} na kwotę {kwota} {waluta}, której termin płatności upłynął dnia {termin_platnosci}.\n\nJeśli płatność została już dokonana, prosimy o zignorowanie niniejszej wiadomości.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (2, 'LEKKA', 'sms'): {
        'tytul': '',
        'tresc': 'Przypominamy o zaległej FV {nr_faktury} ({kwota} {waluta}, termin: {termin_platnosci}). Łączne zobowiązania: {suma_zobowiazan}, w tym przeterminowane: {suma_przeterminowanych}. {firma_nazwa}',
    },
    (2, 'STANDARDOWA', 'email'): {
        'tytul': 'Przypomnienie o nieuregulowanej płatności – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\ninformujemy, że do dnia dzisiejszego nie odnotowaliśmy wpłaty z tytułu faktury nr {nr_faktury} na kwotę {kwota} {waluta}. Termin płatności upłynął dnia {termin_platnosci}.\n\nUprzejmie prosimy o pilne uregulowanie zaległości.\n\n{tabela_zobowiazan}\n\nW razie pytań prosimy o kontakt.\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (2, 'STANDARDOWA', 'sms'): {
        'tytul': '',
        'tresc': 'Zaległa FV {nr_faktury} ({kwota} {waluta}, po terminie od {termin_platnosci}). Łączne zobowiązania: {suma_zobowiazan}, w tym przeterminowane: {suma_przeterminowanych}. Prosimy o pilną wpłatę. {firma_nazwa}',
    },
    (2, 'OSTRA', 'email'): {
        'tytul': 'ZALEGŁOŚĆ PŁATNICZA – wezwanie do uregulowania – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nstwierdzamy brak wpłaty z tytułu faktury nr {nr_faktury} na kwotę {kwota} {waluta}, której termin płatności upłynął dnia {termin_platnosci}.\n\nWzywamy do niezwłocznego uregulowania zaległości. Dalsze opóźnienie skutkować będzie naliczeniem odsetek ustawowych za opóźnienie w transakcjach handlowych.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (2, 'OSTRA', 'sms'): {
        'tytul': '',
        'tresc': 'ZALEGŁOŚĆ: FV {nr_faktury} ({kwota} {waluta}) po terminie od {termin_platnosci}. Zobowiązania: {suma_zobowiazan}, przeterminowane: {suma_przeterminowanych}. Wymagana natychmiastowa wpłata. {firma_nazwa}',
    },
    (2, 'BRAK', 'email'): {'tytul': '', 'tresc': ''},
    (2, 'BRAK', 'sms'): {'tytul': '', 'tresc': ''},

    # --- ETAP 3 : Monit płatności ---
    (3, 'LEKKA', 'email'): {
        'tytul': 'Monit – zaległość z tytułu faktury {nr_faktury}',
        'tresc': 'Szanowni Państwo,\n\npomimo wcześniejszych przypomnień nie odnotowaliśmy wpłaty z tytułu faktury nr {nr_faktury} na kwotę {kwota} {waluta}. Termin płatności upłynął dnia {termin_platnosci}.\n\nProsimy o uregulowanie zaległości w najkrótszym możliwym terminie.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (3, 'LEKKA', 'sms'): {
        'tytul': '',
        'tresc': 'Monit: FV {nr_faktury} ({kwota} {waluta}) nieopłacona od {termin_platnosci}. Zobowiązania: {suma_zobowiazan}, przeterminowane: {suma_przeterminowanych}. Prosimy o kontakt. {firma_nazwa}',
    },
    (3, 'STANDARDOWA', 'email'): {
        'tytul': 'MONIT PŁATNOŚCI – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym wzywamy do uregulowania zaległej należności wynikającej z faktury nr {nr_faktury} na kwotę {kwota} {waluta}. Termin płatności upłynął dnia {termin_platnosci}.\n\nInformujemy, że od kwot przeterminowanych naliczane są odsetki ustawowe za opóźnienie w transakcjach handlowych.\n\nBrak wpłaty w ciągu 7 dni skutkować będzie podjęciem dalszych kroków windykacyjnych.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (3, 'STANDARDOWA', 'sms'): {
        'tytul': '',
        'tresc': 'MONIT: FV {nr_faktury} ({kwota} {waluta}) przeterminowana od {termin_platnosci}. Zobowiązania: {suma_zobowiazan}, przeterminowane: {suma_przeterminowanych}. Wpłata w 7 dni. {firma_nazwa}',
    },
    (3, 'OSTRA', 'email'): {
        'tytul': 'MONIT – PILNE WEZWANIE DO ZAPŁATY – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\npomimo wcześniejszych wezwań faktura nr {nr_faktury} na kwotę {kwota} {waluta} pozostaje nieuregulowana. Termin płatności upłynął dnia {termin_platnosci}.\n\nStanowczo wzywamy do natychmiastowego uregulowania całości zobowiązań. Brak wpłaty w ciągu 5 dni roboczych spowoduje przekazanie sprawy do dalszego postępowania windykacyjnego, co wiązać się będzie z dodatkowymi kosztami po Państwa stronie.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (3, 'OSTRA', 'sms'): {
        'tytul': '',
        'tresc': 'MONIT PILNY: FV {nr_faktury} ({kwota} {waluta}) przeterminowana. Zobowiązania: {suma_zobowiazan}, przeterminowane: {suma_przeterminowanych}. Brak wpłaty w 5 dni = windykacja. {firma_nazwa}',
    },
    (3, 'BRAK', 'email'): {'tytul': '', 'tresc': ''},
    (3, 'BRAK', 'sms'): {'tytul': '', 'tresc': ''},

    # --- ETAP 4 : Wezwanie do zapłaty ---
    (4, 'LEKKA', 'email'): {
        'tytul': 'Wezwanie do zapłaty – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nwobec braku uregulowania zaległości z tytułu faktury nr {nr_faktury} na kwotę {kwota} {waluta} (termin płatności: {termin_platnosci}), niniejszym wzywamy do zapłaty całości zobowiązań.\n\nProsimy o wpłatę w terminie 7 dni od daty otrzymania niniejszego wezwania.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (4, 'LEKKA', 'sms'): {
        'tytul': '',
        'tresc': 'Wezwanie do zapłaty: FV {nr_faktury} ({kwota} {waluta}). Zobowiązania: {suma_zobowiazan}, przeterminowane: {suma_przeterminowanych}. Termin: 7 dni. {firma_nazwa}',
    },
    (4, 'STANDARDOWA', 'email'): {
        'tytul': 'WEZWANIE DO ZAPŁATY – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym wzywamy do niezwłocznego uregulowania zaległych zobowiązań wynikających z faktury nr {nr_faktury} na kwotę {kwota} {waluta}. Termin płatności upłynął dnia {termin_platnosci}.\n\nŻądamy dokonania wpłaty w nieprzekraczalnym terminie 7 dni od otrzymania niniejszego wezwania. W przypadku braku wpłaty sprawa zostanie skierowana na drogę postępowania sądowego, co wiązać się będzie z obciążeniem Państwa kosztami postępowania, kosztami zastępstwa procesowego oraz odsetkami.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (4, 'STANDARDOWA', 'sms'): {
        'tytul': '',
        'tresc': 'WEZWANIE DO ZAPŁATY: FV {nr_faktury} ({kwota} {waluta}), termin minął {termin_platnosci}. Zobowiązania: {suma_zobowiazan}, przeterminowane: {suma_przeterminowanych}. Wpłata w 7 dni lub sąd. {firma_nazwa}',
    },
    (4, 'OSTRA', 'email'): {
        'tytul': 'WEZWANIE DO ZAPŁATY – OSTATECZNE OSTRZEŻENIE – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym kategorycznie wzywamy do natychmiastowego uregulowania zaległych zobowiązań z tytułu faktury nr {nr_faktury} na kwotę {kwota} {waluta} (termin płatności: {termin_platnosci}).\n\nInformujemy, że brak wpłaty pełnej kwoty zobowiązań w terminie 5 dni roboczych od daty niniejszego wezwania skutkować będzie:\n- naliczeniem odsetek ustawowych za opóźnienie w transakcjach handlowych,\n- obciążeniem kosztami windykacji (równowartość 40/70/100 EUR rekompensaty),\n- skierowaniem sprawy na drogę sądową.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (4, 'OSTRA', 'sms'): {
        'tytul': '',
        'tresc': 'OSTATNIE OSTRZEŻENIE: FV {nr_faktury} ({kwota} {waluta}). Zobowiązania: {suma_zobowiazan}, przeterminowane: {suma_przeterminowanych}. Wpłata w 5 dni lub sąd + koszty. {firma_nazwa}',
    },
    (4, 'BRAK', 'email'): {'tytul': '', 'tresc': ''},
    (4, 'BRAK', 'sms'): {'tytul': '', 'tresc': ''},

    # --- ETAP 5 : Wezwanie ostateczne (przedsądowe) ---
    (5, 'LEKKA', 'email'): {
        'tytul': 'Ostateczne przedsądowe wezwanie do zapłaty – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym kierujemy ostateczne przedsądowe wezwanie do zapłaty z tytułu faktury nr {nr_faktury} na kwotę {kwota} {waluta} (termin płatności: {termin_platnosci}).\n\nWzywamy do uregulowania pełnej kwoty zobowiązań w terminie 7 dni od daty doręczenia niniejszego wezwania. Brak wpłaty skutkować będzie skierowaniem sprawy na drogę postępowania sądowego.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (5, 'LEKKA', 'sms'): {
        'tytul': '',
        'tresc': 'Ostateczne wezwanie przedsądowe: FV {nr_faktury} ({kwota} {waluta}). Zobowiązania: {suma_zobowiazan}, przeterminowane: {suma_przeterminowanych}. Wpłata w 7 dni lub sąd. {firma_nazwa}',
    },
    (5, 'STANDARDOWA', 'email'): {
        'tytul': 'OSTATECZNE PRZEDSĄDOWE WEZWANIE DO ZAPŁATY – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym pismem kierujemy ostateczne przedsądowe wezwanie do zapłaty kwoty wynikającej z faktury nr {nr_faktury} na kwotę {kwota} {waluta}, z terminem płatności {termin_platnosci}.\n\nWzywamy do uregulowania całości zobowiązań w nieprzekraczalnym terminie 5 dni roboczych od daty doręczenia niniejszego wezwania.\n\nInformujemy, że niniejsze wezwanie stanowi ostateczną próbę polubownego rozwiązania sporu. Brak terminowej wpłaty skutkować będzie niezwłocznym skierowaniem sprawy na drogę postępowania sądowego, w wyniku czego zostaną Państwo obciążeni pełnymi kosztami postępowania sądowego, egzekucyjnego, kosztami zastępstwa procesowego oraz odsetkami ustawowymi za opóźnienie.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (5, 'STANDARDOWA', 'sms'): {
        'tytul': '',
        'tresc': 'OSTATECZNE WEZWANIE PRZEDSĄDOWE: FV {nr_faktury} ({kwota} {waluta}). Zobowiązania: {suma_zobowiazan}, przeterminowane: {suma_przeterminowanych}. Wpłata w 5 dni lub sąd. {firma_nazwa}',
    },
    (5, 'OSTRA', 'email'): {
        'tytul': 'OSTATECZNE PRZEDSĄDOWE WEZWANIE DO ZAPŁATY – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\ndziałając w imieniu {firma_nazwa}, NIP: {firma_nip}, z siedzibą: {firma_adres}, niniejszym kierujemy OSTATECZNE PRZEDSĄDOWE WEZWANIE DO ZAPŁATY.\n\nPomimo wielokrotnych wezwań faktura nr {nr_faktury} na kwotę {kwota} {waluta} (termin płatności: {termin_platnosci}) pozostaje nieuregulowana.\n\nKATEGORYCZNIE ŻĄDAMY uregulowania pełnej kwoty zobowiązań w terminie 3 dni roboczych od daty doręczenia niniejszego wezwania.\n\nW przypadku bezskutecznego upływu wyznaczonego terminu, bez odrębnego zawiadomienia:\n1. Sprawa zostanie skierowana na drogę postępowania sądowego.\n2. Zostanie złożony wniosek o wpis do rejestru dłużników BIG.\n3. Państwa firma zostanie obciążona pełnymi kosztami: sądowymi, egzekucyjnymi, zastępstwa procesowego, odsetkami ustawowymi za opóźnienie w transakcjach handlowych oraz rekompensatą za koszty odzyskiwania należności.\n\nNiniejsze wezwanie stanowi ostateczną próbę polubownego zakończenia sprawy.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (5, 'OSTRA', 'sms'): {
        'tytul': '',
        'tresc': 'OSTATECZNE WEZWANIE PRZEDSĄDOWE! Zaległości: {suma_przeterminowanych}. Łącznie: {suma_zobowiazan}. Wpłata w 3 dni lub sąd + BIG + pełne koszty. {firma_nazwa}',
    },
    (5, 'BRAK', 'email'): {'tytul': '', 'tresc': ''},
    (5, 'BRAK', 'sms'): {'tytul': '', 'tresc': ''},
}


def seed_szablony():
    """Seed default communication templates if table is empty."""
    if SzablonKomunikacji.query.first():
        return
    for (etap, wariant, kanal), dane in DOMYSLNE_SZABLONY.items():
        s = SzablonKomunikacji(
            etap=etap, wariant=wariant, kanal=kanal,
            tytul=dane['tytul'], tresc=dane['tresc']
        )
        db.session.add(s)
    db.session.commit()


DOMYSLNE_HARMONOGRAMY = {
    1: {'dzien_aktywacji': '-3', 'godzina': '09:00', 'dni_tygodnia': '1,2,3,4,5', 'aktywny': 'tak'},
    2: {'dzien_aktywacji': '1',  'godzina': '09:00', 'dni_tygodnia': '1,2,3,4,5', 'aktywny': 'tak'},
    3: {'dzien_aktywacji': '7',  'godzina': '10:00', 'dni_tygodnia': '1,3,5',     'aktywny': 'tak'},
    4: {'dzien_aktywacji': '14', 'godzina': '10:00', 'dni_tygodnia': '1,3',       'aktywny': 'tak'},
    5: {'dzien_aktywacji': '30', 'godzina': '11:00', 'dni_tygodnia': '2,4',       'aktywny': 'tak'},
}


def seed_harmonogramy():
    """Seed default schedule config per stage if not yet present."""
    for etap, defaults in DOMYSLNE_HARMONOGRAMY.items():
        for key, val in defaults.items():
            cfg_key = f'harmonogram_etap_{etap}_{key}'
            existing = Config.query.filter_by(klucz=cfg_key).first()
            if not existing:
                db.session.add(Config(klucz=cfg_key, wartosc=val))
    db.session.commit()


def get_harmonogram(etap):
    """Return schedule dict for given stage."""
    return {
        'dzien_aktywacji': Config.get(f'harmonogram_etap_{etap}_dzien_aktywacji', '0'),
        'godzina': Config.get(f'harmonogram_etap_{etap}_godzina', '09:00'),
        'dni_tygodnia': Config.get(f'harmonogram_etap_{etap}_dni_tygodnia', '1,2,3,4,5'),
        'aktywny': Config.get(f'harmonogram_etap_{etap}_aktywny', 'nie'),
    }


def _already_sent_for_invoice_stage(invoice_id, etap):
    """Check if correspondence was ever successfully sent for this invoice + stage."""
    if not invoice_id:
        return False
    return Korespondencja.query.filter(
        Korespondencja.invoice_id == invoice_id,
        Korespondencja.etap == etap,
        Korespondencja.status == 'wyslana',
    ).first() is not None


def run_stage_sending(etap, force=False, skip_duplicate_check=False):
    """Execute sending for a given stage. Returns (sent, errors) counts.
    If force=True, skip day-of-week and time checks (manual trigger).
    If skip_duplicate_check=True, ignore the per-invoice-per-stage duplicate guard.
    """
    today = date.today()
    harm = get_harmonogram(etap)
    dzien_aktywacji = int(harm['dzien_aktywacji'])

    # Determine the activation day range for this stage
    # Stage boundaries: this stage's dzien_aktywacji .. next stage's dzien_aktywacji - 1
    sorted_etapy = sorted(DOMYSLNE_HARMONOGRAMY.keys())

    sent = 0
    errors = 0

    if etap == 1:
        # Pre-due: find invoices where termin_platnosci - today == abs(dzien_aktywacji)
        # dzien_aktywacji is negative (e.g. -3 means 3 days before)
        target_days = dzien_aktywacji  # e.g. -3
        invoices = Invoice.query.filter(
            Invoice.status != 'oplacona',
            Invoice.kontrahent_id.isnot(None)
        ).all()
        for inv in invoices:
            inv.oblicz_status()
            diff = (inv.termin_platnosci - today).days  # positive = before due
            if diff != abs(target_days):
                continue
            kontrahent = inv.contractor
            if not kontrahent or kontrahent.sciezka_windykacji == 'BRAK':
                continue
            # Duplicate guard: this invoice + stage already sent ever
            if not skip_duplicate_check and _already_sent_for_invoice_stage(inv.id, etap):
                continue
            success, _ = send_correspondence(kontrahent.id, etap, invoice_override=inv)
            if success:
                sent += 1
            else:
                errors += 1
    else:
        # Stages 2-5: find contractors with oldest overdue invoice matching this stage range
        # Determine upper bound from next stage
        next_etap_dzien = None
        for e in sorted_etapy:
            if e > etap:
                next_harm = get_harmonogram(e)
                next_etap_dzien = int(next_harm['dzien_aktywacji'])
                break

        kontrahenci = Kontrahent.query.filter(
            Kontrahent.sciezka_windykacji != 'BRAK'
        ).all()
        for k in kontrahenci:
            overdue = k.invoices.filter(Invoice.status == 'przeterminowana').order_by(
                Invoice.termin_platnosci.asc()
            ).first()
            if not overdue:
                continue
            overdue.oblicz_status()
            dni = overdue.dni_roznica  # positive days overdue
            if dni < dzien_aktywacji:
                continue
            if next_etap_dzien is not None and dni >= next_etap_dzien:
                continue
            # Duplicate guard: this invoice + stage already sent ever
            if not skip_duplicate_check and _already_sent_for_invoice_stage(overdue.id, etap):
                continue
            success, _ = send_correspondence(k.id, etap)
            if success:
                sent += 1
            else:
                errors += 1

    return sent, errors


def scheduler_job():
    """Background job running every minute. Checks schedule and triggers sending."""
    with app.app_context():
        now = datetime.now()
        current_hhmm = now.strftime('%H:%M')
        # isoweekday: 1=Mon, 7=Sun
        current_dow = str(now.isoweekday())

        for etap in range(1, 6):
            harm = get_harmonogram(etap)
            if harm['aktywny'] != 'tak':
                continue
            dni_tygodnia = [d.strip() for d in harm['dni_tygodnia'].split(',') if d.strip()]
            if current_dow not in dni_tygodnia:
                continue
            if current_hhmm != harm['godzina']:
                continue
            # Conditions met — run sending for this stage
            app.logger.info(f'Scheduler: uruchamiam wysyłkę etapu {etap} ({current_hhmm}, dzień {current_dow})')
            s, e = run_stage_sending(etap)
            app.logger.info(f'Scheduler: etap {etap} — wysłano {s}, błędów {e}')


with app.app_context():
    db.create_all()
    seed_szablony()
    seed_harmonogramy()

# --- APScheduler setup ---
scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(scheduler_job, 'interval', minutes=1, id='windykacja_scheduler')
scheduler.start()


# ---------------------------------------------------------------------------
# Sending functions
# ---------------------------------------------------------------------------

def send_email(to, subject, body):
    """Send email via SMTP using Config settings. Returns (success, error_msg)."""
    host = Config.get('email_smtp_host')
    port = int(Config.get('email_smtp_port', '587'))
    user = Config.get('email_smtp_user')
    password = Config.get('email_smtp_pass')
    use_ssl = Config.get('email_smtp_ssl', 'tak') == 'tak'
    sender = Config.get('email_from') or user

    if not host or not user:
        return False, 'Brak konfiguracji SMTP (serwer/login).'

    msg = MIMEMultipart('alternative')
    msg['From'] = sender
    msg['To'] = to
    msg['Subject'] = subject

    # plain text version
    msg.attach(MIMEText(body, 'plain', 'utf-8'))
    # HTML version (convert newlines to <br>)
    html_body = body.replace('\n', '<br>\n')
    msg.attach(MIMEText(f'<html><body style="font-family:Arial,sans-serif;">{html_body}</body></html>', 'html', 'utf-8'))

    try:
        if use_ssl and port == 465:
            server = smtplib.SMTP_SSL(host, port, timeout=15)
        else:
            server = smtplib.SMTP(host, port, timeout=15)
            if use_ssl:
                server.starttls()
        server.login(user, password)
        server.sendmail(sender, [to], msg.as_string())
        server.quit()
        return True, None
    except Exception as e:
        return False, str(e)


def send_sms(to, message):
    """Send SMS via SMSAPI.pl REST API. Returns (success, error_msg)."""
    token = Config.get('smsapi_token')
    sms_from = Config.get('smsapi_from')

    if not token:
        return False, 'Brak tokena SMSAPI.'

    # Normalize phone number
    phone = to.replace(' ', '').replace('-', '')
    if not phone.startswith('+') and not phone.startswith('48'):
        phone = '48' + phone
    phone = phone.lstrip('+')

    params = {
        'to': phone,
        'message': message,
        'format': 'json',
        'encoding': 'utf-8',
    }
    if sms_from:
        params['from'] = sms_from

    data = '&'.join(f'{k}={urllib.request.quote(str(v))}' for k, v in params.items())
    url = 'https://api.smsapi.pl/sms.do'

    try:
        req = urllib.request.Request(url, data=data.encode('utf-8'), method='POST')
        req.add_header('Authorization', f'Bearer {token}')
        req.add_header('Content-Type', 'application/x-www-form-urlencoded')
        with urllib.request.urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read().decode('utf-8'))
        if 'error' in result:
            return False, f"SMSAPI error: {result.get('message', result['error'])}"
        return True, None
    except Exception as e:
        return False, str(e)


def determine_contractor_stage(kontrahent):
    """Determine dunning stage based on oldest overdue invoice.
    Returns (stage_number, oldest_overdue_invoice_or_None).
    brak przeterminowanych -> 1, <=7d -> 2, <=14d -> 3, <=30d -> 4, >30d -> 5
    """
    overdue_invoices = kontrahent.invoices.filter(
        Invoice.status == 'przeterminowana'
    ).order_by(Invoice.termin_platnosci.asc()).all()

    if not overdue_invoices:
        # No overdue — stage 1 (pre-due reminder), pick earliest unpaid
        earliest_unpaid = kontrahent.invoices.filter(
            Invoice.status != 'oplacona'
        ).order_by(Invoice.termin_platnosci.asc()).first()
        return 1, earliest_unpaid

    oldest = overdue_invoices[0]
    days = oldest.dni_roznica
    if days <= 7:
        return 2, oldest
    elif days <= 14:
        return 3, oldest
    elif days <= 30:
        return 4, oldest
    else:
        return 5, oldest


def calculate_suma_zobowiazan(kontrahent):
    """Sum of all unpaid invoices grouped by currency."""
    unpaid = kontrahent.invoices.filter(Invoice.status != 'oplacona').all()
    sums = {}
    for inv in unpaid:
        cur = inv.waluta or 'PLN'
        sums[cur] = sums.get(cur, 0) + inv.kwota
    return sums


def calculate_suma_przeterminowanych(kontrahent):
    """Sum of overdue invoices grouped by currency."""
    overdue = kontrahent.invoices.filter(Invoice.status == 'przeterminowana').all()
    sums = {}
    for inv in overdue:
        cur = inv.waluta or 'PLN'
        sums[cur] = sums.get(cur, 0) + inv.kwota
    return sums


def _format_currency_sums(sums):
    """Format currency sums dict to string like '5 000.00 PLN, 1 200.00 EUR'."""
    if not sums:
        return '0.00 PLN'
    parts = []
    for cur in sorted(sums.keys()):
        parts.append(f'{sums[cur]:,.2f} {cur}'.replace(',', ' '))
    return ', '.join(parts)


def build_tabela_zobowiazan_html(kontrahent):
    """Build HTML table of obligations for email stages 2-5."""
    overdue = kontrahent.invoices.filter(
        Invoice.status == 'przeterminowana'
    ).order_by(Invoice.termin_platnosci.asc()).all()
    all_unpaid = kontrahent.invoices.filter(
        Invoice.status != 'oplacona'
    ).order_by(Invoice.termin_platnosci.asc()).all()

    rows = all_unpaid if all_unpaid else overdue
    if not rows:
        return ''

    html = '<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse; font-size:13px;">'
    html += '<tr style="background:#f0f0f0;"><th>Nr faktury</th><th>Kwota</th><th>Waluta</th><th>Termin</th><th>Dni po terminie</th><th>Status</th></tr>'
    for inv in rows:
        color = '#fff0f0' if inv.status == 'przeterminowana' else '#ffffff'
        status_txt = 'Przeterminowana' if inv.status == 'przeterminowana' else 'W terminie'
        html += f'<tr style="background:{color};"><td>{inv.nr_faktury}</td><td style="text-align:right;">{inv.kwota:,.2f}</td><td>{inv.waluta}</td>'
        html += f'<td>{inv.termin_platnosci.strftime("%Y-%m-%d")}</td><td style="text-align:center;">{inv.dni_roznica}</td><td>{status_txt}</td></tr>'
    html += '</table>'

    suma_z = calculate_suma_zobowiazan(kontrahent)
    suma_p = calculate_suma_przeterminowanych(kontrahent)
    html += f'<p><strong>Suma zobowiazań:</strong> {_format_currency_sums(suma_z)}<br>'
    html += f'<strong>W tym przeterminowane:</strong> {_format_currency_sums(suma_p)}</p>'
    return html


def build_message_context(kontrahent, invoice, stage):
    """Build dict of placeholders for template rendering."""
    ctx = {
        'kontrahent_nazwa': kontrahent.nazwa or kontrahent.nip,
        'kontrahent_nip': kontrahent.nip,
        'firma_nazwa': Config.get('firma_nazwa', ''),
        'firma_adres': Config.get('firma_adres', ''),
        'firma_nip': Config.get('firma_nip', ''),
        'firma_osoba': Config.get('firma_osoba', ''),
        'tabela_zobowiazan': build_tabela_zobowiazan_html(kontrahent) if stage >= 2 else '',
        'suma_zobowiazan': _format_currency_sums(calculate_suma_zobowiazan(kontrahent)),
        'suma_przeterminowanych': _format_currency_sums(calculate_suma_przeterminowanych(kontrahent)),
    }
    if invoice:
        ctx.update({
            'nr_faktury': invoice.nr_faktury,
            'kwota': f'{invoice.kwota:,.2f}'.replace(',', ' '),
            'waluta': invoice.waluta or 'PLN',
            'data_wystawienia': invoice.data_wystawienia.strftime('%Y-%m-%d'),
            'termin_platnosci': invoice.termin_platnosci.strftime('%Y-%m-%d'),
        })
    else:
        ctx.update({
            'nr_faktury': '—',
            'kwota': '0.00',
            'waluta': 'PLN',
            'data_wystawienia': '—',
            'termin_platnosci': '—',
        })
    return ctx


def render_template_content(text, context):
    """Replace {placeholder} tokens with context values."""
    if not text:
        return ''
    result = text
    for key, val in context.items():
        result = result.replace('{' + key + '}', str(val))
    return result


def send_correspondence(kontrahent_id, etap, invoice_override=None):
    """Main function: find template -> render -> send -> log to Korespondencja.
    Returns (success, message).
    If invoice_override is provided, use that invoice for context instead of auto-detecting.
    """
    kontrahent = Kontrahent.query.get(kontrahent_id)
    if not kontrahent:
        return False, 'Kontrahent nie znaleziony.'

    wariant = kontrahent.sciezka_windykacji or 'STANDARDOWA'
    if wariant == 'BRAK':
        return False, 'Kontrahent ma ustawioną ścieżkę windykacji BRAK — nie wysyłamy.'

    kanal = kontrahent.metoda_kontaktu or 'email'

    # Find the invoice for context
    if invoice_override is not None:
        invoice = invoice_override
    else:
        _, invoice = determine_contractor_stage(kontrahent)

    # Find template
    szablon = SzablonKomunikacji.query.filter_by(etap=etap, wariant=wariant, kanal=kanal).first()
    if not szablon or not szablon.tresc:
        return False, f'Brak szablonu dla etapu {etap}, wariantu {wariant}, kanału {kanal}.'

    # Build context and render
    ctx = build_message_context(kontrahent, invoice, etap)
    rendered_subject = render_template_content(szablon.tytul, ctx)
    rendered_body = render_template_content(szablon.tresc, ctx)

    # Determine recipient
    if kanal == 'email':
        odbiorca = kontrahent.email
        if not odbiorca:
            return False, 'Kontrahent nie ma podanego adresu e-mail.'
    else:
        odbiorca = kontrahent.telefon
        if not odbiorca:
            return False, 'Kontrahent nie ma podanego numeru telefonu.'

    # Send
    if kanal == 'email':
        success, error = send_email(odbiorca, rendered_subject, rendered_body)
    else:
        success, error = send_sms(odbiorca, rendered_body)

    # Log to Korespondencja
    log = Korespondencja(
        kontrahent_id=kontrahent.id,
        invoice_id=invoice.id if invoice else None,
        etap=etap,
        wariant=wariant,
        kanal=kanal,
        tytul=rendered_subject,
        tresc=rendered_body,
        data_wyslania=datetime.now(timezone.utc),
        status='wyslana' if success else 'blad',
        blad_opis=error,
        odbiorca=odbiorca,
    )
    db.session.add(log)
    db.session.commit()

    if success:
        return True, f'Wysłano {kanal} do {odbiorca} (etap {etap}).'
    else:
        return False, f'Błąd wysyłki: {error}'


@app.route('/')
def dashboard():
    invoices = Invoice.query.all()
    # Recalculate statuses
    for inv in invoices:
        inv.oblicz_status()
    db.session.commit()

    rates = get_nbp_rates()

    stats = {
        'total_count': len(invoices),
        'total_amount': sum(kwota_pln(i, rates) for i in invoices),
        'paid_count': sum(1 for i in invoices if i.status == 'oplacona'),
        'paid_amount': sum(kwota_pln(i, rates) for i in invoices if i.status == 'oplacona'),
        'ontime_count': sum(1 for i in invoices if i.status == 'w_terminie'),
        'ontime_amount': sum(kwota_pln(i, rates) for i in invoices if i.status == 'w_terminie'),
        'overdue_count': sum(1 for i in invoices if i.status == 'przeterminowana'),
        'overdue_amount': sum(kwota_pln(i, rates) for i in invoices if i.status == 'przeterminowana'),
    }

    # Aging buckets (only unpaid)
    unpaid = [i for i in invoices if i.status != 'oplacona']
    aging = {
        'w_terminie': sum(kwota_pln(i, rates) for i in unpaid if i.kategoria_zaleglosci == 'w_terminie'),
        'd1_30': sum(kwota_pln(i, rates) for i in unpaid if i.kategoria_zaleglosci == '1-30'),
        'd31_60': sum(kwota_pln(i, rates) for i in unpaid if i.kategoria_zaleglosci == '31-60'),
        'd61_90': sum(kwota_pln(i, rates) for i in unpaid if i.kategoria_zaleglosci == '61-90'),
        'd90plus': sum(kwota_pln(i, rates) for i in unpaid if i.kategoria_zaleglosci == '90+'),
    }

    # Top 5 debtors (by overdue amount in PLN)
    debtor_map = {}
    for i in unpaid:
        if i.dni_roznica > 0:
            if i.kontrahent not in debtor_map:
                debtor_map[i.kontrahent] = {'total': 0, 'count': 0}
            debtor_map[i.kontrahent]['total'] += kwota_pln(i, rates)
            debtor_map[i.kontrahent]['count'] += 1

    top_debtors = sorted(debtor_map.items(), key=lambda x: x[1]['total'], reverse=True)[:5]
    top_debtors = [{'kontrahent': k, 'total': v['total'], 'count': v['count']} for k, v in top_debtors]

    # Prepare exchange rate info for display
    used_currencies = set(i.waluta for i in invoices if i.waluta and i.waluta != 'PLN')
    exchange_info = {c: rates.get(c, '?') for c in sorted(used_currencies)}
    rates_date = _nbp_cache.get('date', '?')

    return render_template('dashboard.html', stats=stats, aging=aging, top_debtors=top_debtors,
                           exchange_info=exchange_info, rates_date=rates_date)


@app.route('/invoices')
def invoices():
    query = Invoice.query

    # Filters
    kontrahent = request.args.get('kontrahent', '').strip()
    status = request.args.get('status', '').strip()
    data_od = request.args.get('data_od', '').strip()
    data_do = request.args.get('data_do', '').strip()

    if kontrahent:
        query = query.filter(Invoice.kontrahent.ilike(f'%{kontrahent}%'))
    if status:
        if status == 'nieoplacona':
            query = query.filter(Invoice.status.in_(['w_terminie', 'przeterminowana']))
        else:
            query = query.filter(Invoice.status == status)
    if data_od:
        query = query.filter(Invoice.data_wystawienia >= datetime.strptime(data_od, '%Y-%m-%d').date())
    if data_do:
        query = query.filter(Invoice.data_wystawienia <= datetime.strptime(data_do, '%Y-%m-%d').date())

    # Sorting
    sort_by = request.args.get('sort', 'termin_platnosci')
    sort_order = request.args.get('order', 'desc')
    valid_cols = ['kontrahent', 'nr_faktury', 'kwota', 'data_wystawienia',
                  'termin_platnosci', 'data_platnosci', 'dni_roznica', 'status']
    if sort_by not in valid_cols:
        sort_by = 'termin_platnosci'

    col = getattr(Invoice, sort_by)
    query = query.order_by(col.asc() if sort_order == 'asc' else col.desc())

    invoice_list = query.all()

    # Recalculate statuses
    for inv in invoice_list:
        inv.oblicz_status()
    db.session.commit()

    filters = {'kontrahent': kontrahent, 'status': status, 'data_od': data_od, 'data_do': data_do}

    return render_template('invoices.html', invoices=invoice_list, filters=filters,
                           sort_by=sort_by, sort_order=sort_order)


@app.route('/import', methods=['GET', 'POST'])
def import_csv():
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or not file.filename.endswith('.csv'):
            flash('Proszę wybrać plik CSV.', 'danger')
            return redirect(url_for('import_csv'))

        try:
            content = file.read().decode('utf-8-sig')

            # Auto-detect delimiter using csv.Sniffer, fallback to manual
            try:
                dialect = csv.Sniffer().sniff(content[:2000], delimiters='\t;,')
                delimiter = dialect.delimiter
            except csv.Error:
                first_line = content.split('\n')[0]
                if '\t' in first_line:
                    delimiter = '\t'
                elif ';' in first_line:
                    delimiter = ';'
                else:
                    delimiter = ','

            reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
            # Normalize header names (strip whitespace and any remaining BOM)
            if reader.fieldnames:
                reader.fieldnames = [f.strip().lstrip('\ufeff').replace('\\ufeff', '') for f in reader.fieldnames]

            detected_headers = reader.fieldnames
            app.logger.info(f'CSV delimiter={repr(delimiter)}, headers={detected_headers}')

            import_record = ImportHistory(nazwa_pliku=file.filename)
            db.session.add(import_record)
            db.session.flush()

            count = 0
            errors = []
            for row_num, row in enumerate(reader, start=2):
                try:
                    # Normalize all values
                    row = {k.strip(): (v.strip() if v else '') for k, v in row.items()}

                    kontrahent = row.get('Kontrahent', '')
                    nip = row.get('NIP', '').strip()
                    nr_faktury = row.get('Nr faktury', '')
                    kwota_str = row.get('Kwota', '').replace(',', '.').replace(' ', '')
                    waluta = row.get('Waluta', 'PLN') or 'PLN'
                    data_wyst_str = row.get('Data wystawienia', '')
                    termin_str = row.get('Termin platnosci', '')
                    data_plat_str = row.get('Data platnosci', '')

                    required = {
                        'Kontrahent': kontrahent,
                        'Nr faktury': nr_faktury,
                        'Kwota': kwota_str,
                        'Data wystawienia': data_wyst_str,
                        'Termin platnosci': termin_str,
                    }
                    missing = [k for k, v in required.items() if not v]
                    if missing:
                        errors.append(f'Wiersz {row_num}: brak pól: {", ".join(missing)} (kolumny w pliku: {list(row.keys())})')
                        continue

                    kwota = float(kwota_str)
                    data_wystawienia = parse_date(data_wyst_str)
                    termin_platnosci = parse_date(termin_str)
                    data_platnosci = None
                    if data_plat_str:
                        data_platnosci = parse_date(data_plat_str)

                    # Handle kontrahent (contractor) lookup/creation
                    kontrahent_obj = None
                    if nip:
                        kontrahent_obj = get_or_create_kontrahent(nip)

                    inv = Invoice(
                        kontrahent=kontrahent,
                        nr_faktury=nr_faktury,
                        kwota=kwota,
                        waluta=waluta,
                        data_wystawienia=data_wystawienia,
                        termin_platnosci=termin_platnosci,
                        data_platnosci=data_platnosci,
                        import_id=import_record.id,
                        kontrahent_id=kontrahent_obj.id if kontrahent_obj else None
                    )
                    inv.oblicz_status()
                    db.session.add(inv)
                    count += 1
                except (ValueError, KeyError) as e:
                    errors.append(f'Wiersz {row_num}: {str(e)}')

            import_record.liczba_rekordow = count
            db.session.commit()

            flash(f'Zaimportowano {count} faktur z pliku {file.filename}.', 'success')
            if errors:
                flash(f'Błędy ({len(errors)}): {"; ".join(errors[:5])}', 'warning')

        except Exception as e:
            db.session.rollback()
            flash(f'Błąd importu: {str(e)}', 'danger')

        return redirect(url_for('import_csv'))

    return render_template('import.html')


@app.route('/import-ksef', methods=['POST'])
def import_ksef():
    files = request.files.getlist('files')
    if not files or all(f.filename == '' for f in files):
        flash('Proszę wybrać co najmniej jeden plik XML.', 'danger')
        return redirect(url_for('import_csv'))

    import_record = ImportHistory(nazwa_pliku=f'KSeF import ({len(files)} plików)')
    db.session.add(import_record)
    db.session.flush()

    count = 0
    errors = []

    for f in files:
        if not f.filename.lower().endswith('.xml'):
            errors.append(f'{f.filename}: nie jest plikiem XML')
            continue
        try:
            content = f.read()
            data = parse_ksef_xml(content)

            kontrahent_obj = None
            if data['nip']:
                kontrahent_obj = get_or_create_kontrahent(data['nip'])

            inv = Invoice(
                kontrahent=data['kontrahent'],
                nr_faktury=data['nr_faktury'],
                kwota=data['kwota'],
                waluta=data['waluta'],
                data_wystawienia=data['data_wystawienia'],
                termin_platnosci=data['termin_platnosci'],
                data_platnosci=data['data_platnosci'],
                import_id=import_record.id,
                kontrahent_id=kontrahent_obj.id if kontrahent_obj else None,
            )
            inv.oblicz_status()
            db.session.add(inv)
            count += 1
        except Exception as e:
            errors.append(f'{f.filename}: {str(e)}')

    import_record.liczba_rekordow = count
    db.session.commit()

    if count:
        flash(f'Zaimportowano {count} faktur z KSeF XML.', 'success')
    if errors:
        flash(f'Błędy ({len(errors)}): {"; ".join(errors[:5])}', 'warning')
    if not count and not errors:
        flash('Nie znaleziono plików do importu.', 'warning')

    return redirect(url_for('import_csv'))


@app.route('/download-template')
def download_template():
    path = os.path.join(os.path.dirname(__file__), 'wzorzec_faktur.csv')
    return send_file(path, as_attachment=True, download_name='wzorzec_faktur.csv')


@app.route('/history')
def history():
    imports = ImportHistory.query.order_by(ImportHistory.data_importu.desc()).all()
    return render_template('history.html', imports=imports)


@app.route('/history/delete/<int:id>', methods=['POST'])
def delete_import(id):
    record = ImportHistory.query.get_or_404(id)
    db.session.delete(record)
    db.session.commit()
    flash(f'Usunięto import "{record.nazwa_pliku}" i powiązane faktury.', 'success')
    return redirect(url_for('history'))


@app.route('/kontrahenci')
def kontrahenci():
    contractors = Kontrahent.query.order_by(Kontrahent.nazwa).all()
    contractors_data = []
    for c in contractors:
        contractors_data.append({
            'obj': c,
            'invoice_count': c.invoices.count(),
        })
    return render_template('kontrahenci.html', contractors=contractors_data)


@app.route('/kontrahenci/<int:id>')
def kontrahent_detail(id):
    kontrahent = Kontrahent.query.get_or_404(id)
    invoice_list = kontrahent.invoices.order_by(Invoice.termin_platnosci.desc()).all()
    for inv in invoice_list:
        inv.oblicz_status()
    db.session.commit()
    # Determine current stage
    stage, _ = determine_contractor_stage(kontrahent)
    # Last 10 correspondence entries
    ostatnia_korespondencja = kontrahent.korespondencja.order_by(
        Korespondencja.data_wyslania.desc()
    ).limit(10).all()
    return render_template('kontrahent_detail.html', kontrahent=kontrahent, invoices=invoice_list,
                           suggested_stage=stage, korespondencja=ostatnia_korespondencja, etapy_info=ETAPY_INFO)


@app.route('/invoices/delete/<int:id>', methods=['POST'])
def delete_invoice(id):
    inv = Invoice.query.get_or_404(id)
    db.session.delete(inv)
    db.session.commit()
    flash(f'Usunięto fakturę "{inv.nr_faktury}".', 'success')
    return redirect(url_for('invoices'))


@app.route('/invoices/delete-all', methods=['POST'])
def delete_all_invoices():
    count = Invoice.query.delete()
    db.session.commit()
    flash(f'Usunięto wszystkie faktury ({count}).', 'success')
    return redirect(url_for('invoices'))


@app.route('/kontrahenci/<int:id>/delete', methods=['POST'])
def delete_kontrahent(id):
    k = Kontrahent.query.get_or_404(id)
    # Unlink invoices (don't delete them)
    Invoice.query.filter_by(kontrahent_id=id).update({'kontrahent_id': None})
    db.session.delete(k)
    db.session.commit()
    flash(f'Usunięto kontrahenta "{k.nazwa or k.nip}".', 'success')
    return redirect(url_for('kontrahenci'))


@app.route('/kontrahenci/delete-all', methods=['POST'])
def delete_all_kontrahenci():
    Invoice.query.update({'kontrahent_id': None})
    count = Kontrahent.query.delete()
    db.session.commit()
    flash(f'Usunięto wszystkich kontrahentów ({count}).', 'success')
    return redirect(url_for('kontrahenci'))


@app.route('/kontrahenci/export-brakujace')
def kontrahenci_export_brakujace():
    """Export CSV of contractors missing contact data for their chosen communication method."""
    contractors = Kontrahent.query.order_by(Kontrahent.nazwa).all()
    brakujace = []
    for k in contractors:
        if k.metoda_kontaktu == 'email' and not k.email:
            brakujace.append(k)
        elif k.metoda_kontaktu == 'sms' and not k.telefon:
            brakujace.append(k)

    if not brakujace:
        flash('Wszyscy kontrahenci mają uzupełnione dane kontaktowe.', 'info')
        return redirect(url_for('kontrahenci'))

    output = io.StringIO()
    output.write('\ufeff')
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['NIP', 'Nazwa', 'Metoda kontaktu', 'E-mail', 'Telefon'])
    for k in brakujace:
        writer.writerow([
            k.nip,
            k.nazwa or '',
            k.metoda_kontaktu,
            k.email or '',
            k.telefon or '',
        ])

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8-sig')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'kontrahenci_do_uzupelnienia_{date.today().isoformat()}.csv'
    )


@app.route('/kontrahenci/import-kontakty', methods=['POST'])
def kontrahenci_import_kontakty():
    """Import CSV with updated contact data for contractors."""
    file = request.files.get('file')
    if not file or not file.filename.endswith('.csv'):
        flash('Proszę wybrać plik CSV.', 'danger')
        return redirect(url_for('kontrahenci'))

    try:
        content = file.read().decode('utf-8-sig')
        try:
            dialect = csv.Sniffer().sniff(content[:2000], delimiters='\t;,')
            delimiter = dialect.delimiter
        except csv.Error:
            delimiter = ';'

        reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
        if reader.fieldnames:
            reader.fieldnames = [f.strip().lstrip('\ufeff') for f in reader.fieldnames]

        updated = 0
        for row in reader:
            row = {k.strip(): (v.strip() if v else '') for k, v in row.items()}
            nip = row.get('NIP', '').replace('-', '').strip()
            if not nip:
                continue
            k = Kontrahent.query.filter_by(nip=nip).first()
            if not k:
                continue
            email = row.get('E-mail', '').strip()
            telefon = row.get('Telefon', '').strip()
            if email:
                k.email = email
            if telefon:
                k.telefon = telefon
            updated += 1

        db.session.commit()
        flash(f'Zaktualizowano dane kontaktowe {updated} kontrahentów.', 'success')
    except Exception as e:
        db.session.rollback()
        flash(f'Błąd importu: {str(e)}', 'danger')

    return redirect(url_for('kontrahenci'))


@app.route('/kontrahenci/<int:id>/set-windykacja', methods=['POST'])
def kontrahent_set_windykacja(id):
    kontrahent = Kontrahent.query.get_or_404(id)
    val = request.form.get('sciezka_windykacji', 'STANDARDOWA')
    if val in ('OSTRA', 'STANDARDOWA', 'LEKKA', 'BRAK'):
        kontrahent.sciezka_windykacji = val
        db.session.commit()
    return redirect(url_for('kontrahenci'))


@app.route('/kontrahenci/<int:id>/set-kontakt', methods=['POST'])
def kontrahent_set_kontakt(id):
    kontrahent = Kontrahent.query.get_or_404(id)
    val = request.form.get('metoda_kontaktu', 'email')
    if val in ('email', 'sms'):
        kontrahent.metoda_kontaktu = val
        db.session.commit()
    return redirect(url_for('kontrahenci'))


@app.route('/kontrahenci/<int:id>/edit', methods=['POST'])
def kontrahent_edit(id):
    kontrahent = Kontrahent.query.get_or_404(id)
    nowy_nip = request.form.get('nip', '').strip()
    if nowy_nip and nowy_nip != kontrahent.nip:
        istniejacy = Kontrahent.query.filter(Kontrahent.nip == nowy_nip, Kontrahent.id != id).first()
        if istniejacy:
            flash(f'Kontrahent z NIP {nowy_nip} już istnieje.', 'danger')
            return redirect(url_for('kontrahent_detail', id=id))
        kontrahent.nip = nowy_nip
    kontrahent.nazwa = request.form.get('nazwa', '').strip() or None
    kontrahent.adres = request.form.get('adres', '').strip() or None
    kontrahent.sciezka_windykacji = request.form.get('sciezka_windykacji', 'STANDARDOWA')
    kontrahent.metoda_kontaktu = request.form.get('metoda_kontaktu', 'email')
    kontrahent.email = request.form.get('email', '').strip() or None
    kontrahent.telefon = request.form.get('telefon', '').strip() or None
    db.session.commit()
    flash(f'Zaktualizowano dane kontrahenta "{kontrahent.nazwa or kontrahent.nip}".', 'success')
    return redirect(url_for('kontrahent_detail', id=id))


@app.route('/kontrahenci/<int:id>/refresh', methods=['POST'])
def kontrahent_refresh(id):
    kontrahent = Kontrahent.query.get_or_404(id)
    company_data = fetch_company_by_nip(kontrahent.nip)
    if company_data:
        kontrahent.nazwa = company_data['nazwa']
        kontrahent.adres = company_data['adres']
        kontrahent.status_vat = company_data['status_vat']
        kontrahent.data_sprawdzenia = datetime.now(timezone.utc)
        db.session.commit()
        flash(f'Dane kontrahenta "{kontrahent.nazwa}" zostały zaktualizowane.', 'success')
    else:
        flash('Nie udało się pobrać danych z API Ministerstwa Finansów.', 'warning')
    return redirect(url_for('kontrahent_detail', id=id))


@app.route('/procedura')
def procedura():
    etapy = []
    for nr in range(1, 6):
        info = ETAPY_INFO[nr]
        szablony = SzablonKomunikacji.query.filter_by(etap=nr).all()
        filled = sum(1 for s in szablony if s.tresc and s.wariant != 'BRAK')
        total = sum(1 for s in szablony if s.wariant != 'BRAK')
        harm = get_harmonogram(nr)
        etapy.append({
            'nr': nr,
            'info': info,
            'szablony_count': filled,
            'szablony_total': total,
            'harmonogram': harm,
        })
    scheduler_active = scheduler.running
    return render_template('procedura.html', etapy=etapy, scheduler_active=scheduler_active)


@app.route('/procedura/<int:etap>')
def procedura_etap(etap):
    if etap not in ETAPY_INFO:
        return redirect(url_for('procedura'))
    info = ETAPY_INFO[etap]
    warianty = ['LEKKA', 'STANDARDOWA', 'OSTRA', 'BRAK']
    kanaly = ['email', 'sms']
    szablony = {}
    for w in warianty:
        szablony[w] = {}
        for k in kanaly:
            s = SzablonKomunikacji.query.filter_by(etap=etap, wariant=w, kanal=k).first()
            szablony[w][k] = s
    return render_template('procedura_etap.html', etap=etap, info=info,
                           warianty=warianty, kanaly=kanaly, szablony=szablony)


@app.route('/procedura/<int:etap>/save', methods=['POST'])
def procedura_etap_save(etap):
    if etap not in ETAPY_INFO:
        return redirect(url_for('procedura'))
    wariant = request.form.get('wariant')
    kanal = request.form.get('kanal')
    tytul = request.form.get('tytul', '').strip()
    tresc = request.form.get('tresc', '').strip()
    s = SzablonKomunikacji.query.filter_by(etap=etap, wariant=wariant, kanal=kanal).first()
    if s:
        s.tytul = tytul
        s.tresc = tresc
        db.session.commit()
        flash(f'Szablon etapu {etap} ({wariant} / {kanal}) został zapisany.', 'success')
    return redirect(url_for('procedura_etap', etap=etap))


@app.route('/procedura/reset', methods=['POST'])
def procedura_reset():
    SzablonKomunikacji.query.delete()
    db.session.commit()
    seed_szablony()
    flash('Przywrócono domyślne szablony komunikacji.', 'success')
    return redirect(url_for('procedura'))


@app.route('/procedura/<int:etap>/harmonogram', methods=['POST'])
def procedura_harmonogram_save(etap):
    if etap not in ETAPY_INFO:
        return redirect(url_for('procedura'))
    dzien_aktywacji = request.form.get('dzien_aktywacji', '0').strip()
    godzina = request.form.get('godzina', '09:00').strip()
    # Collect weekday checkboxes
    dni = []
    for d in range(1, 8):
        if request.form.get(f'dzien_{d}'):
            dni.append(str(d))
    dni_tygodnia = ','.join(dni) if dni else '1,2,3,4,5'
    aktywny = 'tak' if request.form.get('aktywny') else 'nie'

    Config.set(f'harmonogram_etap_{etap}_dzien_aktywacji', dzien_aktywacji)
    Config.set(f'harmonogram_etap_{etap}_godzina', godzina)
    Config.set(f'harmonogram_etap_{etap}_dni_tygodnia', dni_tygodnia)
    Config.set(f'harmonogram_etap_{etap}_aktywny', aktywny)
    db.session.commit()
    flash(f'Harmonogram etapu {etap} został zapisany.', 'success')
    return redirect(url_for('procedura'))


@app.route('/wyslij-automatycznie', methods=['POST'])
def wyslij_automatycznie():
    """Manual trigger — run all active stages immediately, ignoring time/day checks."""
    skip_dup = request.form.get('skip_duplicate_check') == '1'
    total_sent = 0
    total_errors = 0
    for etap in range(1, 6):
        harm = get_harmonogram(etap)
        if harm['aktywny'] != 'tak':
            continue
        s, e = run_stage_sending(etap, force=True, skip_duplicate_check=skip_dup)
        total_sent += s
        total_errors += e
    label = 'Wysyłka automatyczna (bez kontroli powtórzeń)' if skip_dup else 'Wysyłka automatyczna'
    if total_sent or total_errors:
        flash(f'{label}: wysłano {total_sent}, błędów {total_errors}.', 'success' if total_errors == 0 else 'warning')
    else:
        flash('Brak kontrahentów kwalifikujących się do wysyłki.', 'info')
    return redirect(url_for('procedura'))


@app.route('/konfiguracja', methods=['GET', 'POST'])
def konfiguracja():
    if request.method == 'POST':
        section = request.form.get('section')
        if section == 'profil':
            Config.set('firma_nazwa', request.form.get('firma_nazwa', '').strip())
            Config.set('firma_adres', request.form.get('firma_adres', '').strip())
            Config.set('firma_nip', request.form.get('firma_nip', '').strip())
            Config.set('firma_osoba', request.form.get('firma_osoba', '').strip())
            db.session.commit()
            flash('Profil firmy został zapisany.', 'success')
        elif section == 'email':
            Config.set('email_smtp_host', request.form.get('email_smtp_host', '').strip())
            Config.set('email_smtp_port', request.form.get('email_smtp_port', '').strip())
            Config.set('email_smtp_user', request.form.get('email_smtp_user', '').strip())
            Config.set('email_smtp_pass', request.form.get('email_smtp_pass', '').strip())
            Config.set('email_smtp_ssl', request.form.get('email_smtp_ssl', 'tak').strip())
            Config.set('email_from', request.form.get('email_from', '').strip())
            db.session.commit()
            flash('Konfiguracja e-mail została zapisana.', 'success')
        elif section == 'smsapi':
            Config.set('smsapi_token', request.form.get('smsapi_token', '').strip())
            Config.set('smsapi_from', request.form.get('smsapi_from', '').strip())
            db.session.commit()
            flash('Konfiguracja SMSAPI została zapisana.', 'success')
        elif section == 'gus':
            Config.set('gus_api_key', request.form.get('gus_api_key', '').strip())
            db.session.commit()
            flash('Klucz API GUS REGON został zapisany.', 'success')
        return redirect(url_for('konfiguracja'))

    cfg = {
        'firma_nazwa': Config.get('firma_nazwa'),
        'firma_adres': Config.get('firma_adres'),
        'firma_nip': Config.get('firma_nip'),
        'firma_osoba': Config.get('firma_osoba'),
        'email_smtp_host': Config.get('email_smtp_host'),
        'email_smtp_port': Config.get('email_smtp_port', '587'),
        'email_smtp_user': Config.get('email_smtp_user'),
        'email_smtp_pass': Config.get('email_smtp_pass'),
        'email_smtp_ssl': Config.get('email_smtp_ssl', 'tak'),
        'email_from': Config.get('email_from'),
        'smsapi_token': Config.get('smsapi_token'),
        'smsapi_from': Config.get('smsapi_from'),
        'gus_api_key': Config.get('gus_api_key'),
    }
    return render_template('konfiguracja.html', cfg=cfg)


@app.route('/export')
def export_invoices():
    query = Invoice.query

    kontrahent = request.args.get('kontrahent', '').strip()
    status = request.args.get('status', '').strip()
    data_od = request.args.get('data_od', '').strip()
    data_do = request.args.get('data_do', '').strip()

    if kontrahent:
        query = query.filter(Invoice.kontrahent.ilike(f'%{kontrahent}%'))
    if status:
        if status == 'nieoplacona':
            query = query.filter(Invoice.status.in_(['w_terminie', 'przeterminowana']))
        else:
            query = query.filter(Invoice.status == status)
    if data_od:
        query = query.filter(Invoice.data_wystawienia >= datetime.strptime(data_od, '%Y-%m-%d').date())
    if data_do:
        query = query.filter(Invoice.data_wystawienia <= datetime.strptime(data_do, '%Y-%m-%d').date())

    invoice_list = query.order_by(Invoice.termin_platnosci.desc()).all()

    output = io.StringIO()
    output.write('\ufeff')  # BOM for Excel
    writer = csv.writer(output, delimiter=';')
    writer.writerow(['Kontrahent', 'NIP', 'Nr faktury', 'Kwota', 'Waluta', 'Data wystawienia',
                     'Termin platnosci', 'Data platnosci', 'Dni roznica', 'Status'])

    for inv in invoice_list:
        nip = inv.contractor.nip if inv.contractor else ''
        writer.writerow([
            inv.kontrahent,
            nip,
            inv.nr_faktury,
            f'{inv.kwota:.2f}',
            inv.waluta,
            inv.data_wystawienia.strftime('%Y-%m-%d'),
            inv.termin_platnosci.strftime('%Y-%m-%d'),
            inv.data_platnosci.strftime('%Y-%m-%d') if inv.data_platnosci else '',
            inv.dni_roznica,
            inv.status
        ])

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8-sig')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'faktury_export_{date.today().isoformat()}.csv'
    )


@app.route('/korespondencja')
def korespondencja_list():
    query = Korespondencja.query

    # Filters
    kontrahent_id = request.args.get('kontrahent_id', '').strip()
    etap = request.args.get('etap', '').strip()
    kanal = request.args.get('kanal', '').strip()
    status = request.args.get('status', '').strip()

    if kontrahent_id:
        query = query.filter(Korespondencja.kontrahent_id == int(kontrahent_id))
    if etap:
        query = query.filter(Korespondencja.etap == int(etap))
    if kanal:
        query = query.filter(Korespondencja.kanal == kanal)
    if status:
        query = query.filter(Korespondencja.status == status)

    items = query.order_by(Korespondencja.data_wyslania.desc()).all()
    kontrahenci = Kontrahent.query.order_by(Kontrahent.nazwa).all()
    filters = {'kontrahent_id': kontrahent_id, 'etap': etap, 'kanal': kanal, 'status': status}
    return render_template('korespondencja.html', items=items, kontrahenci=kontrahenci,
                           filters=filters, etapy_info=ETAPY_INFO)


@app.route('/korespondencja/<int:id>')
def korespondencja_detail(id):
    item = Korespondencja.query.get_or_404(id)
    return render_template('korespondencja_detail.html', item=item, etapy_info=ETAPY_INFO)


@app.route('/kontrahenci/<int:id>/wyslij', methods=['POST'])
def kontrahent_wyslij(id):
    etap = int(request.form.get('etap', 1))
    success, msg = send_correspondence(id, etap)
    if success:
        flash(msg, 'success')
    else:
        flash(msg, 'danger')
    return redirect(url_for('kontrahent_detail', id=id))


@app.route('/wyslij-masowo', methods=['POST'])
def wyslij_masowo():
    ids = request.form.getlist('kontrahent_ids')
    etap_mode = request.form.get('etap_mode', 'auto')  # auto or fixed
    fixed_etap = int(request.form.get('etap', 1))

    sent = 0
    errors = 0
    for kid in ids:
        kid = int(kid)
        k = Kontrahent.query.get(kid)
        if not k:
            continue
        if etap_mode == 'auto':
            stage, _ = determine_contractor_stage(k)
        else:
            stage = fixed_etap
        success, _ = send_correspondence(kid, stage)
        if success:
            sent += 1
        else:
            errors += 1

    flash(f'Wysyłka masowa: wysłano {sent}, błędów {errors}.', 'success' if errors == 0 else 'warning')
    return redirect(url_for('kontrahenci'))


@app.route('/konfiguracja/test-email', methods=['POST'])
def test_email():
    to = request.form.get('test_email_to', '').strip()
    if not to:
        flash('Podaj adres e-mail do testu.', 'danger')
        return redirect(url_for('konfiguracja'))
    success, error = send_email(to, 'Test PayTiq — konfiguracja SMTP', 'To jest wiadomość testowa z systemu PayTiq.\n\nJeśli ją widzisz, konfiguracja SMTP działa poprawnie.')
    if success:
        flash(f'E-mail testowy wysłany do {to}.', 'success')
    else:
        flash(f'Błąd wysyłki e-mail: {error}', 'danger')
    return redirect(url_for('konfiguracja'))


@app.route('/konfiguracja/test-sms', methods=['POST'])
def test_sms():
    to = request.form.get('test_sms_to', '').strip()
    if not to:
        flash('Podaj numer telefonu do testu.', 'danger')
        return redirect(url_for('konfiguracja'))
    success, error = send_sms(to, 'Test PayTiq: konfiguracja SMSAPI dziala poprawnie.')
    if success:
        flash(f'SMS testowy wysłany do {to}.', 'success')
    else:
        flash(f'Błąd wysyłki SMS: {error}', 'danger')
    return redirect(url_for('konfiguracja'))


@app.route('/privacy')
def privacy():
    return render_template('privacy.html')


@app.route('/terms')
def terms():
    return render_template('terms.html')


if __name__ == '__main__':
    app.run(debug=True, port=5000)
