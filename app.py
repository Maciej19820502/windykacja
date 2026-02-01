import csv
import io
import json
import os
import urllib.request
from datetime import date, datetime, timezone

from flask import (Flask, flash, redirect, render_template, request,
                   send_file, url_for)

from models import Config, ImportHistory, Invoice, Kontrahent, SzablonKomunikacji, db


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


def fetch_company_by_nip(nip):
    """Fetch company data from MF Biala Lista VAT API. Returns dict with nazwa, adres, status_vat or None."""
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
        'tresc': 'Przypominamy o zaległej FV {nr_faktury} ({kwota} {waluta}, termin: {termin_platnosci}). Łączne zobowiązania: {suma_zobowiazan}. {firma_nazwa}',
    },
    (2, 'STANDARDOWA', 'email'): {
        'tytul': 'Przypomnienie o nieuregulowanej płatności – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\ninformujemy, że do dnia dzisiejszego nie odnotowaliśmy wpłaty z tytułu faktury nr {nr_faktury} na kwotę {kwota} {waluta}. Termin płatności upłynął dnia {termin_platnosci}.\n\nUprzejmie prosimy o pilne uregulowanie zaległości.\n\n{tabela_zobowiazan}\n\nW razie pytań prosimy o kontakt.\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (2, 'STANDARDOWA', 'sms'): {
        'tytul': '',
        'tresc': 'Zaległa FV {nr_faktury} ({kwota} {waluta}, po terminie od {termin_platnosci}). Łączne zaległości: {suma_zobowiazan}. Prosimy o pilną wpłatę. {firma_nazwa}',
    },
    (2, 'OSTRA', 'email'): {
        'tytul': 'ZALEGŁOŚĆ PŁATNICZA – wezwanie do uregulowania – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nstwierdzamy brak wpłaty z tytułu faktury nr {nr_faktury} na kwotę {kwota} {waluta}, której termin płatności upłynął dnia {termin_platnosci}.\n\nWzywamy do niezwłocznego uregulowania zaległości. Dalsze opóźnienie skutkować będzie naliczeniem odsetek ustawowych za opóźnienie w transakcjach handlowych.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (2, 'OSTRA', 'sms'): {
        'tytul': '',
        'tresc': 'ZALEGŁOŚĆ: FV {nr_faktury} ({kwota} {waluta}) po terminie od {termin_platnosci}. Łączne zaległości: {suma_zobowiazan}. Wymagana natychmiastowa wpłata. {firma_nazwa}',
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
        'tresc': 'Monit: FV {nr_faktury} ({kwota} {waluta}) nieopłacona od {termin_platnosci}. Łączne zaległości: {suma_zobowiazan}. Prosimy o kontakt. {firma_nazwa}',
    },
    (3, 'STANDARDOWA', 'email'): {
        'tytul': 'MONIT PŁATNOŚCI – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym wzywamy do uregulowania zaległej należności wynikającej z faktury nr {nr_faktury} na kwotę {kwota} {waluta}. Termin płatności upłynął dnia {termin_platnosci}.\n\nInformujemy, że od kwot przeterminowanych naliczane są odsetki ustawowe za opóźnienie w transakcjach handlowych.\n\nBrak wpłaty w ciągu 7 dni skutkować będzie podjęciem dalszych kroków windykacyjnych.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (3, 'STANDARDOWA', 'sms'): {
        'tytul': '',
        'tresc': 'MONIT: FV {nr_faktury} ({kwota} {waluta}) przeterminowana od {termin_platnosci}. Zaległości łącznie: {suma_zobowiazan}. Prosimy o wpłatę w 7 dni. {firma_nazwa}',
    },
    (3, 'OSTRA', 'email'): {
        'tytul': 'MONIT – PILNE WEZWANIE DO ZAPŁATY – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\npomimo wcześniejszych wezwań faktura nr {nr_faktury} na kwotę {kwota} {waluta} pozostaje nieuregulowana. Termin płatności upłynął dnia {termin_platnosci}.\n\nStanowczo wzywamy do natychmiastowego uregulowania całości zobowiązań. Brak wpłaty w ciągu 5 dni roboczych spowoduje przekazanie sprawy do dalszego postępowania windykacyjnego, co wiązać się będzie z dodatkowymi kosztami po Państwa stronie.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (3, 'OSTRA', 'sms'): {
        'tytul': '',
        'tresc': 'MONIT PILNY: FV {nr_faktury} ({kwota} {waluta}) przeterminowana. Łączne zaległości: {suma_zobowiazan}. Brak wpłaty w 5 dni = windykacja. {firma_nazwa}',
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
        'tresc': 'Wezwanie do zapłaty: FV {nr_faktury} ({kwota} {waluta}). Łączne zobowiązania: {suma_zobowiazan}. Termin: 7 dni. {firma_nazwa}',
    },
    (4, 'STANDARDOWA', 'email'): {
        'tytul': 'WEZWANIE DO ZAPŁATY – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym wzywamy do niezwłocznego uregulowania zaległych zobowiązań wynikających z faktury nr {nr_faktury} na kwotę {kwota} {waluta}. Termin płatności upłynął dnia {termin_platnosci}.\n\nŻądamy dokonania wpłaty w nieprzekraczalnym terminie 7 dni od otrzymania niniejszego wezwania. W przypadku braku wpłaty sprawa zostanie skierowana na drogę postępowania sądowego, co wiązać się będzie z obciążeniem Państwa kosztami postępowania, kosztami zastępstwa procesowego oraz odsetkami.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (4, 'STANDARDOWA', 'sms'): {
        'tytul': '',
        'tresc': 'WEZWANIE DO ZAPŁATY: FV {nr_faktury} ({kwota} {waluta}), termin minął {termin_platnosci}. Zaległości: {suma_zobowiazan}. Wpłata w 7 dni lub postępowanie sądowe. {firma_nazwa}',
    },
    (4, 'OSTRA', 'email'): {
        'tytul': 'WEZWANIE DO ZAPŁATY – OSTATECZNE OSTRZEŻENIE – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym kategorycznie wzywamy do natychmiastowego uregulowania zaległych zobowiązań z tytułu faktury nr {nr_faktury} na kwotę {kwota} {waluta} (termin płatności: {termin_platnosci}).\n\nInformujemy, że brak wpłaty pełnej kwoty zobowiązań w terminie 5 dni roboczych od daty niniejszego wezwania skutkować będzie:\n- naliczeniem odsetek ustawowych za opóźnienie w transakcjach handlowych,\n- obciążeniem kosztami windykacji (równowartość 40/70/100 EUR rekompensaty),\n- skierowaniem sprawy na drogę sądową.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (4, 'OSTRA', 'sms'): {
        'tytul': '',
        'tresc': 'OSTATNIE OSTRZEŻENIE: FV {nr_faktury} ({kwota} {waluta}). Zaległości: {suma_zobowiazan}. Wpłata w 5 dni lub sprawa sądowa + koszty. {firma_nazwa}',
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
        'tresc': 'Ostateczne wezwanie przedsądowe: FV {nr_faktury} ({kwota} {waluta}). Zaległości: {suma_zobowiazan}. Wpłata w 7 dni lub postępowanie sądowe. {firma_nazwa}',
    },
    (5, 'STANDARDOWA', 'email'): {
        'tytul': 'OSTATECZNE PRZEDSĄDOWE WEZWANIE DO ZAPŁATY – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\nniniejszym pismem kierujemy ostateczne przedsądowe wezwanie do zapłaty kwoty wynikającej z faktury nr {nr_faktury} na kwotę {kwota} {waluta}, z terminem płatności {termin_platnosci}.\n\nWzywamy do uregulowania całości zobowiązań w nieprzekraczalnym terminie 5 dni roboczych od daty doręczenia niniejszego wezwania.\n\nInformujemy, że niniejsze wezwanie stanowi ostateczną próbę polubownego rozwiązania sporu. Brak terminowej wpłaty skutkować będzie niezwłocznym skierowaniem sprawy na drogę postępowania sądowego, w wyniku czego zostaną Państwo obciążeni pełnymi kosztami postępowania sądowego, egzekucyjnego, kosztami zastępstwa procesowego oraz odsetkami ustawowymi za opóźnienie.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (5, 'STANDARDOWA', 'sms'): {
        'tytul': '',
        'tresc': 'OSTATECZNE WEZWANIE PRZEDSĄDOWE: FV {nr_faktury} ({kwota} {waluta}). Zaległości: {suma_zobowiazan}. Wpłata w 5 dni roboczych lub sprawa trafia do sądu. {firma_nazwa}',
    },
    (5, 'OSTRA', 'email'): {
        'tytul': 'OSTATECZNE PRZEDSĄDOWE WEZWANIE DO ZAPŁATY – {kontrahent_nazwa}',
        'tresc': 'Szanowni Państwo,\n\ndziałając w imieniu {firma_nazwa}, NIP: {firma_nip}, z siedzibą: {firma_adres}, niniejszym kierujemy OSTATECZNE PRZEDSĄDOWE WEZWANIE DO ZAPŁATY.\n\nPomimo wielokrotnych wezwań faktura nr {nr_faktury} na kwotę {kwota} {waluta} (termin płatności: {termin_platnosci}) pozostaje nieuregulowana.\n\nKATEGORYCZNIE ŻĄDAMY uregulowania pełnej kwoty zobowiązań w terminie 3 dni roboczych od daty doręczenia niniejszego wezwania.\n\nW przypadku bezskutecznego upływu wyznaczonego terminu, bez odrębnego zawiadomienia:\n1. Sprawa zostanie skierowana na drogę postępowania sądowego.\n2. Zostanie złożony wniosek o wpis do rejestru dłużników BIG.\n3. Państwa firma zostanie obciążona pełnymi kosztami: sądowymi, egzekucyjnymi, zastępstwa procesowego, odsetkami ustawowymi za opóźnienie w transakcjach handlowych oraz rekompensatą za koszty odzyskiwania należności.\n\nNiniejsze wezwanie stanowi ostateczną próbę polubownego zakończenia sprawy.\n\n{tabela_zobowiazan}\n\nZ poważaniem,\n{firma_nazwa}\n{firma_adres}\nNIP: {firma_nip}\nOsoba kontaktowa: {firma_osoba}',
    },
    (5, 'OSTRA', 'sms'): {
        'tytul': '',
        'tresc': 'OSTATECZNE WEZWANIE PRZEDSĄDOWE: FV {nr_faktury} ({kwota} {waluta}). Zaległości: {suma_zobowiazan}. Wpłata w 3 dni lub sąd + BIG + pełne koszty. {firma_nazwa}',
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


with app.app_context():
    db.create_all()
    seed_szablony()


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
    return render_template('kontrahent_detail.html', kontrahent=kontrahent, invoices=invoice_list)


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
    kontrahent.sciezka_windykacji = request.form.get('sciezka_windykacji', 'STANDARDOWA')
    kontrahent.metoda_kontaktu = request.form.get('metoda_kontaktu', 'email')
    kontrahent.email = request.form.get('email', '').strip() or None
    kontrahent.telefon = request.form.get('telefon', '').strip() or None
    db.session.commit()
    flash(f'Zaktualizowano ustawienia kontrahenta "{kontrahent.nazwa or kontrahent.nip}".', 'success')
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
        etapy.append({
            'nr': nr,
            'info': info,
            'szablony_count': filled,
            'szablony_total': total,
        })
    return render_template('procedura.html', etapy=etapy)


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


if __name__ == '__main__':
    app.run(debug=True, port=5000)
