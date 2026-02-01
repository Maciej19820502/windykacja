from flask_sqlalchemy import SQLAlchemy
from datetime import date, datetime

db = SQLAlchemy()


class Kontrahent(db.Model):
    __tablename__ = 'kontrahenci'
    id = db.Column(db.Integer, primary_key=True)
    nip = db.Column(db.String(20), unique=True, nullable=False)
    nazwa = db.Column(db.String(255), nullable=True)
    adres = db.Column(db.String(500), nullable=True)
    status_vat = db.Column(db.String(50), nullable=True)
    data_sprawdzenia = db.Column(db.DateTime, nullable=True)
    sciezka_windykacji = db.Column(db.String(20), default='STANDARDOWA')  # OSTRA, STANDARDOWA, LEKKA, BRAK
    metoda_kontaktu = db.Column(db.String(10), default='email')  # email, sms
    email = db.Column(db.String(255), nullable=True)
    telefon = db.Column(db.String(20), nullable=True)
    invoices = db.relationship('Invoice', backref='contractor', lazy='dynamic')


class Config(db.Model):
    __tablename__ = 'config'
    id = db.Column(db.Integer, primary_key=True)
    klucz = db.Column(db.String(100), unique=True, nullable=False)
    wartosc = db.Column(db.Text, nullable=True)

    @staticmethod
    def get(klucz, default=''):
        row = Config.query.filter_by(klucz=klucz).first()
        return row.wartosc if row and row.wartosc else default

    @staticmethod
    def set(klucz, wartosc):
        row = Config.query.filter_by(klucz=klucz).first()
        if row:
            row.wartosc = wartosc
        else:
            db.session.add(Config(klucz=klucz, wartosc=wartosc))


class SzablonKomunikacji(db.Model):
    __tablename__ = 'szablony_komunikacji'
    id = db.Column(db.Integer, primary_key=True)
    etap = db.Column(db.Integer, nullable=False)          # 1-5
    wariant = db.Column(db.String(20), nullable=False)     # LEKKA, STANDARDOWA, OSTRA, BRAK
    kanal = db.Column(db.String(10), nullable=False)       # email, sms
    tytul = db.Column(db.String(500), nullable=True)       # subject (email only)
    tresc = db.Column(db.Text, nullable=True)              # body

    __table_args__ = (
        db.UniqueConstraint('etap', 'wariant', 'kanal', name='uq_etap_wariant_kanal'),
    )


class Korespondencja(db.Model):
    __tablename__ = 'korespondencja'
    id = db.Column(db.Integer, primary_key=True)
    kontrahent_id = db.Column(db.Integer, db.ForeignKey('kontrahenci.id'), nullable=False)
    invoice_id = db.Column(db.Integer, db.ForeignKey('invoices.id'), nullable=True)
    etap = db.Column(db.Integer, nullable=False)
    wariant = db.Column(db.String(20), nullable=False)
    kanal = db.Column(db.String(10), nullable=False)
    tytul = db.Column(db.String(500), nullable=True)
    tresc = db.Column(db.Text, nullable=True)
    data_wyslania = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.String(20), default='wyslana')  # wyslana / blad
    blad_opis = db.Column(db.Text, nullable=True)
    odbiorca = db.Column(db.String(255), nullable=True)

    kontrahent_rel = db.relationship('Kontrahent', backref=db.backref('korespondencja', lazy='dynamic'))
    invoice_rel = db.relationship('Invoice', backref=db.backref('korespondencja', lazy='dynamic'))


class ImportHistory(db.Model):
    __tablename__ = 'import_history'
    id = db.Column(db.Integer, primary_key=True)
    nazwa_pliku = db.Column(db.String(255), nullable=False)
    data_importu = db.Column(db.DateTime, default=datetime.utcnow)
    liczba_rekordow = db.Column(db.Integer, default=0)
    invoices = db.relationship('Invoice', backref='import_record', cascade='all, delete-orphan')


class Invoice(db.Model):
    __tablename__ = 'invoices'
    id = db.Column(db.Integer, primary_key=True)
    kontrahent = db.Column(db.String(255), nullable=False)
    nr_faktury = db.Column(db.String(100), nullable=False)
    kwota = db.Column(db.Float, nullable=False)
    waluta = db.Column(db.String(10), default='PLN')
    data_wystawienia = db.Column(db.Date, nullable=False)
    termin_platnosci = db.Column(db.Date, nullable=False)
    data_platnosci = db.Column(db.Date, nullable=True)
    dni_roznica = db.Column(db.Integer, default=0)
    status = db.Column(db.String(50), default='nieoplacona')
    import_id = db.Column(db.Integer, db.ForeignKey('import_history.id'), nullable=False)
    kontrahent_id = db.Column(db.Integer, db.ForeignKey('kontrahenci.id'), nullable=True)

    def oblicz_status(self):
        today = date.today()
        if self.data_platnosci:
            self.dni_roznica = (self.data_platnosci - self.termin_platnosci).days
            self.status = 'oplacona'
        else:
            self.dni_roznica = (today - self.termin_platnosci).days
            if self.dni_roznica <= 0:
                self.status = 'w_terminie'
            else:
                self.status = 'przeterminowana'

    @property
    def kategoria_zaleglosci(self):
        if self.status == 'oplacona':
            return 'oplacona'
        if self.dni_roznica <= 0:
            return 'w_terminie'
        elif self.dni_roznica <= 30:
            return '1-30'
        elif self.dni_roznica <= 60:
            return '31-60'
        elif self.dni_roznica <= 90:
            return '61-90'
        else:
            return '90+'
