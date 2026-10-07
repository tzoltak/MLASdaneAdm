![KL+RP+IBE+EFS](inst/Belka-Losy-absolwentow-Kolor-PL.png)

# MLASdaneAdm

Pakiet został opracowany w ramach projektu *Monitorowanie losów absolwentów szkół zawodowych - etap II* (POWR.02.15.00-IP.02-00-002/19) prowadzonego w Instytucie Badań Edukacyjnych w ramach działania 2.15. Kształcenie i szkolenie zawodowe dostosowane do potrzeb zmieniającej się gospodarki II. osi priorytetowej Efektywne polityki publiczne dla rynku pracy, gospodarki i edukacji Programu Operacyjnego Wiedza, Edukacja, Rozwój

Pakiet służy do złączania danych z rejestrów, baz danych administracyjnych i baz danych wyników egzaminów na potrzeby prowadzenia monitoringu dalszych losów edukacyjnych i zawodowych absolwentów polskich szkół ponadpodstawowych.

# Instalacja / aktualizacja

Pakiet nie jest wypchnięty na CRAN, więc trzeba instalować go ze źródeł.

Ponieważ jednak zawiera jedynie kod w R, nie ma potrzeby zaopatrywać się w kompilatory, itp.

Instalację najprościej przeprowadzić wykorzystując pakiet *devtools*:

``` r
install.packages('devtools') # potrzebne tylko, gdy nie jest jeszcze zainstalowany
devtools::install_github('tzoltak/MLASdaneAdm', build_opts = c("--no-resave-data"))
```

Dokładnie w ten sam sposób można przeprowadzić aktualizację pakietu do najnowszej wersji.

# Użycie

## Funkcje pakietu

Pakiet udostępnia obecnie następujące podstawowe funkcje:

1.  `przygotuj_dane_do_w19()` pozwala przygotować dane ze wskaźnikami bezrobocia i przecietnych miesięcznych wynagrodzeń w formie, w jakiej są zwracane przez `MLASZdane::pobierz_dane_bdl` do formatu wymaganego dla tabeli *wejściowej* W19 (przykład użycia dostępny w dokumentacji funkcji: `?przygotuj_dane_do_w19`).
2.  `przygotuj_dane_do_w26()` pozwala przygotować, na podstawie eksportu z hurtowni SIO (lub ew. publicznego API WWW RSPO), zestawienie danych wszystkich szkół (i placówek oświatowych) objętych SIO, które występują w danych z danej edycji monitoringu, w formacie wymaganym dla tabeli *wejściowej* W26.
2.  `przygotuj_dane_do_w30()` pozwala przygotować, na podstawie eksportu z publicznego API RAD-on oraz pliku *SUCZELNIA.csv*, zestawienie danych wszystkich szkół wyższych (i ich filii), które występują w danych z danej edycji monitoringu, w formacie wymaganym dla tabeli *wejściowej* W30.
3.  `wczytaj_tabele_wejsciowe()` odpowiada za zdiagnozowanie spójności zbiorów danych CSV zawierających poszczególne zestawienia wyeksportowane z systemów informatycznych zawierających informacje wykorzystywane w monitoringu (przez gestorów tych systemów) oraz wczytanie ich, po niewielkiej obróbce, do relacyjnej bazy danych,
4.  `przygotuj_tabele_posrednie()` odpowiada za przygotowanie na podstawie danych wczytanych do relacyjnej bazy danych w poprzednim kroku 4 *tabel pośrednich*, które zawierają zestawienia wskaźników opisujących sytuację absolwentów, które są już bezpośrednio użyteczne analitycznie:
    1) *p1* - zestawienie uzyskanych przez absolwentów certyfikatów i dyplomów (rekord stanowi absolwento-certyfikat/dyplom),
    2) *p2* - zestawienie kontynuacji kształcenia przez absolwentów w wybranych punktach czasu (miesiącach),
    3) *p3* - miesięczne dane o statusach edukacyjnych i zawodowych oraz dane o wynagrodzeniach,
    4) *p4* - zestawienie stałych w czasie cech absolwenta i wskaźników obliczonych na podstawie całego okresu od ukończenia przez niego szkoły,
    5) *p5* - zestawienie charakterystyk opisujących miesiąco-miejsca zatrudnienia, na podstawie którego można obliczać wskaźniki stałości zatrudnienia (od edycji monitoringu 2022),
    6) *p6* - charakterystyki szkół - w tym szkół, w których absolwenci kontynuowali kształcenie - z RSPO,
5.  `wczytaj_tabele_posrednie()` odpowiada za zapisanie do relacyjnej bazy danych *tabel pośrednich* powstałych w wyniku użycia funkcji `przygotuj_tabele_posrednie()` (sensem takiego zapisu jest głównie walidacja przygotowanych danych, bo nie przyjęło się w praktyce korzystać z tabel *pośrednich* zapisanych do bazy),
6.  `usun_duplikaty()` pozwala usunąć z danych absolwentów, którzy występują wielokrotnie w ramach tej samej szkoły (historyczny problem ze sposobem ewidencjonowania w SIO uczniów oddziałów wielozawodowych; rzadziej ukończenie kilku różnych zawodów w tej samej spolic.) lub zostali absolwentami kilku różnych szkół w tym samym roku.

`wczytaj_tabele_wejsciowe()` wymaga wskazania lokalizacji folderu z plikami CSV zawierającymi dane, które mają zostać wczytane do bazy oraz danych niezbędnych do połączenia się z bazą (p. następna sekcja). Funkcja nic nie zwraca, niemniej domyślnie generuje w katalogu roboczym pliki CSV zawierające zestawienia problematycznych danych (uwaga, ich występowanie **nie** uniemożliwia wczytania danych do bazy i ich dalszego przetwarzania! niemniej w ostatecznym rozrachunku rzutuje na trafność i rzetelność wskaźników).

-   Wczytanie danych z monitoringu 2021 r. (tylko roczniki *w rok* i *w dwa lata* od ukończenia szkoły) do działającej lokalnie bazy PostgreSQL 10.1 na komputerze z 16 GB RAM i procesorem Intel i7-4720HQ trwa 35-40 minut (w konfiguracji z 32 GB RAM i procesorem Ryzen 5 5600H dane z monitoringu 2022 r. wczytywały się ~30 minut).
-   Wczytanie danych z monitoringu 2026 r. (roczniki *w rok*, *w dwa lata* i w *siedem lat* od ukończenia szkoły, poszerzony zakres przetwarzanych danych) do działającej lokalnie bazy PostgreSQL 16 na komputerze z 32 GB RAM i procesorem Intel i5-12450H trwa ~3:30 h.

`przygotuj_tabele_posrednie()` wymaga jedynie przekazania danych niezbędnych do połączenia się z bazą (p. następna sekcja). Przygotowane *tabele pośrednie* domyślnie zwraca jako listę *ramek danych* w środowisku R, z której pobierała dane wejściowe.

-   Dla danych z monitoringu 2021 r. (tylko roczniki *w rok* i *w dwa lata* od ukończenia szkoły) , na komputerze z 16 GB RAM i procesorem Intel i7-4720HQ, z wykorzystaniem działającej lokalnie bazy PostgreSQL 10.1 trwa ~20 minut - bez tworzenia *tabeli pośredniej p5* (w konfiguracji z 32 GB RAM i procesorem Ryzen 5 5600H dane z monitoringu 2022 r. przetwarzały się ~18 minut, z tworzeniem *tabeli pośredniej p5*).
-   Dla danych z monitoringu 2026 r. (roczniki *w rok*, *w dwa lata* i w *siedem lat* od ukończenia szkoły, poszerzony zakres przetwarzanych danych) do działającej lokalnie bazy PostgreSQL 16 na komputerze z 32 GB RAM i procesorem Intel i5-12450H1 trwa ~4 h.

Sposób działania obu funkcji - a szczególnie `przygotuj_tabele_posrednie()` - można modyfikować przy pomocy dodatkowych argumentów. Więcej na ten temat można dowiedzieć się w ich dokumentacji:

```{r}
?wczytaj_tabele_wejsciowe()
?przygotuj_tabele_posrednie()
```

## Współpraca z bazą danych

Aby móc skorzystać z pakietu konieczne jest posiadanie dostępu do odpowiednio przygotowanej relacyjnej bazy danych, która służy jako miejsce przechowywania przygotowywanych przez pakiet zbiorów danych oraz w pewnym zakresie jako backend obliczeniowy przy przygotowywaniu tych zbiorów. Pakiet testowany był we współpracy z bazą PostgreSQL 10.1, 14.0 oraz 16.2, niemniej powinien być zdolny do współpracy z dowolną relacyjną bazą danych **dla której istnieje sterownik zgodny z pakietem DBI środowiska R** (p. [strona pakietu DBI](https://dbi.r-dbi.org/)). Skrypt SQL tworzący strukturę bazy danych (pomijając kwestię przyznawania uprawnień) znajduje się w ostatniej sekcji niniejszego dokumentu.

Dane niezbędne do połączenia z bazą można przekazać funkcjom pakietu na dwa sposoby (tu na przykładzie połączenia z bazą PostgreSQL o nazwie 'monitoring-abs', działającej lokalnie na tym samym komputerze):

1.  Jako uchwyt połączenia z bazą danych:

```{r}
baza <- DBI::dbConnect(RPostgres::Postgres(),
                       dbname = "monitoring-abs",
                       host = "127.0.0.1",
                       port = 5432,
                       user = "nazwa_uzytkownika",
                       password = "haslo")
```

2.  Jako listę argumentów niezbędnych do nawiązania połączenia z bazą danych:

```{r}
baza <- list(drv = RPostgres::Postgres(),
             dbname = "monitoring-abs",
             host = "127.0.0.1",
             port = 5432,
             user = "nazwa_uzytkownika",
             password = "haslo")
```

# Skrypt tworzący bazę danych

## Edycja 2026

```{sql}
/* ### "słownikowe" typy danych ############################################# */
CREATE TYPE plec AS ENUM ('K', 'M');
CREATE TYPE profil_stu AS ENUM ('P', 'O', 'N');
CREATE TYPE forma_stu AS ENUM ('S', 'N');
CREATE TYPE adres_typ AS ENUM ('meld', 'zam', 'koresp');
CREATE TYPE rodzaj_dyplomu AS ENUM ('tytuł czeladnika', 'matura', 'certyfikat kwalifikacji', 'dyplom zawodowy', 'dyplom licencjata/inżyniera', 'dyplom magistra/lekarza', 'dyplom oficera');
CREATE TYPE kat_uczn AS ENUM ('Dzieci lub młodzież', 'Dorośli', 'Bez kategorii');
CREATE TYPE specyfika AS ENUM ('brak specyfiki', 'specjalna');
CREATE TYPE sposob_ewidencjonowania AS ENUM ('Prowadzona', 'Rejestrowana');
CREATE TYPE miejsce_w_strukt AS ENUM ('samodzielna', 'filia szkoły lub placówki', 'szkoła/placówka wchodząca w skład jednostki złożonej', 'jednostka złożona');
CREATE TYPE rok_szk AS ENUM ('1980/1981', '1981/1982', '1982/1983', '1983/1984', '1984/1985', '1985/1986', '1986/1987', '1987/1988', '1988/1989', '1989/1990', '1990/1991', '1991/1992', '1992/1993', '1993/1994', '1994/1995', '1995/1996', '1996/1997', '1997/1998', '1998/1999', '1999/2000',
                             '2000/2001', '2001/2002', '2002/2003', '2003/2004', '2004/2005', '2005/2006', '2006/2007', '2007/2008', '2008/2009', '2009/2010', '2010/2011', '2011/2012', '2012/2013', '2013/2014', '2014/2015', '2015/2016', '2016/2017', '2017/2018', '2018/2019', '2019/2020',
                             '2020/2021', '2021/2022', '2022/2023', '2023/2024', '2024/2025', '2025/2026');
CREATE TYPE forma_kontynuacji AS ENUM ('uczeń', 'KKZ', 'KUZ', 'student');
CREATE TYPE status AS ENUM ('Tylko nauka', 'Nauka i praca', 'Tylko praca', 'Bezrobocie', 'Brak danych o aktywności');
CREATE TYPE typ_szk AS ENUM ('Branżowa szkoła I stopnia', 'Branżowa szkoła II stopnia', 'Technikum', 'Liceum ogólnokształcące', 'Liceum dla dorosłych', 'Szkoła policealna', 'Szkoła specjalna przysposabiająca do pracy');
CREATE TYPE typ_szk_mlodoc AS ENUM ('Młodociani w Branżowej szkole I stopnia', 'Niemłodociani w Branżowej szkole I stopnia', 'Branżowa szkoła II stopnia', 'Technikum', 'Liceum ogólnokształcące', 'Liceum dla dorosłych', 'Szkoła policealna', 'Szkoła specjalna przysposabiająca do pracy');
CREATE TYPE forma_ksztalcenia AS ENUM ('dzienna', 'stacjonarna', 'zaoczna');
CREATE TYPE rodzaj_uczelni AS ENUM ('Uczelnia publiczna', 'Uczelnia niepubliczna', 'Uczelnia kościelna', 'Filia uczelni publicznej', 'Filia uczelni niepublicznej', 'Filia uczelni kościelnej');
CREATE TYPE typ_podmiotu AS ENUM ('akademicka', 'zawodowa');
CREATE TYPE status_podmiotu AS ENUM ('Działająca', 'W likwidacji', 'Dane ekstrapolowane w przeszłość', 'Dane ekstrapolowane w przyszłość');
CREATE TYPE stan_zdawania AS ENUM ('Przystąpił', 'Finalista', 'Laureat', 'Nieobecny', 'Nie przeprowadzano egzaminu', 'Unieważniony', 'Zwolniony', 'Przerwany');
CREATE TYPE termin_matury AS ENUM ('główny', 'poprawkowy', 'dodatkowy');
CREATE TYPE poziom_matury AS ENUM ('P', 'R', 'D', 'N', 'Z');
CREATE TYPE forma_zdawania AS ENUM ('pisemna', 'ustna');

/* ### tabele "wejściowe" ################################################### */
CREATE TABLE w29 (
	kraj_pochodzenia text PRIMARY KEY,
	region_pochodzenia text NOT NULL CHECK (region_pochodzenia != '')
);
CREATE TABLE w1 (
	id_abs int,
	rok_abs int CHECK (rok_abs >= 2019),
	kraj_pochodzenia text,
	PRIMARY KEY (id_abs, rok_abs)
);
CREATE TABLE typy_szkol (
	typ_szk text PRIMARY KEY
);
CREATE TABLE w25 (
	teryt_pow int CHECK (teryt_pow >= 20100 AND teryt_pow <= 329900),
	powiat text NOT NULL CHECK (powiat != ''),
	wojewodztwo text NOT NULL CHECK (wojewodztwo != ''),
	nts int NOT NULL,
	makroregion text NOT NULL CHECK (makroregion != ''),
	region text NOT NULL CHECK (region != ''),
	podregion text NOT NULL CHECK (podregion != ''),
	PRIMARY KEY (teryt_pow),
	UNIQUE (teryt_pow, powiat, wojewodztwo, makroregion, region, podregion, nts)
);
CREATE TABLE w19 (
	rok int CHECK (rok > 2000),
	miesiac int CHECK (miesiac >=1 AND miesiac <= 12),
	teryt_pow int REFERENCES w25 (teryt_pow) ON DELETE CASCADE ON UPDATE CASCADE,
	stopa_bezrobocia real CHECK (stopa_bezrobocia >= 0),
	sr_wynagrodzenia real CHECK (sr_wynagrodzenia > 0),
	PRIMARY KEY (rok, miesiac, teryt_pow)
);
/* unikalne id szkół */
CREATE TABLE w26a (
	id_szk int PRIMARY KEY CHECK (id_szk > 0),
	/* typ szkoły jest w praktyce jedyną cechą szkoły, dla której gwarantowana jest stałość w czasie! */
	typ_szk text NOT NULL REFERENCES typy_szkol (typ_szk) ON DELETE CASCADE ON UPDATE CASCADE,
	UNIQUE (id_szk, typ_szk)
);
/* dane wszystkich szkół występujących w danych z RSPO */
CREATE TABLE w26 (
	id_szk int REFERENCES w26a (id_szk) ON DELETE CASCADE ON UPDATE CASCADE,
	rok_szk rok_szk NOT NULL,
	nazwa_szk text NOT NULL CHECK (nazwa_szk != ''),
	teryt_gmi_szk int NOT NULL,
	wojewodztwo_szk text NOT NULL CHECK (wojewodztwo_szk != ''),
	powiat_szk text NOT NULL CHECK (powiat_szk != ''),
	gmina_szk text NOT NULL CHECK (gmina_szk != ''),
	simc_miejsc int NOT NULL,
	miejscowosc text NOT NULL CHECK (miejscowosc != ''),
	rodzaj_miejsc text NOT NULL CHECK (rodzaj_miejsc != ''),
	sym_ul int,
	ulica text,
	nr_budynku text,
	nr_lokalu text,
	pna text NOT NULL CHECK (pna != ''),
	poczta text NOT NULL CHECK (poczta != ''),
	publicznosc text NOT NULL CHECK (publicznosc != ''),
	kategoria_uczniow kat_uczn NOT NULL,
	specyfika specyfika NOT NULL,
	organ_rejestrujacy_id int NOT NULL,
	organ_rejestrujacy_typ text NOT NULL CHECK (organ_rejestrujacy_typ != ''),
	organ_rejestrujacy_nazwa text NOT NULL CHECK (organ_rejestrujacy_nazwa != ''),
	organ_sposob_ewidencjonowania sposob_ewidencjonowania NOT NULL,
	organ_rejestrujacy_teryt int,
	organ_prowadzacy_typ text NOT NULL CHECK (organ_prowadzacy_typ != ''),
	organ_prowadzacy_nazwa text NOT NULL CHECK (organ_prowadzacy_nazwa != ''),
	organ_prowadzacy_regon text,
	organ_prowadzacy_teryt int NOT NULL,
	organ_prowadzacy_woj text NOT NULL CHECK (organ_prowadzacy_woj != ''),
	organ_prowadzacy_pow text NOT NULL CHECK (organ_prowadzacy_pow != ''),
	organ_prowadzacy_gmi text NOT NULL CHECK (organ_prowadzacy_gmi != ''),
	miejsce_w_strukt miejsce_w_strukt NOT NULL,
	jedn_nadrz_id int,
	jedn_nadrz_typ text CHECK (CASE WHEN jedn_nadrz_id IS NOT NULL THEN jedn_nadrz_typ IS NOT NULL AND jedn_nadrz_typ != '' ELSE jedn_nadrz_typ IS NULL END),
	PRIMARY KEY (id_szk, rok_szk)
);
/* unikalne kody zawodow (w tym zawody nie przypisane do żadnej branży) */
CREATE TABLE w20a (
	kod_zaw int PRIMARY KEY
);
/* uwaga! mapowanie zawodów na branże jest 1:n (są tu też stare klasyfikacje) */
CREATE TABLE w20 (
	kod_zaw int REFERENCES w20a (kod_zaw) ON DELETE CASCADE ON UPDATE CASCADE,
	wersja_klasyfikacji int CHECK (wersja_klasyfikacji IN (1, 2, 3, 4)),
	branza text CHECK (branza != ''),
	branza_kod text CHECK (branza_kod != ''),
	PRIMARY KEY (kod_zaw, wersja_klasyfikacji)
);
/* uwaga! zdarzają się osoby kończące w tym samym roku kilka zawodów nawet w jednej szkole (zwykle policealnej) */
CREATE TABLE w2 (
	id_abs int,
	rok_abs int,
	rok_ur int,
	plec plec NOT NULL,
	id_szk int NOT NULL,
	typ_szk text NOT NULL,
	teryt_szk int CHECK (teryt_szk >= 201011 AND teryt_szk <= 3299999),
	teryt_pow_szk int NOT NULL REFERENCES w25 (teryt_pow) ON DELETE CASCADE ON UPDATE CASCADE,
	lp int NOT NULL,
	kod_zaw int REFERENCES w20a (kod_zaw) ON DELETE CASCADE ON UPDATE CASCADE,
	nazwa_zaw text,
	forma_ksztalc forma_ksztalcenia NOT NULL,
	nauka_poza_szkola boolean NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, id_szk, lp),
	FOREIGN KEY (id_szk, typ_szk) REFERENCES w26a (id_szk, typ_szk) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w3 (
	id_abs int,
	rok_abs int,
	id_szk_kont int NOT NULL,
	typ_szk_kont text NOT NULL,
	data_od_szk_kont date NOT NULL,
	lp int,
	czy_ukoncz_szk_kont int NOT NULL CHECK (czy_ukoncz_szk_kont >= 0 AND czy_ukoncz_szk_kont <= 2),
	data_do_szk_kont date, CHECK (CASE WHEN czy_ukoncz_szk_kont != 0 THEN data_do_szk_kont IS NOT NULL ELSE data_do_szk_kont IS NULL END),
	kod_zaw_kont int REFERENCES w20a (kod_zaw) ON DELETE CASCADE ON UPDATE CASCADE,
	nazwa_zaw_kont text CHECK (CASE WHEN kod_zaw_kont IS NOT NULL THEN nazwa_zaw_kont IS NOT NULL AND nazwa_zaw_kont != '' END),
	PRIMARY KEY (id_abs, rok_abs, id_szk_kont, data_od_szk_kont, lp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (id_szk_kont, typ_szk_kont) REFERENCES w26a (id_szk, typ_szk)
);
/* uwaga! mapowanie kwalifikacji na zawody jest 1:n */
CREATE TABLE w21a (
	kod_kwal text PRIMARY KEY CHECK (kod_kwal != '')
);
CREATE TABLE w21 (
	kod_kwal text REFERENCES w21a (kod_kwal) ON DELETE CASCADE ON UPDATE CASCADE,
	kod_zaw int REFERENCES w20a (kod_zaw) ON DELETE CASCADE ON UPDATE CASCADE,
	PRIMARY KEY (kod_kwal, kod_zaw)
);
CREATE TABLE w4 (
	id_abs int,
	rok_abs int,
	id_szk_kont int NOT NULL REFERENCES w26a (id_szk) ON DELETE CASCADE ON UPDATE CASCADE,
	lp int NOT NULL,
	data_od_kkz date NOT NULL,
	data_do_kkz date,
	kod_kwal_kkz text NOT NULL REFERENCES w21a (kod_kwal) ON DELETE CASCADE ON UPDATE CASCADE,
	PRIMARY KEY (id_abs, rok_abs, id_szk_kont, lp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w5 (
	id_abs int,
	rok_abs int,
	id_szk_kont int NOT NULL REFERENCES w26a (id_szk) ON DELETE CASCADE ON UPDATE CASCADE,
	lp int NOT NULL,
	data_od_kuz date NOT NULL,
	data_do_kuz date,
	kod_zaw_kuz int NOT NULL REFERENCES w20a (kod_zaw) ON DELETE CASCADE ON UPDATE CASCADE,
	PRIMARY KEY (id_abs, rok_abs, id_szk_kont, lp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w6 (
	id_abs int,
	rok_abs int,
	kod_zaw_czel int NOT NULL REFERENCES w20a (kod_zaw) ON DELETE CASCADE ON UPDATE CASCADE,
	PRIMARY KEY (id_abs, rok_abs, kod_zaw_czel),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w7 (
	id_abs int,
	rok_abs int,
	rok_matura int NOT NULL,
	czy_zdana_matura boolean NOT NULL,
	data_swiad_matura date CHECK ((NOT czy_zdana_matura AND data_swiad_matura IS NULL) OR (czy_zdana_matura AND data_swiad_matura IS NOT NULL)),
	PRIMARY KEY (id_abs, rok_abs, rok_matura),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w27 (
	id_abs int,
	rok_abs int,
	rok_e8 int NOT NULL CHECK (rok_e8 >= 2019),
	termin_e8 termin_matury NOT NULL CHECK (termin_e8 != 'poprawkowy'),
	przedmiot_e8 text NOT NULL,
	dostos_form_e8 bool NOT NULL,
	dostos_war_e8 bool NOT NULL,
	stan_zdawania_e8 stan_zdawania NOT NULL,
	wynik_e8 real CHECK ((wynik_e8 >= 0 AND wynik_e8 <= 100) OR wynik_e8 IS NULL)
		CHECK ((stan_zdawania_e8 IN ('Przystąpił', 'Finalista', 'Laureat') AND wynik_e8 IS NOT NULL) OR (stan_zdawania_e8 NOT IN ('Przystąpił', 'Finalista', 'Laureat') AND wynik_e8 IS NULL)),
	PRIMARY KEY (id_abs, rok_abs, rok_e8, przedmiot_e8),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w27d (
	id_abs int,
	rok_abs int,
	rok_e8 int NOT NULL CHECK (rok_e8 >= 2019),
	przedmiot_e8 text NOT NULL,
	dostos_form_opis_e8 text NOT NULL CHECK (dostos_form_opis_e8 != ''),
	dostos_war_e8 bool NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, rok_e8, przedmiot_e8, dostos_form_opis_e8),
	FOREIGN KEY (id_abs, rok_abs, rok_e8, przedmiot_e8) REFERENCES w27 (id_abs, rok_abs, rok_e8, przedmiot_e8) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w28 (
	id_abs int,
	rok_abs int,
	rok_matura int NOT NULL,
	termin_matura termin_matury NOT NULL,
	przedmiot_matura text NOT NULL,
	poziom_matura poziom_matury NOT NULL,
	forma_zdawania forma_zdawania NOT NULL,
	dostos_form_matura bool CHECK ((rok_matura < 2021 AND dostos_form_matura IS NULL) OR (rok_matura >= 2021 AND dostos_form_matura IS NOT NULL)),
	dostos_war_matura bool CHECK ((rok_matura < 2021 AND dostos_war_matura IS NULL) OR (rok_matura >= 2021 AND dostos_war_matura IS NOT NULL)),
	stan_zdawania_matura stan_zdawania NOT NULL,
	wynik_matura real CHECK ((wynik_matura >= 0 AND wynik_matura <= 100) OR wynik_matura IS NULL)
		CHECK ((stan_zdawania_matura IN ('Przystąpił', 'Finalista', 'Laureat') AND wynik_matura IS NOT NULL) OR (stan_zdawania_matura NOT IN ('Przystąpił', 'Finalista', 'Laureat') AND wynik_matura IS NULL)),
	PRIMARY KEY (id_abs, rok_abs, rok_matura, termin_matura, przedmiot_matura, poziom_matura, forma_zdawania),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w28d (
	id_abs int,
	rok_abs int,
	rok_matura int NOT NULL,
	termin_matura termin_matury NOT NULL,
	przedmiot_matura text NOT NULL,
	poziom_matura poziom_matury NOT NULL,
	forma_zdawania forma_zdawania NOT NULL,
	dostos_form_opis_matura text NOT NULL CHECK (dostos_form_opis_matura != ''),
	dostos_war_matura bool NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, rok_matura, termin_matura, przedmiot_matura, poziom_matura, forma_zdawania, dostos_form_opis_matura),
	FOREIGN KEY (id_abs, rok_abs, rok_matura, termin_matura, przedmiot_matura, poziom_matura, forma_zdawania) REFERENCES w28 (id_abs, rok_abs, rok_matura, termin_matura, przedmiot_matura, poziom_matura, forma_zdawania) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w810 (
	id_abs int,
	rok_abs int,
	rok_egz_zaw int NOT NULL,
	sesja_egz_zaw text NOT NULL CHECK (sesja_egz_zaw != ''),
	kod_kwal text REFERENCES w21a (kod_kwal) ON DELETE CASCADE ON UPDATE CASCADE,
	wynik_pisemn_egz_zaw real CHECK ((wynik_pisemn_egz_zaw >= 0 AND wynik_pisemn_egz_zaw <= 100) OR wynik_pisemn_egz_zaw IS NULL),
	wynik_prakt_egz_zaw real CHECK ((wynik_prakt_egz_zaw >= 0 AND wynik_prakt_egz_zaw <= 100) OR wynik_prakt_egz_zaw IS NULL),
	dok_potw_kwal int CHECK (dok_potw_kwal IN (1, 2)),
	data_kwal date,
	PRIMARY KEY (id_abs, rok_abs, rok_egz_zaw, sesja_egz_zaw, kod_kwal),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w810d (
	id_abs int,
	rok_abs int,
	rok_egz_zaw int NOT NULL,
	sesja_egz_zaw text NOT NULL CHECK (sesja_egz_zaw != ''),
	kod_kwal text NOT NULL,
	dostosowanie_egz_zaw text NOT NULL CHECK (dostosowanie_egz_zaw != ''),
	PRIMARY KEY (id_abs, rok_abs, rok_egz_zaw, sesja_egz_zaw, kod_kwal, dostosowanie_egz_zaw),
	FOREIGN KEY (id_abs, rok_abs, rok_egz_zaw, sesja_egz_zaw, kod_kwal) REFERENCES w810 (id_abs, rok_abs, rok_egz_zaw, sesja_egz_zaw, kod_kwal)
);
CREATE TABLE w911 (
	id_abs int,
	rok_abs int,
	kod_zaw int NOT NULL REFERENCES w20a (kod_zaw) ON DELETE CASCADE ON UPDATE CASCADE,
	rok_dyp int NOT NULL,
	dok_potw_dyp int NOT NULL CHECK (dok_potw_dyp IN (1, 2)),
	data_dyp_zaw date NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, kod_zaw),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w24 (
	kod_zaw int NOT NULL REFERENCES w20a (kod_zaw) ON DELETE CASCADE ON UPDATE CASCADE,
	kod_isced char(4) NOT NULL,
	grupa_isced text NOT NULL CHECK (grupa_isced != ''),
	podgrupa_isced text NOT NULL CHECK (podgrupa_isced != ''),
	nazwa_isced text NOT NULL CHECK (nazwa_isced != ''),
	PRIMARY KEY (kod_zaw),
	UNIQUE (kod_zaw, kod_isced, grupa_isced, podgrupa_isced, nazwa_isced)
);
CREATE TABLE w13a (
	id_kierunku_stu text PRIMARY KEY CHECK (id_kierunku_stu != '')
);
CREATE TABLE w13 (
	id_kierunku_stu text NOT NULL REFERENCES w13a (id_kierunku_stu) ON DELETE CASCADE ON UPDATE CASCADE,
	dziedzina text NOT NULL CHECK (dziedzina != ''),
	dyscyplina text NOT NULL CHECK (dyscyplina != ''),
	czy_dyscyplina_wiodaca boolean NOT NULL,
	PRIMARY KEY (id_kierunku_stu, dziedzina, dyscyplina)
);
CREATE TABLE w30a (
  id_podmiotu text NOT NULL PRIMARY KEY CHECK (id_podmiotu != ''),
	id_uczelni text NOT NULL CHECK (id_uczelni != ''),
	rodzaj_uczelni rodzaj_uczelni NOT NULL
);
CREATE TABLE w30 (
	id_podmiotu text NOT NULL REFERENCES w30a (id_podmiotu) ON DELETE CASCADE ON UPDATE CASCADE,
	rok_akad rok_szk NOT NULL,
	typ_podmiotu typ_podmiotu NOT NULL,
	nazwa_podmiotu text NOT NULL CHECK (nazwa_podmiotu != ''),
	status_podmiotu status_podmiotu NOT NULL,
	kraj_podmiotu text NOT NULL CHECK (kraj_podmiotu != ''),
	teryt_pow_podmiotu integer CHECK ((kraj_podmiotu = 'Polska' AND teryt_pow_podmiotu IS NOT NULL) OR (kraj_podmiotu != 'Polska' AND teryt_pow_podmiotu IS NULL)),
	wojewodztwo_podmiotu text CHECK ((kraj_podmiotu = 'Polska' AND wojewodztwo_podmiotu IS NOT NULL) OR (kraj_podmiotu != 'Polska' AND wojewodztwo_podmiotu IS NULL)),
	powiat_podmiotu text CHECK ((kraj_podmiotu = 'Polska' AND powiat_podmiotu IS NOT NULL) OR (kraj_podmiotu != 'Polska' AND powiat_podmiotu IS NULL)),
	rodzaj_gminy_podmiotu integer CHECK ((kraj_podmiotu = 'Polska' AND rodzaj_gminy_podmiotu IN (1, 2, 4, 5)) OR (kraj_podmiotu != 'Polska' AND rodzaj_gminy_podmiotu IS NULL)),
	ulica text NOT NULL CHECK (ulica != ''),
	pna text NOT NULL CHECK (pna != ''),
	poczta text NOT NULL CHECK (poczta != ''),
	organ_nadzorujacy text,
	organ_nadzorujacy_typ text,
	PRIMARY KEY (id_podmiotu, rok_akad)
);
CREATE TABLE w12 (
	id_abs int,
	rok_abs int,
	id_kierunku_stu text NOT NULL REFERENCES w13a (id_kierunku_stu) ON DELETE CASCADE ON UPDATE CASCADE,
	kierunek_stu text NOT NULL CHECK (kierunek_stu != ''),
	profil_stu profil_stu NOT NULL,
	forma_stu forma_stu NOT NULL,
	id_podmiotu text NOT NULL REFERENCES w30a (id_podmiotu) ON DELETE CASCADE ON UPDATE CASCADE,
	data_od_stu date NOT NULL,
	czy_ukoncz_stu int NOT NULL CHECK (czy_ukoncz_stu IN (0, 1, 2)),
	data_do_stu date CHECK (CASE WHEN czy_ukoncz_stu = 1 THEN data_do_stu IS NOT NULL ELSE data_do_stu IS NULL END),
	tytul_zaw_stu text CHECK (CASE WHEN czy_ukoncz_stu = 1 THEN tytul_zaw_stu IS NOT NULL ELSE tytul_zaw_stu IS NULL END),
	data_skresl_stu date CHECK (CASE WHEN czy_ukoncz_stu = 2 THEN data_skresl_stu IS NOT NULL ELSE data_skresl_stu IS NULL END),
	PRIMARY KEY (id_abs, rok_abs, id_kierunku_stu, data_od_stu),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w14 (
	id_abs int,
	rok_abs int,
	rok_zgonu int NOT NULL,
	mies_zgonu int NOT NULL CHECK (mies_zgonu >=1 AND mies_zgonu <= 12),
	PRIMARY KEY (id_abs, rok_abs),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w15 (
	id_abs int,
	rok_abs int,
	adres_typ adres_typ NOT NULL,
	data_adr_od date NOT NULL,
	lp int NOT NULL,
	data_adr_do date,
	teryt int,
	PRIMARY KEY (id_abs, rok_abs, adres_typ, data_adr_od, lp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w18 (
	id_platnika int PRIMARY KEY,
	pkd text,
	rok_wyrej int,
	mies_wyrej int CHECK (mies_wyrej >=1 AND mies_wyrej <= 12)
);
CREATE TABLE w22 (
	kod_zus int PRIMARY KEY,
	opis text NOT NULL CHECK (opis != ''),
	etat_ibe boolean NOT NULL,
	netat_ibe boolean NOT NULL,
	samoz_ela boolean NOT NULL,
	bezrob_ibe boolean NOT NULL,
	dziecko boolean NOT NULL,
	inne_ibe boolean NOT NULL,
	macierz boolean NOT NULL,
	wychow boolean NOT NULL,
	mlodoc boolean NOT NULL,
	pomoc_spol boolean NOT NULL,
	bezrobotnystaz boolean NOT NULL,
	bierny_skladka boolean NOT NULL,
	wychowawczy_opieka boolean NOT NULL,
	skladka_szac_wynag boolean NOT NULL,
	etat_ela boolean NOT NULL,
	netat_ela boolean NOT NULL,
	zlec_ela boolean NOT NULL,
	bezrob_ela boolean NOT NULL,
	student_ela boolean NOT NULL,
	inne_ela boolean NOT NULL,
	mundur_ela boolean NOT NULL,
	prawnik_ela boolean NOT NULL,
	rolnik_ela boolean NOT NULL,
	zagranic_ela boolean NOT NULL,
	nspraw_ela boolean NOT NULL,
	rentemer_ela boolean NOT NULL,
	macierzynski_ela boolean NOT NULL,
	dziecko_pracownik_ela boolean NOT NULL,
	dziecko_samozatrudnienie_ela boolean NOT NULL,
	dziecko_zlecenie_ela boolean NOT NULL,
	dziecko_bezpracy_ela boolean NOT NULL,
	etatnokid boolean NOT NULL,
	netatnokid_ela boolean NOT NULL,
	samoznokid_ela boolean NOT NULL
);
CREATE TABLE w16 (
	id_abs int,
	rok_abs int,
	rok_skladka int NOT NULL,
	mies_skladka int NOT NULL CHECK (mies_skladka >=1 AND mies_skladka <= 12),
	id_platnika int NOT NULL REFERENCES w18 (id_platnika) ON DELETE CASCADE ON UPDATE CASCADE,
	kod_zus int NOT NULL REFERENCES w22 (kod_zus) ON DELETE CASCADE ON UPDATE CASCADE,
	lp int NOT NULL,
	podst_chor int NOT NULL,
	podst_wypad int NOT NULL,
	podst_emer int NOT NULL,
	podst_zdrow int NOT NULL,
	czy_30 boolean NOT NULL,
	czy_rsa boolean NOT NULL,
	udzial_dni_zatr_mies real CHECK ((udzial_dni_zatr_mies > 0 AND udzial_dni_zatr_mies <= 1) OR udzial_dni_zatr_mies IS NULL),
	wymiar_zatr real CHECK (wymiar_zatr > 0 OR wymiar_zatr IS NULL),
	kod_zaw_praca int CHECK ((kod_zaw_praca > 10000 AND kod_zaw_praca <= 999999) OR udzial_dni_zatr_mies IS NULL),
	emeryt_rencista int NOT NULL CHECK (emeryt_rencista IN (0, 1, 2)),
	niepelnosprawny int NOT NULL CHECK (niepelnosprawny IN (0, 1, 2, 3, 4)),
	PRIMARY KEY (id_abs, rok_abs, rok_skladka, mies_skladka, id_platnika, kod_zus, lp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w23 (
	kod int PRIMARY KEY,
	opis text NOT NULL CHECK (opis != ''),
	bierny_zawodowo boolean NOT NULL,
	dziecko2 boolean NOT NULL,
	wypadek boolean NOT NULL,
	choroba boolean NOT NULL,
	choroba_macierz boolean NOT NULL
);
CREATE TABLE w17 (
	id_abs int,
	rok_abs int,
	id_platnika int NOT NULL REFERENCES w18 (id_platnika) ON DELETE CASCADE ON UPDATE CASCADE,
	data_od_przerwa date NOT NULL,
	kod_przerwy int NOT NULL REFERENCES w23 (kod) ON DELETE CASCADE ON UPDATE CASCADE,
	data_do_przerwa date,
	PRIMARY KEY (id_abs, rok_abs, id_platnika, data_od_przerwa, kod_przerwy, data_do_przerwa),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w31 (
	id_abs int,
	rok_abs int,
	orzeczenie_data_od date NOT NULL,
	orzeczenie_data_do date NOT NULL,
	niepelnosprawnosc text NOT NULL CHECK (niepelnosprawnosc != ''),
	niepelnosprawnosc_sprz2 bool NOT NULL,
	niepelnosprawnosc_sprz3 bool NOT NULL,
	niepelnosprawnosc_sprz4 bool NOT NULL,
	niepelnosprawnosc_sprz5 bool NOT NULL,
	niepelnosprawnosc_sprz6 bool NOT NULL,
	niepelnosprawnosc_sprz17 bool NOT NULL,
	niepelnosprawnosc_sprz18 bool NOT NULL,
	niepelnosprawnosc_sprz20 bool NOT NULL,
	niepelnosprawnosc_sprz21 bool NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, orzeczenie_data_od, niepelnosprawnosc),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w32 (
	id_abs int,
	rok_abs int,
	data_od_ubezp date NOT NULL,
	data_do_ubezp date,
	rola_ubezp int CHECK (rola_ubezp IN (1, 2, 3, 4, 5) OR rola_ubezp IS NULL),
	PRIMARY KEY (id_abs, rok_abs, data_od_ubezp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w33 (
	id_abs int,
	rok_abs int,
	rodzaj_swiadcz int NOT NULL CHECK (rodzaj_swiadcz IN (1, 2, 3, 4)),
	data_od_swiadcz date NOT NULL,
	data_do_swiadcz date NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, data_od_swiadcz, rodzaj_swiadcz),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
```
