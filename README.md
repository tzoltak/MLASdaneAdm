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

Pakiet udostępnia obecnie trzy podstawowe funkcje:

1.  `przygotuj_dane_do_w19()` pozwala przygotować dane ze wskaźnikami bezrobocia i przecietnych miesięcznych wynagrodzeń w formie, w jakiej są zwracane przez `MLASZdane::pobierz_dane_bdl` do formatu wymaganego dla tabeli *wejściowej* W19 (przykład użycia dostępny w dokumentacji funkcji: `?przygotuj_dane_do_w19`).
2.  `przygotuj_dane_do_w26()` pozwala przygotować, na podstawie eksportu z publicznego API WWW RSPO, zestawienie danych wszystkich szkół (i placówek oświatowych) objętych SIO, które występują w danych z danej edycji monitoringu, w formatu wymaganym dla tabeli *wejściowej* W26.
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

-   Wczytanie danych z monitoringu 2021 r. do działającej lokalnie bazy PostgreSQL 10.1 na komputerze z 16 GB RAM i procesorem Intel i7-4720HQ trwa 35-40 minut (w konfiguracji z 32 GB RAM i procesorem Ryzen 5 5600H dane z monitoringu 2022 r. wczytywały się ~30 minut).

`przygotuj_tabele_posrednie()` wymaga jedynie przekazania danych niezbędnych do połączenia się z bazą (p. następna sekcja). Przygotowane *tabele pośrednie* domyślnie zwraca zarówno jako listę *ramek danych* w środowisku R, z której pobierała dane wejściowe.

-   Dla danych z monitoringu 2021 r., na komputerze z 16 GB RAM i procesorem Intel i7-4720HQ, z wykorzystaniem działającej lokalnie bazy PostgreSQL 10.1 trwa ~20 minut - bez tworzenia *tabeli pośredniej p5* (w konfiguracji z 32 GB RAM i procesorem Ryzen 5 5600H dane z monitoringu 2022 r. przetwarzały się ~18 minut, z tworzeniem *tabeli pośredniej p5*).

Sposób działania obu funkcji - a szczególnie `przygotuj_tabele_posrednie()` - można modyfikować przy pomocy dodatkowych argumentów. Więcej na ten temat można dowiedzieć się w ich dokumentacji:

```{r}
?wczytaj_tabele_wejsciowe()
?przygotuj_tabele_posrednie()
```

`wczytaj_tabele_posrednie()` wymaga przekazania jako argumentu listy zwróconej przez `przygotuj_tabele_posrednie()` oraz danych niezbędnych do połączenia się z bazą (p. następna sekcja).

-   Wczytanie danych z monitoringu 2021 r. do działającej lokalnie bazy PostgreSQL 10.1 na komputerze z 16 GB RAM i procesorem Intel i7-4720HQ trwa ~45 minut - bez *tabeli pośredniej p5* (w konfiguracji z 32 GB RAM i procesorem Ryzen 5 5600H dane z monitoringu 2022 r. wczytywały się ~45 minut, z *tabelą pośrednia p5*).

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

```{sql}
CREATE TABLE w1 (
	id_abs int,
	rok_abs int CHECK (rok_abs >= 2019),
	PRIMARY KEY (id_abs, rok_abs)
);
CREATE TABLE typy_szkol (
	typ_szk text PRIMARY KEY
);
/* unikalne id szkół */
CREATE TABLE w26a (
	id_szk int PRIMARY KEY CHECK (id_szk > 0)
);
/* dane szkół, co do których zakładam, że są stałe w czasie i nie mogą być brakami danych */
CREATE TABLE w26b (
	id_szk int REFERENCES w26a (id_szk),
	typ_szk text NOT NULL REFERENCES typy_szkol (typ_szk),
	publicznosc text NOT NULL,
	kategoria_uczniow kat_uczn NOT NULL,
	specyfika specyfika NOT NULL,
	organ_rejestrujacy_id int NOT NULL,
	organ_rejestrujacy_typ text NOT NULL CHECK (organ_rejestrujacy_typ != ''),
	organ_rejestrujacy_nazwa text NOT NULL CHECK (organ_rejestrujacy_nazwa != ''),
	organ_sposob_ewidencjonowania sposob_ewidencjonowania NOT NULL,
	organ_prowadzacy_typ text NOT NULL CHECK (organ_prowadzacy_typ != ''),
	PRIMARY KEY (id_szk, typ_szk, publicznosc, kategoria_uczniow, specyfika, organ_rejestrujacy_id, organ_rejestrujacy_typ, organ_rejestrujacy_nazwa, organ_sposob_ewidencjonowania, organ_prowadzacy_typ)
);
/* dane wszystkich szkół występujących w danych z RSPO */
CREATE TABLE w26 (
	id_szk int NOT NULL REFERENCES w26a (id_szk),
	rok_szk rok_szk,
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
	organ_rejestrujacy_teryt int,
	organ_prowadzacy_nazwa text NOT NULL CHECK (organ_prowadzacy_nazwa != ''),
	organ_prowadzacy_regon text,
	organ_prowadzacy_teryt int NOT NULL,
	organ_prowadzacy_woj text NOT NULL CHECK (organ_prowadzacy_woj != ''),
	organ_prowadzacy_pow text NOT NULL CHECK (organ_prowadzacy_pow != ''),
	organ_prowadzacy_gmi text NOT NULL CHECK (organ_prowadzacy_gmi != ''),
	miejsce_w_strukt miejsce_w_strukt NOT NULL,
	jedn_nadrz_id int,
	jedn_nadrz_typ text,
	PRIMARY KEY (id_szk, rok_szk),
	UNIQUE (id_szk, rok_szk, nazwa_szk, teryt_gmi_szk, wojewodztwo_szk, powiat_szk, gmina_szk,
            simc_miejsc, miejscowosc, rodzaj_miejsc, sym_ul, ulica, nr_budynku, nr_lokalu, pna, poczta,
            organ_rejestrujacy_teryt, organ_prowadzacy_nazwa, organ_prowadzacy_regon, organ_prowadzacy_teryt,
            organ_prowadzacy_woj, organ_prowadzacy_pow, organ_prowadzacy_gmi, miejsce_w_strukt,
            jedn_nadrz_id, jedn_nadrz_typ)
);
/* unikalne kody zawodow (w tym zawody nie przypisane do żadnej branży) */
CREATE TABLE w20a (
	kod_zaw int PRIMARY KEY
);
/* uwaga! mapowanie zawodów na branże jest 1:n (są tu też stare klasyfikacje) */
CREATE TABLE w20 (
	kod_zaw int REFERENCES w20a (kod_zaw),
	wersja_klasyfikacji int CHECK (wersja_klasyfikacji IN (1, 2, 3, 4)),
	branza text CHECK (branza != ''),
	branza_kod text CHECK (branza_kod != ''),
	PRIMARY KEY (kod_zaw, wersja_klasyfikacji)
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
/* uwaga! zdarzają się osoby kończące w tym samym roku kilka zawodów nawet w jednej szkole (zwykle policealnej) */
CREATE TABLE w2 (
	id_abs int,
	rok_abs int,
	rok_ur int,
	plec plec,
	id_szk int NOT NULL REFERENCES w26a (id_szk),
	typ_szk text NOT NULL REFERENCES typy_szkol (typ_szk),
	teryt_szk int CHECK (teryt_szk >= 201011 AND teryt_szk <= 3299999),
	teryt_pow_szk int NOT NULL REFERENCES w25 (teryt_pow),
	lp int,
	kod_zaw int REFERENCES w20a (kod_zaw),
	nazwa_zaw text,
	PRIMARY KEY (id_abs, rok_abs, id_szk, lp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w3 (
	id_abs int,
	rok_abs int,
	id_szk_kont int NOT NULL REFERENCES w26a (id_szk),
	typ_szk_kont text NOT NULL REFERENCES typy_szkol (typ_szk),
	data_od_szk_kont date NOT NULL,
	lp int,
	czy_ukoncz_szk_kont int CHECK (czy_ukoncz_szk_kont >= 0 AND czy_ukoncz_szk_kont <= 2),
	data_do_szk_kont date, CHECK (CASE WHEN czy_ukoncz_szk_kont != 0 THEN data_do_szk_kont IS NOT NULL ELSE data_do_szk_kont IS NULL END),
	kod_zaw_kont int REFERENCES w20a (kod_zaw),
	PRIMARY KEY (id_abs, rok_abs, id_szk_kont, data_od_szk_kont, lp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
/* uwaga! mapowanie kwalifikacji na zawody jest 1:n */
CREATE TABLE w21a (
	kod_kwal text PRIMARY KEY
);
CREATE TABLE w21 (
	kod_kwal text REFERENCES w21a (kod_kwal),
	kod_zaw int REFERENCES w20a (kod_zaw),
	PRIMARY KEY (kod_kwal, kod_zaw)
);
CREATE TABLE w4 (
	id_abs int,
	rok_abs int,
	id_szk_kont int NOT NULL REFERENCES w26a (id_szk),
	lp int,
	data_od_kkz date NOT NULL,
	data_do_kkz date,
	kod_kwal_kkz text REFERENCES w21a (kod_kwal),
	PRIMARY KEY (id_abs, rok_abs, id_szk_kont, lp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w5 (
	id_abs int,
	rok_abs int,
	id_szk_kont int NOT NULL REFERENCES w26a (id_szk),
	lp int,
	data_od_kuz date NOT NULL,
	data_do_kuz date,
	kod_zaw_kuz int REFERENCES w20a (kod_zaw),
	PRIMARY KEY (id_abs, rok_abs, id_szk_kont, lp),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w6 (
	id_abs int,
	rok_abs int,
	kod_zaw_czel int REFERENCES w20a (kod_zaw),
	PRIMARY KEY (id_abs, rok_abs, kod_zaw_czel),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w7 (
	id_abs int,
	rok_abs int,
	rok_matura int,
	czy_zdana_matura boolean NOT NULL,
	data_swiad_matura date CHECK (CASE WHEN czy_zdana_matura THEN data_swiad_matura IS NOT NULL ELSE TRUE END),
	PRIMARY KEY (id_abs, rok_abs, rok_matura),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w810 (
	id_abs int,
	rok_abs int,
	kod_kwal text REFERENCES w21a (kod_kwal),
	rok_kwal int NOT NULL,
	dok_potw_kwal int NOT NULL CHECK (dok_potw_kwal >= 1 AND dok_potw_kwal <= 2),
	data_kwal date NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, kod_kwal),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w911 (
	id_abs int,
	rok_abs int,
	kod_zaw int REFERENCES w20a (kod_zaw),
	rok_dyp int NOT NULL,
	dok_potw_dyp int NOT NULL CHECK (dok_potw_dyp >= 1 AND dok_potw_dyp <= 2),
	data_dyp_zaw date NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, kod_zaw),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w24 (
	kod_zaw int REFERENCES w20a (kod_zaw),
	kod_isced char(4) NOT NULL,
	grupa_isced text NOT NULL CHECK (grupa_isced != ''),
	podgrupa_isced text NOT NULL CHECK (podgrupa_isced != ''),
	nazwa_isced text NOT NULL CHECK (nazwa_isced != ''),
	PRIMARY KEY (kod_zaw),
	UNIQUE (kod_zaw, kod_isced, grupa_isced, podgrupa_isced, nazwa_isced)
);
CREATE TABLE w13a (
	id_kierunku_stu text PRIMARY KEY
);
CREATE TABLE w13 (
	id_kierunku_stu text REFERENCES w13a (id_kierunku_stu),
	dziedzina text,
	dyscyplina text,
	czy_dyscyplina_wiodaca boolean NOT NULL,
	PRIMARY KEY (id_kierunku_stu, dziedzina, dyscyplina)
);
CREATE TABLE w12 (
	id_abs int,
	rok_abs int,
	id_kierunku_stu text REFERENCES w13a (id_kierunku_stu),
	kierunek_stu text NOT NULL,
	profil_stu profil_stu NOT NULL,
	data_od_stu date NOT NULL,
	czy_ukoncz_stu int NOT NULL CHECK (czy_ukoncz_stu >= 0 AND czy_ukoncz_stu <= 2),
	data_do_stu date CHECK (CASE WHEN czy_ukoncz_stu = 1 THEN data_do_stu IS NOT NULL ELSE data_do_stu IS NULL END),
	tytul_zaw_stu text CHECK (CASE WHEN czy_ukoncz_stu = 1 THEN tytul_zaw_stu IS NOT NULL ELSE tytul_zaw_stu IS NULL END),
	data_skresl_stu date CHECK (CASE WHEN czy_ukoncz_stu = 2 THEN data_skresl_stu IS NOT NULL ELSE data_skresl_stu IS NULL END),
	PRIMARY KEY (id_abs, rok_abs, id_kierunku_stu, data_od_stu),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w14 (
	id_abs int,
	rok_abs int,
	rok_zgonu int,
	mies_zgonu int CHECK (mies_zgonu >=1 AND mies_zgonu <= 12),
	PRIMARY KEY (id_abs, rok_abs),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w15 (
	id_abs int,
	rok_abs int,
	adres_typ adres_typ,
	data_adr_od date,
	lp int,
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
	rok_skladka int,
	mies_skladka int CHECK (mies_skladka >=1 AND mies_skladka <= 12),
	id_platnika int REFERENCES w18 (id_platnika),
	kod_zus int REFERENCES w22 (kod_zus),
	podst_chor int NOT NULL,
	podst_wypad int NOT NULL,
	podst_emer int NOT NULL,
	podst_zdrow int NOT NULL,
	czy_30 boolean NOT NULL,
	czy_rsa boolean NOT NULL,
	emeryt_rencista int NOT NULL CHECK (emeryt_rencista >= 0 AND emeryt_rencista <= 2),
	niepelnosprawny int NOT NULL CHECK (niepelnosprawny >= 0 AND niepelnosprawny <= 4),
	PRIMARY KEY (id_abs, rok_abs, rok_skladka, mies_skladka, id_platnika, kod_zus),
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
	id_platnika int REFERENCES w18 (id_platnika),
	data_od_przerwa date,
	kod_przerwy int REFERENCES w23 (kod),
	data_do_przerwa date,
	PRIMARY KEY (id_abs, rok_abs, id_platnika, data_od_przerwa, kod_przerwy, data_do_przerwa),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE w19 (
	rok int CHECK (rok > 2000),
	miesiac int CHECK (miesiac >=1 AND miesiac <= 12),
	teryt_pow int REFERENCES w25 (teryt_pow),
	stopa_bezrobocia real CHECK (stopa_bezrobocia >= 0),
	sr_wynagrodzenia real CHECK (sr_wynagrodzenia > 0),
	PRIMARY KEY (rok, miesiac, teryt_pow)
);
/* ### tabele "pośrednie" ################################################### */
CREATE TABLE p1 (
	id_abs int,
	rok_abs int,
	rodzaj_dyplomu rodzaj_dyplomu,
	dyplom_szczegoly text CHECK (CASE WHEN rodzaj_dyplomu IN ('certyfikat kwalifikacji', 'dyplom licencjata/inżyniera', 'dyplom magistra/lekarza', 'dyplom oficera') THEN dyplom_szczegoly IS NOT NULL ELSE dyplom_szczegoly IS NULL END),
	rok int CHECK (CASE WHEN rodzaj_dyplomu != 'tytuł czeladnika' THEN rok IS NOT NULL END),
	miesiac int CHECK (CASE WHEN rodzaj_dyplomu != 'tytuł czeladnika' THEN miesiac IS NOT NULL END),
	mies_od_ukoncz int CHECK (CASE WHEN rodzaj_dyplomu != 'tytuł czeladnika' THEN mies_od_ukoncz IS NOT NULL END),
	okres int CHECK (CASE WHEN rodzaj_dyplomu != 'tytuł czeladnika' THEN okres IS NOT NULL END),
	lp_dyplom int,
	kod_zaw int REFERENCES w20a (kod_zaw),
	branza text,
	kod_zaw_dyplom int REFERENCES w20a (kod_zaw),
	branza_dyplom text CHECK (CASE WHEN rodzaj_dyplomu NOT IN ('certyfikat kwalifikacji', 'dyplom zawodowy', 'tytuł czeladnika') THEN branza_dyplom IS NULL END),
	dziedzina text CHECK (CASE WHEN rodzaj_dyplomu NOT IN ('dyplom licencjata/inżyniera', 'dyplom magistra/lekarza', 'dyplom oficera') THEN dziedzina IS NULL END),
	dyscyplina_wiodaca text CHECK (CASE WHEN rodzaj_dyplomu NOT IN ('dyplom licencjata/inżyniera', 'dyplom magistra/lekarza', 'dyplom oficera') THEN dyscyplina_wiodaca IS NULL END),
	PRIMARY KEY (id_abs, rok_abs, rodzaj_dyplomu, lp_dyplom),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE,
	UNIQUE (id_abs, rok_abs, kod_zaw, rodzaj_dyplomu, dyplom_szczegoly, dziedzina, dyscyplina_wiodaca, okres)
);
CREATE TABLE p2 (
	id_abs int,
	rok_abs int,
	rok int NOT NULL,
	miesiac int NOT NULL,
	mies_od_ukoncz int NOT NULL,
	okres int,
	lp_kont int,
	kod_zaw int REFERENCES w20a (kod_zaw),
	branza text,
	id_szk_kont int REFERENCES w26a (id_szk),
	typ_szk_kont text NOT NULL,
	forma_kont forma_kontynuacji NOT NULL,
	teryt_pow_kont int REFERENCES w25 (teryt_pow),
	kod_zaw_kont int REFERENCES w20a (kod_zaw) CHECK (CASE WHEN forma_kont = 'student' THEN kod_zaw_kont IS NULL END),
	branza_kont text CHECK (CASE WHEN forma_kont = 'student' THEN branza_kont IS NULL END),
	dziedzina_kont text CHECK (CASE WHEN forma_kont != 'student' THEN dziedzina_kont IS NULL END),
	dyscyplina_wiodaca_kont text CHECK (CASE WHEN forma_kont != 'student' THEN dyscyplina_wiodaca_kont IS NULL END),
	zrodlo text NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, okres, lp_kont),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE p3 (
	id_abs int,
	rok_abs int,
	rok int NOT NULL,
	miesiac int NOT NULL,
	mies_od_ukoncz int NOT NULL,
	okres int,
	status status NOT NULL,
	zmarl boolean NOT NULL,
	brak_danych_z_zus boolean NOT NULL,
	praca int CHECK (praca in (0, 1, 2, 3, 4, 5, 6, 7)) CHECK (CASE WHEN brak_danych_z_zus THEN praca IS NULL ELSE praca IS NOT NULL END),
	mlodociany int CHECK (mlodociany in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN mlodociany IS NULL ELSE mlodociany IS NOT NULL END),
	kont_mlodoc_prac int CHECK (kont_mlodoc_prac in (1, 2, 3, 4, 5, 6)) CHECK (CASE WHEN brak_danych_z_zus THEN kont_mlodoc_prac IS NULL END),
	bezrobocie int CHECK (bezrobocie in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN bezrobocie IS NULL ELSE bezrobocie IS NOT NULL END),
	bezrobocie_staz int CHECK (bezrobocie_staz in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN bezrobocie_staz IS NULL ELSE bezrobocie_staz IS NOT NULL END),
	dziecko int CHECK (dziecko in (0, 1, 2)) CHECK (CASE WHEN brak_danych_z_zus THEN dziecko IS NULL ELSE dziecko IS NOT NULL END),
	macierz int CHECK (macierz in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN macierz IS NULL ELSE macierz IS NOT NULL END),
	wychow int CHECK (wychow in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN wychow IS NULL ELSE wychow IS NOT NULL END),
	pomoc_spol int CHECK (pomoc_spol in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN pomoc_spol IS NULL ELSE pomoc_spol IS NOT NULL END),
	emeryt_rencista int CHECK (emeryt_rencista in (0, 1, 2)) CHECK (CASE WHEN brak_danych_z_zus THEN emeryt_rencista IS NULL END),
	niepelnosprawny int CHECK (niepelnosprawny in (0, 1, 2, 3, 4)) CHECK (CASE WHEN brak_danych_z_zus THEN niepelnosprawny IS NULL END),
	biernosc_zus int CHECK (biernosc_zus in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN biernosc_zus IS NULL ELSE biernosc_zus IS NOT NULL END),
	wypadek int CHECK (wypadek in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN wypadek IS NULL ELSE wypadek IS NOT NULL END),
	choroba int CHECK (choroba in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN choroba IS NULL ELSE choroba IS NOT NULL END),
	choroba_macierz int CHECK (choroba_macierz in (0, 1)) CHECK (CASE WHEN brak_danych_z_zus THEN choroba_macierz IS NULL ELSE choroba_macierz IS NOT NULL END),
	wynagrodzenie real CHECK (CASE WHEN brak_danych_z_zus THEN wynagrodzenie IS NULL END),
	wynagrodzenie_uop real CHECK (CASE WHEN praca IN (1, 4, 5, 7) THEN wynagrodzenie_uop IS NOT NULL ELSE wynagrodzenie_uop IS NULL END),
	teryt_zam int REFERENCES w25 (teryt_pow),
	powiat_bezrobocie real,
	powiat_sr_wynagrodzenie real,
	nauka int NOT NULL CHECK (nauka in (0, 1)),
	nauka2 int NOT NULL CHECK (nauka2 in (0, 1)),
	nauka_szk_abs int NOT NULL CHECK (nauka_szk_abs in (0, 1)),
	nauka_bs1st int NOT NULL CHECK (nauka_bs1st in (0, 1, 2)),
	nauka_bs2st int NOT NULL CHECK (nauka_bs2st in (0, 1, 2)),
	nauka_technikum int NOT NULL CHECK (nauka_technikum in (0, 1, 2)),
	nauka_lo int NOT NULL CHECK (nauka_lo in (0, 1, 2, 3)),
	nauka_spolic int NOT NULL CHECK (nauka_spolic in (0, 1, 2, 3, 4)),
	nauka_artystyczna int NOT NULL CHECK (nauka_artystyczna in (0, 1, 2, 3, 4, 5, 6, 7, 8)),
	nauka_sspdp int NOT NULL CHECK (nauka_sspdp in (0, 1)),
	nauka_kpsp int NOT NULL CHECK (nauka_kpsp in (0, 1)),
	nauka_studia int NOT NULL CHECK (nauka_studia in (0, 1)),
	nauka_kkz int NOT NULL CHECK (nauka_kkz in (0, 1)),
	nauka_kuz int NOT NULL CHECK (nauka_kuz in (0, 1)),
	PRIMARY KEY (id_abs, rok_abs, okres),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE p4 (
	id_abs int,
	rok_abs int,
	rok_ur int,
	plec plec,
	id_szk int,
	duplikat_w_szkole boolean NOT NULL,
	duplikat_wiele_szkol boolean NOT NULL,
	mlodoc_byl boolean NOT NULL,
	typ_szk typ_szk NOT NULL,
	szk_specjalna boolean NOT NULL,
	typ_szk_mlodoc typ_szk_mlodoc NOT NULL,
	teryt_pow_szk int NOT NULL,
	nazwa_pow_szk text NOT NULL,
	teryt_woj_szk int NOT NULL,
	nazwa_woj_szk text NOT NULL,
	nazwa_makroreg_szk text NOT NULL,
	nazwa_reg_szk text NOT NULL,
	nazwa_podreg_szk text NOT NULL,
	nts_podreg_szk int NOT NULL,
	lp int,
	kod_zaw int REFERENCES w20a (kod_zaw),
	nazwa_zaw text,
	branza text,
	kod_isced char(4),
	grupa_isced text,
	podgrupa_isced text,
	nazwa_isced text,
	l_prac_ucz_uop int NOT NULL,
	l_prac_nucz_uop int NOT NULL,
	l_prac_nucz_nuop int NOT NULL,
	zawod_sr_wynagrodzenie real,
	abs_w_cke boolean NOT NULL,
	abs_w_sio boolean NOT NULL,
	abs_w_polon boolean NOT NULL,
	abs_w_zus boolean NOT NULL,
	PRIMARY KEY (id_abs, rok_abs, id_szk, lp),
	FOREIGN KEY (id_abs, rok_abs, id_szk, lp) REFERENCES w2 (id_abs, rok_abs, id_szk, lp) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (teryt_pow_szk, nazwa_pow_szk, nazwa_woj_szk, nazwa_makroreg_szk, nazwa_reg_szk, nazwa_podreg_szk, nts_podreg_szk) REFERENCES w25 (teryt_pow, powiat, wojewodztwo, makroregion, region, podregion, nts) ON UPDATE CASCADE,
	FOREIGN KEY (kod_zaw, kod_isced, grupa_isced, podgrupa_isced, nazwa_isced) REFERENCES w24 (kod_zaw, kod_isced, grupa_isced, podgrupa_isced, nazwa_isced) ON UPDATE CASCADE,
	UNIQUE (id_abs, rok_abs, id_szk, kod_zaw)
);
CREATE TABLE p5 (
	id_abs int,
	rok_abs int,
	rok int NOT NULL,
	miesiac int NOT NULL,
	mies_od_ukoncz int NOT NULL,
	okres int,
	lp_pracod int,
	pkd_pracod text,
	forma_zatrudnienia int NOT NULL CHECK (forma_zatrudnienia in (1, 2, 3)),
	mlodociany int NOT NULL CHECK (mlodociany in (0, 1)),
	wynagrodzenie real,
	wynagrodzenie_uop real,
	PRIMARY KEY (id_abs, rok_abs, okres, lp_pracod),
	FOREIGN KEY (id_abs, rok_abs) REFERENCES w1 (id_abs, rok_abs) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE p6 (
	id_szk int NOT NULL REFERENCES w26a (id_szk),
	szk_ma_abs boolean NOT NULL,
	typ_szk text NOT NULL,
	typ_szk_sdd text NOT NULL,
	typ_szk_rspo text NOT NULL REFERENCES typy_szkol (typ_szk),
	publicznosc text NOT NULL,
	kategoria_uczniow kat_uczn NOT NULL,
	specyfika specyfika NOT NULL,
	organ_rejestrujacy_id int NOT NULL,
	organ_rejestrujacy_typ text NOT NULL CHECK (organ_rejestrujacy_typ != ''),
	organ_rejestrujacy_nazwa text NOT NULL CHECK (organ_rejestrujacy_nazwa != ''),
	organ_rejestrujacy_teryt int,
	organ_sposob_ewidencjonowania sposob_ewidencjonowania NOT NULL,
	rok_szk rok_szk,
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
	organ_prowadzacy_typ text NOT NULL CHECK (organ_prowadzacy_typ != ''),
	organ_prowadzacy_nazwa text NOT NULL CHECK (organ_prowadzacy_nazwa != ''),
	organ_prowadzacy_regon text,
	organ_prowadzacy_teryt int NOT NULL,
	organ_prowadzacy_woj text NOT NULL CHECK (organ_prowadzacy_woj != ''),
	organ_prowadzacy_pow text NOT NULL CHECK (organ_prowadzacy_pow != ''),
	organ_prowadzacy_gmi text NOT NULL CHECK (organ_prowadzacy_gmi != ''),
	miejsce_w_strukt miejsce_w_strukt NOT NULL,
	jedn_nadrz_id int,
	jedn_nadrz_typ text,
	PRIMARY KEY (id_szk, rok_szk),
	FOREIGN KEY (id_szk, typ_szk_rspo, publicznosc, kategoria_uczniow, specyfika, organ_rejestrujacy_id, organ_rejestrujacy_typ, organ_rejestrujacy_nazwa,
				 organ_sposob_ewidencjonowania, organ_prowadzacy_typ)
	             REFERENCES w26b (id_szk, typ_szk, publicznosc, kategoria_uczniow, specyfika, organ_rejestrujacy_id, organ_rejestrujacy_typ,
								  organ_rejestrujacy_nazwa, organ_sposob_ewidencjonowania,
                   				  organ_prowadzacy_typ) ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (id_szk, rok_szk, nazwa_szk, teryt_gmi_szk, wojewodztwo_szk, powiat_szk, gmina_szk,
                 simc_miejsc, miejscowosc, rodzaj_miejsc, sym_ul, ulica, nr_budynku, nr_lokalu, pna, poczta,
                 organ_rejestrujacy_teryt, organ_prowadzacy_nazwa, organ_prowadzacy_regon, organ_prowadzacy_teryt,
                 organ_prowadzacy_woj, organ_prowadzacy_pow, organ_prowadzacy_gmi, miejsce_w_strukt,
                 jedn_nadrz_id, jedn_nadrz_typ)
				 REFERENCES w26 (id_szk, rok_szk, nazwa_szk, teryt_gmi_szk, wojewodztwo_szk, powiat_szk, gmina_szk,
                                 simc_miejsc, miejscowosc, rodzaj_miejsc, sym_ul, ulica, nr_budynku, nr_lokalu, pna, poczta,
                                 organ_rejestrujacy_teryt, organ_prowadzacy_nazwa, organ_prowadzacy_regon, organ_prowadzacy_teryt,
                                 organ_prowadzacy_woj, organ_prowadzacy_pow, organ_prowadzacy_gmi, miejsce_w_strukt,
                                 jedn_nadrz_id, jedn_nadrz_typ) ON DELETE CASCADE ON UPDATE CASCADE
);
```
