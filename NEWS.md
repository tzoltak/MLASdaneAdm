# MLASdaneAdm 0.2.0 (3.11.2021)

## Nowy wskaźnik

- W *tabeli pośredniej* `p3` tworzonej przez `przygotuj_tabele_posrednie()` tworzony jest nowy wskaznik `nauka_szk_abs`, który wskazuje, czy w danym miesiącu dana osoba uczyła się jeszcze w szkole, jako absolwent której została objęta monitoringiem lub dany miesiąc zawierał się w okresie pomiędzy ukończeniem tej szkoły a podjęciem dalszej nauki w formie innej niż KKZ lub KUZ, najdalej w październiku roku zostania absolwentem. Odpowiednio zmodyfikowano też funkcję `wczytaj_tabele_posrednie()`.

# MLASdaneAdm 0.1.1 (18.10.2021)

## Sposób obliczania wskaźników

- Przy wczytywaniu danych o składkach z pliku *W16.csv* `wczytaj_tabele_wejsciowe()` agreguje (w praktyce niezbyt liczne) duplikaty (id_abs, rok_abs, rok_skladka, mies_skladka, id_platnika, kod_zus) sumując podstawy składek i wybierając maksimum spośród CZY_RSA, zamiast wybierać jeden rekord z najwyższą podstawą składki (na ubezpieczenie chorobowe). Takie duplikaty opisują osoby, które miały u jednego pracodawcy w tym samym miesiącu kilka różnych umów (w praktyce to bywają umowy bardzo różnych typów).

## Organizaca pakietu

- Zapis *tabel pośrednich* do relacyjnej bazy danych został wydzielony z `przygotuj_tabele_posrednie()` odrębnej funkcji `wczytaj_tabele_posrednie()`.

# MLASdaneAdm 0.1.0 (15.10.2021)

- Pierwsza wersja pakietu. Opis podstawowych funkcjonalności znajduje się w pliku README.md.
