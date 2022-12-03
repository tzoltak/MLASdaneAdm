# MLASdaneAdm 0.3.1 (3.12.2022)

## Ulepszona diagnostyka

- `wczytaj_tabele_wejsciowe()`:
  - Sprawdza, czy w danych (`w2` i `w3`, w praktyce raczej w tej drugiej) występują kody typów szkół, dla których brak jest mapowania na nazwę typu szkoły w pliku 'STYPSZK.csv'.
  - Sprawdza, czy w danych o kontynuacji kształcenia w szkołach objętych SIO (`w3`) występują rekordy, które najprawdopodobniej opisują naukę w szkole (i zawodzie), jako absolwent której dana osoba została objeta monitoringiem (wystąpienie takiej sytuacji traktowane jest jako błąd).

# MLASdaneAdm 0.3.0 (29.08.2022)

## Nowe funkcjonalnośi:

- Tworzenie i zapis nowej *tabeli pośredniej* `p5` zawierającej dane o absolwento-miesiąco-pracodawcach, która umożliwia tworzenie wskaźników stałości zatrudnienia.

## Ulepszona diagnostyka

- `wczytaj_tabele_wejsciowe()`:
  - Sprawdza. czy wszystkie kody zawodów są liczbami całkowitymi (jeśli nie, to je usuwa i zmienia typ kolumny, informującym o tym).
  - Przygotowuje zestawienie opisujące kompletność danych o wynikach egzaminów.
  - Sprawdza, czy w danych o dyplomach czeladnika (`w6`) występują powtórzenia.
  - Sprawdza, czy w danych o dyplomach nie ma braków danych w kodach zawodów.

## Naprawione błedy

- `wczytaj_tabele_wejsciowe()`:
  - Zwracając kody tytułów ubezpieczenia i przerw w opłacaniu składek, które nie mają mapowania na kategorie analityczne (odpowiednio w tabelach `w22` i `w23`) wypisuje tylko unikalne kody (zamiast wszystkich wystąpień);
  - Tworząc listę unikalnych kodów zawodów uwzględnia również te występujące w tabeli `w21`.
- `przygotuj_tabele_posrednie()`:
  - Sprawdza w poprawny sposób warunki, bez utworzenia których innych *tablic pośrednich* nie da się przygotować tablicy `p4`.

# MLASdaneAdm 0.2.1 (17.11.2021)

## Naprawione błedy

- Domyślna wartość argumentu `okresyP2` funkcji `przygotuj_tabele_posrednie()` została zmieniona na opisującą grudnie od roku ukończenia szkoły do roku prowadzenia monitoringu (wcześniej okres ten był błędnie przesuniety o 1 rok wstecz).

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
