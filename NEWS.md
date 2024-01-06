# MLASdaneAdm 0.4.1 (6.01.2024)

## Naprawione błędy

-   Funkcja `przygotuj_tabele_posrednie()` poprawnie wypełnia wartości zmiennej `nauka_szk_abs` w sytuacji, gdy mają one wskazywać na kontynuowanie nauki w okresie wakacji pomiędzy (założonym) ukończeniem szkoły, jako absolwent której ktoś został objęty monitoringiem a podjęciem nauki bezpośrednio z początkiem kolejnego roku szkolnego (wcześniej służący temu kod nie działał, bo był uruchamiany zanim w zmiennej `nauka2` braki danych zostały zastąpione przez 0).
-   Funkcja `przygotuj_tabele_posrednie()` konwertuje nietypowe formaty zmiennych zwrócone przez interfejs PostgreSQLa (*pq_plec* dla płci i *integer64*) na typowe formaty R, co pozwala uniknąć problemów przy ich późniejszym wykorzystaniu.

# MLASdaneAdm 0.4.0 (27.09.2023)

## Nowe funkcjonalności

-   Funkcja `wczytaj_tabele_wejsciowe()` otrzymała nowy argument `usunAbsWKilkuZaw`, pozwalający określić, czy absolwenci, którzy ukończyli kilka różnych zawodów w ramach jednej szkoły, niebędącej szkołą policealną, mają zostać usunięci z analizy.
    -   **Domyślnie przyjmuje on wartość `TRUE`, co oznacza zachowanie odmienne, niż we wcześniejszych wersjach pakietu.** Ręczne ustawienie jego wartości na `FALSE` umożliwia uzyskanie wcześniej stosowanego podejścia.
-   Funkcja `wczytaj_tabele_wejsciowe()` otrzymała nowy argument `plikLogu`, pozwalający zapisać strumień błędów generowanych podczas działania funkcji do pliku o podanej nazwie.
    -   Ponieważ strumień błędów obejmuje również wywołania `message()`, do wskazanego pliku zostaną zapisane wszystkie powiadomienia o nietypowych wartościach występujących w danych, jakie wygeneruje funkcja `wczytaj_tabele_wejsciowe()`. Należy przy tym pamiętać, że ze względów technicznych (p. `?sink`) w przypadku korzystania z zapisu do pliku z logiem, powiadomienia te nie zostaną wyświetlone w konsoli R (tak samo, jak nie zostaną na niej wyświetlone ew. komunikaty o błędach!).

## Zmiany w definicjach wskaźników

-   Nowa tabela wejściowa `w24` przechowuje mapowanie kodów zawodów na kategorie klasyfikacji ISCED-F.
    -   Wartości zapisane w tabeli są przyłączane do tabeli pośredniej `p4`, tworząc w niej kolumny: `kod_isced`, `grupa_isced`, `podgrupa_isced` i `nazwa_isced`.
-   Nowa tabela wejściowa `w25` przechowuje mapowania kodów TERYT powiatów na nawzy powiatów, województw i jednostek podziału statystycznego.
    -   Wartości zapisane w tabeli są przyłączane do tabeli pośredniej `p4`, tworząc w niej kolumny: `nazwa_pow_szk`, `nazwa_woj_szk`, `nazwa_makroreg_szk`, `nazwa_reg_szk`, `nazwa_podreg_szk` i `nts_podreg_szk`.
-   Nowe kolumny w tabeli `w16` i odpowiadające im kolumny w tabeli pośredniej `p3`: `emeryt_rencista` i `niepelnosprawny`, których wartości przypisywane są na podstawie dwóch ostatnich cyfr kodu składki ZUS. W konsekwencji zmianie uległa definicja rekordu w tabeli `w16` - jest on teraz określany również przez kombinację wartości tych dwóch zmiennych (wcześniej dane z rekordów o tym samym "przyciętym o dwie ostatnie cyfry" kodzie składki były agregowane przed zapisaniem do `w16`, a informacje dot. emerytury/renty i niepełnosprawności były pomijane).
-   Nowe kolumny w tabeli `w22` i odpowiadające im kolumny w tabeli pośredniej `p3`: `pomoc_spol`, `macierz` i `wychow`.
-   Nowe kolumny w tabeli `w23` i odpowiadające im kolumny w tabeli pośredniej `p3`: `dziecko2`, `wypadek`, `choroba`, `choroba_macierz`.

## Ulepszona diagnostyka

-   `wczytaj_tabele_wejsciowe()`:
    -   Wykrywa rekordy występujące w `W1.csv`, ale nie w `W2.csv`.
    -   Sprawdza, czy wszystkie TERYTy powiatów utworzone na podstawie TERYTów szkół z `W1.csv` występują w `W25.csv`.
    -   Sprawdza, czy wysępują absolwenci więcej niż jednego zawodu w ramach tej samej szkoły, nie będącej szkołą policealną (i domyślnie ich usuwa, choć można to zachowanie zmienić nowym argumentem `usunAbsWKilkuZaw`).
    -   Sprawdza, czy w `W20.csv` (w połączeniu z `W20aneks.csv`) występują zawody przypisane do więcej niż jednej branży.
-   `przygotuj_tabele_posrednie()`:
    -   Sprawdza, czy podany `rokMonitoringu` wydaje się wiarygodny w kontekście przetwarzanych danych i ostrzega, jeśli stwierdzi, że nie jest.

## Naprawione błędy

-   Przy wczytywaniu tabel wejściowych rekordy z ewidetnie zduplikowanym `id_abs` są usuwane (wcześniej były oznaczane przypisaniem unikalnych, ujemnych wartości `id_abs`, które nie łączyły się z danymi w innych tabelach, ale nie były usuwane i przechodziły do tabel pośrednich w formie absolwentów, o których nic nie było wiadomo).
-   Przy tworzeniu tabeli pośredniej `p1`, zgodnie z pierwotną intencją, nie pojawiają się duplikaty dyplomów, jeśli dana kwalifikacja jest przypisana do kilku zawodów lub branż (wcześniej przypisanie do zawodu lub branży było dla wszystkich takich rekordów albo ustawiane na zgodne z zawodem/branżą zawodu, w którym uczył się absolwent, jeśli takowy wśród nich występował, lub na brak danych w przeciwnym przypadku, ale pozostawiano wszystkie kopie, które po takiej zmianie były ze sobą tożsame).
-   Przy tworzeniu tabeli pośredniej `p3` uczniowie policealnych szkół muzycznych są klasyfikowani jako uczący się w szkołach policealnych (wcześniej warunek pomijał ten typ szkoły).
-   Przy tworzenniu tabeli pośredniej `p3` prawidłowo obsługiwana jest sytuacja, kiedy dana osoba uczyła się w szkole (kontynuując kształcenie) tak krótko, że po skorygowaniu dat rozpoczęcia i zakończenia nauki o `minDniEdukacjiWMiesiacu` data ukończenia szkoły była wcześniejsza, niż data jej rozpoczęcia (wcześniej w takiej sytuacji generowany był nieprawidłowy rekord wskazujące na naukę).
-   Przy tworzeniu wskaźnika `nauka_szk_abs` w tabeli pośredniej `p3` funkcja `przygotuj_tabele_posrednie()` potrafi obsłużyć sytuację, gdy w danych nie występuje okres odpowiadający październikowi roku zostania absolwentem.
    -   Co nie zmienia faktu, że sytuacja taka wskazuje albo na krytyczne braki w przetwarzanych danych, albo na podanie złej wartości argumentu `rokMonitoringu` w wywołaniu `przygotuj_tabele_posrednie()`, w związku z czym użytkownik otrzyma teraz odpowiednie ostrzeżenie.

## Inne zmiany

-   Usunięto z kodu wywołania `summarise()` zwracające więcej niż jeden wiersz na grupę (w związku ze zmianami w API pakietu *dplyr*). Przy okazji przepisywania kodu nieco przyspieszono działanie funkcji `wczytaj_tabele_wejsciowe()` i `przygotuj_tabele_posrednie()`.
-   Email autora pakietu zmieniono na aktualny.

# MLASdaneAdm 0.3.2 (12.04.2023)

## Zmiany w definicjach wskaźników

-   W tabeli `w22` tworzone są dwie nowe kolumny `bezrob_ibe` i `inne_ibe`, a ich wartości wykorzystywane są przy tworzeniu wskaźników w *tabelach pośrednich* zamiast dotychczas używanych `bezrob_ela` i `inne_ela`. Nowe kolumny bardziej adekwatnie opisują sytuacje w kontekście monitoringu losów absolwentów szkół ponadpodstawowych związane z pewnymi specyficznymi kodami ZUS.
-   Wiersze w danych wejściowych do tablicy `w16` zawierające kod ZUS 90000 są już obsługiwane w normalny sposób przez mapowanie tego kodu w `w22`, zamiast wykluczane z analizy.

# MLASdaneAdm 0.3.1 (3.12.2022)

## Ulepszona diagnostyka

-   `wczytaj_tabele_wejsciowe()`:
    -   Sprawdza, czy w danych (`w2` i `w3`, w praktyce raczej w tej drugiej) występują kody typów szkół, dla których brak jest mapowania na nazwę typu szkoły w pliku 'STYPSZK.csv'.
    -   Sprawdza, czy w danych o kontynuacji kształcenia w szkołach objętych SIO (`w3`) występują rekordy, które najprawdopodobniej opisują naukę w szkole (i zawodzie), jako absolwent której dana osoba została objęta monitoringiem (wystąpienie takiej sytuacji traktowane jest jako błąd).

# MLASdaneAdm 0.3.0 (29.08.2022)

## Nowe funkcjonalnośi:

-   Tworzenie i zapis nowej *tabeli pośredniej* `p5` zawierającej dane o absolwento-miesiąco-pracodawcach, która umożliwia tworzenie wskaźników stałości zatrudnienia.

## Ulepszona diagnostyka

-   `wczytaj_tabele_wejsciowe()`:
    -   Sprawdza. czy wszystkie kody zawodów są liczbami całkowitymi (jeśli nie, to je usuwa i zmienia typ kolumny, informującym o tym).
    -   Przygotowuje zestawienie opisujące kompletność danych o wynikach egzaminów.
    -   Sprawdza, czy w danych o dyplomach czeladnika (`w6`) występują powtórzenia.
    -   Sprawdza, czy w danych o dyplomach nie ma braków danych w kodach zawodów.

## Naprawione błedy

-   `wczytaj_tabele_wejsciowe()`:
    -   Zwracając kody tytułów ubezpieczenia i przerw w opłacaniu składek, które nie mają mapowania na kategorie analityczne (odpowiednio w tabelach `w22` i `w23`) wypisuje tylko unikalne kody (zamiast wszystkich wystąpień);
    -   Tworząc listę unikalnych kodów zawodów uwzględnia również te występujące w tabeli `w21`.
-   `przygotuj_tabele_posrednie()`:
    -   Sprawdza w poprawny sposób warunki, bez utworzenia których innych *tablic pośrednich* nie da się przygotować tablicy `p4`.

# MLASdaneAdm 0.2.1 (17.11.2021)

## Naprawione błedy

-   Domyślna wartość argumentu `okresyP2` funkcji `przygotuj_tabele_posrednie()` została zmieniona na opisującą grudnie od roku ukończenia szkoły do roku prowadzenia monitoringu (wcześniej okres ten był błędnie przesunięty o 1 rok wstecz).

# MLASdaneAdm 0.2.0 (3.11.2021)

## Nowy wskaźnik

-   W *tabeli pośredniej* `p3` tworzonej przez `przygotuj_tabele_posrednie()` tworzony jest nowy wskaźnik `nauka_szk_abs`, który wskazuje, czy w danym miesiącu dana osoba uczyła się jeszcze w szkole, jako absolwent której została objęta monitoringiem lub dany miesiąc zawierał się w okresie pomiędzy ukończeniem tej szkoły a podjęciem dalszej nauki w formie innej niż KKZ lub KUZ, najdalej w październiku roku zostania absolwentem. Odpowiednio zmodyfikowano też funkcję `wczytaj_tabele_posrednie()`.

# MLASdaneAdm 0.1.1 (18.10.2021)

## Sposób obliczania wskaźników

-   Przy wczytywaniu danych o składkach z pliku *W16.csv* `wczytaj_tabele_wejsciowe()` agreguje (w praktyce niezbyt liczne) duplikaty (id_abs, rok_abs, rok_skladka, mies_skladka, id_platnika, kod_zus) sumując podstawy składek i wybierając maksimum spośród CZY_RSA, zamiast wybierać jeden rekord z najwyższą podstawą składki (na ubezpieczenie chorobowe). Takie duplikaty opisują osoby, które miały u jednego pracodawcy w tym samym miesiącu kilka różnych umów (w praktyce to bywają umowy bardzo różnych typów).

## Organizaca pakietu

-   Zapis *tabel pośrednich* do relacyjnej bazy danych został wydzielony z `przygotuj_tabele_posrednie()` odrębnej funkcji `wczytaj_tabele_posrednie()`.

# MLASdaneAdm 0.1.0 (15.10.2021)

-   Pierwsza wersja pakietu. Opis podstawowych funkcjonalności znajduje się w pliku README.md.
