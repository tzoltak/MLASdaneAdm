# MLASdaneAdm 1.4.1 (17.11.2025)

## Naprawione błędy

-   Podniesiono zależność od pakietu *LOSYwskazniki* do wersji 0.1.3, w związku z czym prawidłowo określana jest liczba kontynuujących naukę na KKZ i KUZ we wskaźnikach dot. kontynuacji nauki w p4 (zamiast błędnego przypisywania zer).

# MLASdaneAdm 1.4.0 (14.11.2025)

## Zmiany w w strukturze tabel *wejściowych*

-   W pliku 'W26.csv' oczekuje się wystąpienia nowych kolumn: `ORGAN_REJESTRUJACY_ID` (zamiast `ORGAN_PROWADZACY_ID`), `ORGAN_REJESTRUJACY_TYP`, `ORGAN_REJESTRUJACY_NAZWA`, `ORGAN_REJESTRUJACY_TERYT` i `ORGAN_SPOSOB_EWIDENCJONOWANIA` (zamiast `ORGAN_PRWADZACY_SPOSOB`).
    -   Odpowiednio zmodyfikowano działanie funkcji `przygotuj_dane_do_w26()`, która domyślnie oczekuje teraz innej nazwy pliku zawierającego dane wyeksportowane z hurtowni SIO: 'id-organow-rejestrujacych.csv' (zamiast 'id-organow-prowadzacych.csv').
    -   Odpowiednio zmodyfikowano funkcje `tabele_wejsciowe()`.

## Zmiany w tworzeniu tabel *pośrednich*

-   W tabeli *pośredniej* p6 pojawiły się nowe kolumny, analogicznie do zmian w pliku wejściowym 'w26.csv':
    -  `organ_rejestrujacy_id` - zmiana nazwy z `organ_prowadzacy_id`, opisuje *idPodmiotu* SIO organu rejestrującego (co robiła też wcześniej, ale teraz zgodnie ze swoją nazwą);
	  -  `organ_rejestrujacy_typ` - typ organu rejestrującego (może to być JST lub ministerstwo);
	  -  `organ_rejestrujacy_nazwa` - nazwa organu rejestrującego;
	  -  `organ_rejestrujacy_teryt` - TERYT organu rejestrującego (6/7-mio cyfrowy, może być brakiem danych) - w zależności od typu organu rejestrującego będzie to TERYT gminy (z cyfrą kodującą rodzaj gminy), powiatu albo województwa, a w przypadku ministerstw brak danych;
	  -  `organ_sposob_ewidencjonowania` - zmiana nazwy z `organ_prowadzacy_sposob`, opisuje, czy organ prowadzący jest tożsamy z rejestrującym ("Prowadzona"), czy też nie ("Rejestrowana").

## Usprawnienia

-    `wczytaj_tabele_wejsciowe()` otrzymała nowy argument `nieWczytujDanychZUS` (domyślnie `FALSE`), który pozwala pominąć wczytywanie danych z ZUS do bazy i tym samym wydatnie skrócić ten proces, jeśli wiadomo, że nie będą one potrzebne, bo proces przeprowadzany jest w celu poprawienia czegoś (niedotyczącego danych z ZUS) w już wcześniej przygotowanych tabelach *pośrednich*.

# MLASdaneAdm 1.3.0 (03.11.2025)

## Nowe funkcje

-   'przytnij_wynagrodzenia()` - p. niżej.

## Zmiany w tworzeniu tabel *pośrednich*

-   Wartości zmiennych `wynagrodzenie` i `wynagrodzenie_uop` w tabeli *pośredniej* P3 są przycinane z wykorzystaniem nowo dodanej funkcji `przytnij_wynagrodzenia()`:
    -   wartości mniejsze niż kwantyl 0.005 i większe niż kwantyl 0.995 zmiennej `wynagrodzenie` zastępowane są wartościami odpowiedniego z tych kwantyli.\;
    -   wartości kwantyli obliczane są oddzielnie dla każdej z grup powstałych przez skrzyżowanie wartości zmiennych `rok_abs` i `rok`;
    -   przy obliczaniu kwantyli pomijane są wartości zmiennej `wynagrodzenie` równe 0;
    -   do przycięcia zmiennej `wynagrodzenie_uop` wykorzystuje się te same progi, obliczone (w danej grupie) w odniesieniu do zmiennej `wynagrodzenie`;
-   Ww. przycinanie zmiennych `wynagrodzenie` i `wynagrodzenie_uop` wtórnie oddziałuje również na obliczone wartości wskaźników opisujących wynagrodzenia w tabeli *pośredniej* P4.

# MLASdaneAdm 1.2.2 (14.10.2025)

## Zmiany w tworzeniu tabel *pośrednich*

-   Podniesiono zależność od pakietu *LOSYwskazniki* do wersji 0.1.2, aby w tabeli *pośredniej* P4 dodawany był również wskaźnik opisujący wynagrodzenia w I kwartale roku następującego po roku ukończenia szkoły oraz wskaźniki opisujące wynagrodzenia z umów o pracę od września do grudnia roku ukończenia szkoły w podziale na miesiące kontynuacji i nie kontynuowania nauki.

## Naprawione błędy

-   Poprawiono sposób obliczania zmiennej `kont_mlodoc_prac` przez `przygotuj_tabele_posrednie()`._

# MLASdaneAdm 1.2.1 (24.08.2025)

## Naprawione błędy

-   Poprawiono zamienione miejscami progi dla dyplomów i świadectw w diagnostyce danych o certyfikatach i dyplomach zawodowych we `wczytaj_tabele_wejsciowe()`.

# MLASdaneAdm 1.2.0 (08.08.2025)

## Ulepszona diagnostyka

-   `wczytaj_tabele_wejsciowe()`:
    -   sprawdza, czy w plikach z tabelami wejściowymi (innych niż 'W1.csv') nie występują kombinacje (`ID_ABS`, `ROK_ABS`), których brakuje w 'W1.csv' (a jeśli występują, to zwraca błąd),
    -   sprawdza, czy daty w plikach z tabelami wejściowymi 'W7.csv', 'W8.csv', 'W9.csv', 'W10.csv' i 'W11.csv') nie ma braków danych w kolumnach z datami (już po próbie konwersji z ciągów znaków na daty) oraz czy nie ma w nich dat wskazują na przyszłość (a jeśli tak, rzuca ostrzeżeniem, żeby nie blokować możliwości przeprowadzenia dalszej diagnostyki),
    -   sprawdza, czy kolumny z datami w pliku z tabelami wejściowymi 'W12.csv' nie są brakami danych - odpowiednio do wartości kolumny `CZY_UKONCZ_STU` - oraz nie ma w nich dat wskazują na przyszłość (a jeśli tak, rzuca ostrzeżeniem, żeby nie blokować możliwości przeprowadzenia dalszej diagnostyki),
    -   sprawdza, czy w plikach z tabelami wejściowymi nie brakuje danych dot. niektórych roczników (które występują w innych plikach wejściowych),
    -   jest nieco bardziej liberalna w stosunku do absolwentów BS I, a mniej w stosunku do absolwentów innych typów szkół, jeśli chodzi o kryteria klasyfikowania odsetka dołączających się certyfikatów kwalifikacji i dyplomów zawodowych jako podejrzanie niskich,
    -   komunikuje brakujące kody składek i przerw w opłacaniu składek ZUS przed próbą zapisu do bazy (która w sytuacji takiego braku nie może się powieść) i robi to teraz w formie ostrzeżenia.
-   Nowa funkcja `porownaj_liczbe_wierszy_w_tabelach_wejsciowych()` pozwala (dosyć) szybko i wygodnie sprawdzić, czy jakieś pliki z tabelami *wejściowymi* nie są podejrzanie małe w porównaniu do tych z poprzednich edycji.

## Ulepszony import tabel wejściowych

-   `wczytaj_tabele_wejsciowe()` jest liberalniejsze względem formatów dat w plikach wejściowych i akceptuje zarówno "%d-%m-%Y" jak i "%d/%m/%Y".
-   `wczytaj_tabele_wejsciowe()` (w końcu) potrafi działać bez przekazania jej połączenia z bazą danych, jeśli tylko ustawiono wartość argumentu `wczytajDoBazy` na `FALSE`.

## Zmiany w tworzeniu tabel *pośrednich*

-   `przygotuj_tabele_posrednie()` używa funkcji pakietu *LOSYwskazniki* (który został w ten sposób nową zależnością) do uzupełnienia tabeli *pośredniej* P4 o zestaw wszystkich wskaźników, których (agregatów) używamy w obecnie publikowanych raportach z monitoringu.
-   `przygotuj_tabele_posrednie()` przygotowując P2 bierze pod uwagę wszystkie typy szkół z W3 (zgodnie z dokumentacją), a nie tylko te kształcące zawodowo.
-   `przygotuj_tabele_posrednie()` przygotowując P2 zwraca wartość `typ_szk_kont` dla studiów jako "Studia", zamiast "studia", dla zachowania zgodności konwencji pisowni z typami szkół objętymi SIO.

# MLASdaneAdm 1.1.0 (06.08.2025)

## Nowe funkcje

-   `przygotuj_dane_do_w26()` automatyzuje proces przygotowywania tabeli *wejściowej* W26 na podstawie danych wyeksportowanych z publicznego API WWW RSPO oraz danych o *idPodmiotu* SIO organów prowadzących wyeksportowane z hurtowni SIO.
    -   Fragment kodu odpowiedzialny za zidentyfikowanie w danych monitoringu unikalnych szkoło-lat szkolnych został wydzielony do nowej, nieeksportowanej funkcji `zidentyfikuj_szkolo_lata_w_danych()`, gdyż jest on wykorzystywany również w ramach `wczytaj_tabele_wejsciowe()`.

# MLASdaneAdm 1.0.1 (23.07.2025)

## Naprawione błędy

-   `przygotuj_tabele_posrednie()` konwertuje kolumny `l_prac_(...)` w P4 z formatu *int64* (nieobsługiwanego poprawnie bez załadowania biblioteki DBI) na *integer* oraz, co ma mniejszą wagę, usuwa z kilku zmiennych (tekstowyych) w P6 typy specyficzne typy danych (enumerujące w bazie dopuszczalne wartości) pobrane z bazy.

# MLASdaneAdm 1.0.0 (27.06.2025)

## Nowa tabela wejściowa

-   W26, zawierająca dane szkół z RSPO, ale także idPodmiot organu prowadzącego z SIO.

## Ulepszony import tabel wejściowych

-   `wczytaj_tabele_wejsciowe()` zamienia twarde spacje w nazwach zawodów na zwykłe i usuwa spacje na końcu nazw zawodów.

## Zmiany w tworzeniu tabel *pośrednich*

-   Wartości domyślne parametrów `przygotuj_tabele_posrednie()`:
    -   rekordy w P3 są domyślnie tworzone tylko do marca roku prowadzenia monitoringu (w pierwszych edycjach ZUS wyrabiał się z zapewnieniem kompletnych danych do maja, ale obecnie już tylko do marca),
    -   rekordy w P2 są domyślnie tworzone zarówno dla grudni jak i dla czerwców (wcześniej tylko dla grudni).
-   Rozszerzono zestaw zmiennych opisujących daty (miesiące) w P1, P2, P3 i P5 o zmienne `rok`, `miesiac` i `mies_od_ukoncz` (miesiąc od czerwca roku zostania absolwentem).
-	  Nazwy zmiennych `okres_dyplom` w P1 i `okres_kont` w P2 zmieniono na `okres`.
-   W P1 zmienna `dyplom_szczegoly` jest tworzona jako czynnik (*factor*).
-   Gruntownie zmieniono format P2:
    -   teraz opisuje ona wszystkie kontynuacje nauki - **w tym w szkołach ogólnokształcących** - w **czerwcach** i **grudniach**, na poziomie konkretnego szkoło-zawodu (szkoło-dziedzino-dyscypliny wiodącej),
  	-   odpowiednio rozszerzono zakres zmiennych o identyfikator szkoły, jej typ, formę (rozróżnienie uczniów od uczestników KKZ/KUZ i studentów), teryt powiatu w którym znajduje się szkoła (tylko szkoły z RSPO) oraz kod zawodu,
  	-   kolumna `zrodlo` zastąpiła kolumnę `branza_kont_zrodlo` i teraz opisuje tabelę (już pojedynczą, a nie zestaw, jak wcześniej), na podstawie której utworzono każdy rekord,
  	-   przekształcenie do poprzedniej formy jest możliwe przez usunięcie nowych zmiennych (`id_szk_kont`, `typ_szk_kont`, `forma_kont`, `teryt_pow_kont`, `kod_zaw_kont`, `zrodlo`), usunięcie rekordów z brakami danych w zmiennej `branza_kont` i usunięcie zduplikowanych rekordów.
-   Rozszerzono zestaw zmiennych w P3 i zmieniono zestaw wartości niektórych wcześniej istniejących:
    -   `status` - tworzona jako czynnik (*factor*) - zawierająca wartości naszego podstawowego wskaźnika analitycznego opisującego 5 statusów edukacyjno-zawodowych,
  	-   przemianowano `status_nieokreslony` na `brak_danych_z_zus` i `biernosc` na `biernosc_zus`, co lepiej oddaje ich interpretację,
  	-   sposób tworzenia zmiennych opisujących wyniki z ZUS zmieniono w ten sposób, że przyjmują one wartości niebędące brakami danych również w sytuacji, gdy dla danego absolwenta w danym miesiącu była informacja w tabeli W17 (*przerwy w opłacaniu składek*, czyli *de facto* okresy wypłacania świadczeń przez ZUS), choć nie było w W16 (składki),
		- 	nie ma to znaczenia dla żadnego z dotychczas raportowanych przez nas wskaźników, bo odnoszą się one wyłącznie do informacji z W16 (wskazujących, że *coś miało miejsce*, więc traktujących brak danych tak samo, jak wartość wskazującą, że dane zdarzenie nie zaszło, choć mamy o danym absolwento-miesiącu dane z ZUS),
	  -   w zmiennej `dziecko` zintegrowano wcześniejsze zmienne `dziecko` i `dziecko2`, co wiązało się rozszerzeniem zakresu jej wartości do {0, 1, 2},
    -   zmieniono zakres wartości `kont_mlodoc_prac`, aby uwzględniał pełne przecięcie kontynuowania nauki z (byciem zatrudnionym u tego samego pracodawcy na umowie o pracę vs. w innej formie vs. u innego pracodawcy),
    -   zmienne opisujące kontynuację nauki (na podstawie informacji z W26):
		-   nowe zmienne `nauka_bs1st`, `nauka_technikum`, `nauka_artystyczna`, `nauka_sspdp` i `nauka_kpsp` opisująca mniej typowe (w tym w większości wypadków *w zasadzie nielegalne*) formy kontynuacji kształcenia,
		-	zmienne `nauka_bs1st`, `nauka_bs2st`, `nauka_technikum` rozróżniają szkoły specjalne {2} od tych bez specyfiki {1},
		-	zmienna `nauka_lo` zastąpiła `nauka_lodd` i rozróżnia szkoły dla dorosłych {1} od dla młodzieży specjalnych {3} i bez specyfiki {2},
		-	zmienne `nauka_spolic` i `nauka_artystyczna` wyróżniają różne typy tych szkół (w tym - obie - policealne szkoły artystyczne).
-   Rozszerzono zakres zmiennych w P4:
    -   zmienna `typ_szk` wyróżnia licea dla dorosłych, a Bednarską Szkołę Realną traktuje jako LO (dzięki dobrodziejstwu dostępu do W26),
	  -   zmienna `szk_specjalna` pozwala zidentyfikować szkoły specjalne (**nie**będące szkołami specjalnymi przysposabiającymi do pracy),
  	-   zmienne pozwalające łatwo odsiać zduplikowanych absolwentów: `duplikat_w_szkole`, `duplikat_wiele_szkol`,
	  -	  zmienne identyfikujące młodocianych absolwentów: `mlodoc_byl` i `typ_szk_mlodoc`,
	  -   zmienne `plec`, `typ_szk` i `typ_szk_mlodoc` są tworzone jako czynniki (*factor*).
-   Nowa tabela P6 (będąca nieco rozszerzoną W26), zawierająca dane wszystkich szkół (a docelowo również uczelni) występujących w danych.

## Nowe funkcje (pomocnicze)

-   Związane z konwersjami zmiennych opisujących miesiące (daty):
    -   `data2rokszk()` i `okres2rokszk()` pozwalają na konwersje odpowiednio dat zapisanych w formacie "%d-%m-%Y" (jak w *tabelach wejściowych*) oraz numerów *okresów* opisujących miesiące, w szczególności w *tabeli pośredniej* P3, na odpowiadające im lata szkolne (opisane ciągiem znaków, np. "2024/2025" lub wartości liczbowe opisujące **pierwszy** rok kalendarzowy w ramach odpowiedniego roku szkolnego).
    -   `okres2data` pozwala na konwersję numerów *okresów* opisujących miesiące na ciągi znaków opisujące daty w formacie "%d-%m-%Y".
    -   `okres2rokmiesiac()` została zwektorowana, ale zachowała wsteczną kompatybilność zwracanej wartości w sytuacji, gdy jest wywoływana z pojedynczą wartością.
    -   `rokmiesiac2okres()` implementująca obliczenie numerów *okresów* na podstawie podanych wartości lat i miesięcy.
        -   W kodzie `przygotuj_tabele_posrednie()` jest ona teraz wykorzystywana lub nie w zależności od tego, czy przekształcenie ma być wykonane po stronie R, czy po stronie serwera bazy danych (który tej funkcji nie zna, choć w zasadzie można by mu ją zdefiniować, tylko potem trzeba by jeszcze sprawdzić, czy to się da obsłużyć na etapie tłumaczenia kodu na SQL przez *dplyr*).
-   `przygotuj_dane_do_w19()` automatyzuje proces przekształcania zestawień wartości wskaźników bezrobocia i przeciętnych miesięcznych wynagrodzeń w powiatach, pobranych z BDL przy pomocy funkcji `pobierz_dane_bdl()` z pakietu *MLASZdane*, do formatu tabeli *wejściowej* W19.
    -   W funkcji zaimplementowano również imputację braków danych wskaźnika przeciętnych wynagrodzeń dla pojedynczych powiatów, zaproponowany i wdrożony wcześniej na potrzeby przygotowywania raportów szkolnych przez Bartka Płatkowskiego. Wykorzystuje on informację o relatywnej zmianie przeciętnych miesięcznych wynagrodzeń w odpowiednim województwie względem roku, dla którego ostatni raz wartość wskaźnika w danym powiecie była znana.
    -   Wewnętrzne funkcja korzysta z nowych funkcji pomocniczych `przygotuj_zestawienie_brakow_danych()`, `przygotuj_komunikat_o_brakach_wskazniki()`, `przygotuj_komunikat_o_brakach_powiaty()` i `imputuj_wynagrodzenia()`.
-   `usun_duplikaty()` pozwala usunąć zduplikowane wystąpienia absolwentów (zarówno w ramach jednej szkoły, jak i między szkołami) z przygotowanych tabel *pośrednich*.

## Inne zmiany

-   Kod pakietu został przepisany tak, aby przechodził R CMD CHECK.

# MLASdaneAdm 0.4.3 (22.08.2024)

## Ulepszona diagnostyka

-   `wczytaj_tabele_wejsciowe()`:
    -   sprawdza, czy kolumny `ID_ABS` i `ROK_ABS` we wczytywanych plikach (w których te kolumny występują) nie zawierają braków danych;
    -   zapisuje w pliku `szkoły-z-multiabsolwentami.csv` dodatkową kolumnę opisującą, czy osoby będące absolwentami danej szkoły jednocześnie w kilku różnych zawodach zostały usunięte z wczytywanych danych, czy nie;
    -   sprawdza kompletność przypisania danych o certyfikatach i dyplomach zawodowych oraz świadectwach maturalnych i zapisuje odpowiednie zestawienia w plikach `matura-kompletnosc.csv` i `certyfikaty-i-dyplomy-kompletnosc.csv`.

## Naprawione błędy

-   `wczytaj_tabele_wejsciowe()` tworząc zestawienie szkół, w których absolwenci kontynuują naukę, w których te same osoby są absolwentami więcej niż jednego zawodu (zapisywane w pliku `szkoły-z-multiabsolwentami-kontynuacja-nauki.csv`) grupuje po kombinacji (`ID_ABS`, `ROK_ABS`), zamiast po samym `ID_ABS`.

## Dostosowania do zmian klasyfikacji

-   Przy wczytywaniu pliku `W20.csv` wyróżnia się nową wersję klasyfikacji branżowej (oznaczana jako `4L`) - oznaczane są nią wyłącznie przypisania do dwóch branż, których nazwy uległy zmianie w 2023 r. (*branża chemiczna* -> *branża chemiczna i ochrony środowiska*, *branża poligraficzna* -> *branża poligraficzno-księgarska*), w ich nowym brzmieniu. Pozwala to zachować dotychczasową strukturę klucza podstawowego tabeli `w20` w bazie danych.

## Drobne ulepszenia

-   `wczytaj_tabele_wejsciowe()` zapisując pliki CSV z zestawieniami problemów używa argumentu `na=''` (tj. zapisuje braki danych jako pustą komórkę), co czyni pliki łatwiejszymi w odbiorze przy ich przeglądaniu w zewnętrznych aplikacjach.

# MLASdaneAdm 0.4.2 (21.02.2024)

## Ulepszona diagnostyka

-   `wczytaj_tabele_wejsciowe()` sprawdza, czy w pliku `W7.csv` znajdują sie dane o maturach z lat wcześniejszych, niż rok ukończenia szkoły przez najmłodszy rocznik absolwentów objętych monitoringiem, a jeśli takich nie znajdzie, generuje ostrzeżenie.

## Inne zmiany

-   W związku ze zmianami w API dplyr-a, które dopuszcza teraz podawanie tylko jednej kolumny jako argumentu `order_by` w wywołaniach `slice_min()` i `slice_max()`, uproszczono wywołanie `slice_min()` przy tworzeniu *tablicy pośredniej* `p1`.

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
-   Nowa tabela wejściowa `w25` przechowuje mapowania kodów TERYT powiatów na nazwy powiatów, województw i jednostek podziału statystycznego.
    -   Wartości zapisane w tabeli są przyłączane do tabeli pośredniej `p4`, tworząc w niej kolumny: `nazwa_pow_szk`, `nazwa_woj_szk`, `nazwa_makroreg_szk`, `nazwa_reg_szk`, `nazwa_podreg_szk` i `nts_podreg_szk`.
-   Nowe kolumny w tabeli `w16` i odpowiadające im kolumny w tabeli pośredniej `p3`: `emeryt_rencista` i `niepelnosprawny`, których wartości przypisywane są na podstawie dwóch ostatnich cyfr kodu składki ZUS. W konsekwencji zmianie uległa definicja rekordu w tabeli `w16` - jest on teraz określany również przez kombinację wartości tych dwóch zmiennych (wcześniej dane z rekordów o tym samym "przyciętym o dwie ostatnie cyfry" kodzie składki były agregowane przed zapisaniem do `w16`, a informacje dot. emerytury/renty i niepełnosprawności były pomijane).
-   Nowe kolumny w tabeli `w22` i odpowiadające im kolumny w tabeli pośredniej `p3`: `pomoc_spol`, `macierz` i `wychow`.
-   Nowe kolumny w tabeli `w23` i odpowiadające im kolumny w tabeli pośredniej `p3`: `dziecko2`, `wypadek`, `choroba`, `choroba_macierz`.

## Ulepszona diagnostyka

-   `wczytaj_tabele_wejsciowe()`:
    -   Wykrywa rekordy występujące w `W1.csv`, ale nie w `W2.csv`.
    -   Sprawdza, czy wszystkie TERYTy powiatów utworzone na podstawie TERYTów szkół z `W1.csv` występują w `W25.csv`.
    -   Sprawdza, czy występują absolwenci więcej niż jednego zawodu w ramach tej samej szkoły, nie będącej szkołą policealną (i domyślnie ich usuwa, choć można to zachowanie zmienić nowym argumentem `usunAbsWKilkuZaw`).
    -   Sprawdza, czy w `W20.csv` (w połączeniu z `W20aneks.csv`) występują zawody przypisane do więcej niż jednej branży.
-   `przygotuj_tabele_posrednie()`:
    -   Sprawdza, czy podany `rokMonitoringu` wydaje się wiarygodny w kontekście przetwarzanych danych i ostrzega, jeśli stwierdzi, że nie jest.

## Naprawione błędy

-   Przy wczytywaniu tabel wejściowych rekordy z ewidentnie zduplikowanym `id_abs` są usuwane (wcześniej były oznaczane przypisaniem unikalnych, ujemnych wartości `id_abs`, które nie łączyły się z danymi w innych tabelach, ale nie były usuwane i przechodziły do tabel pośrednich w formie absolwentów, o których nic nie było wiadomo).
-   Przy tworzeniu tabeli pośredniej `p1`, zgodnie z pierwotną intencją, nie pojawiają się duplikaty dyplomów, jeśli dana kwalifikacja jest przypisana do kilku zawodów lub branż (wcześniej przypisanie do zawodu lub branży było dla wszystkich takich rekordów albo ustawiane na zgodne z zawodem/branżą zawodu, w którym uczył się absolwent, jeśli takowy wśród nich występował, lub na brak danych w przeciwnym przypadku, ale pozostawiano wszystkie kopie, które po takiej zmianie były ze sobą tożsame).
-   Przy tworzeniu tabeli pośredniej `p3` uczniowie policealnych szkół muzycznych są klasyfikowani jako uczący się w szkołach policealnych (wcześniej warunek pomijał ten typ szkoły).
-   Przy tworzeniu tabeli pośredniej `p3` prawidłowo obsługiwana jest sytuacja, kiedy dana osoba uczyła się w szkole (kontynuując kształcenie) tak krótko, że po skorygowaniu dat rozpoczęcia i zakończenia nauki o `minDniEdukacjiWMiesiacu` data ukończenia szkoły była wcześniejsza, niż data jej rozpoczęcia (wcześniej w takiej sytuacji generowany był nieprawidłowy rekord wskazujące na naukę).
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

## Nowe funkcjonalności:

-   Tworzenie i zapis nowej *tabeli pośredniej* `p5` zawierającej dane o absolwento-miesiąco-pracodawcach, która umożliwia tworzenie wskaźników stałości zatrudnienia.

## Ulepszona diagnostyka

-   `wczytaj_tabele_wejsciowe()`:
    -   Sprawdza. czy wszystkie kody zawodów są liczbami całkowitymi (jeśli nie, to je usuwa i zmienia typ kolumny, informującym o tym).
    -   Przygotowuje zestawienie opisujące kompletność danych o wynikach egzaminów.
    -   Sprawdza, czy w danych o dyplomach czeladnika (`w6`) występują powtórzenia.
    -   Sprawdza, czy w danych o dyplomach nie ma braków danych w kodach zawodów.

## Naprawione błędy

-   `wczytaj_tabele_wejsciowe()`:
    -   Zwracając kody tytułów ubezpieczenia i przerw w opłacaniu składek, które nie mają mapowania na kategorie analityczne (odpowiednio w tabelach `w22` i `w23`) wypisuje tylko unikalne kody (zamiast wszystkich wystąpień);
    -   Tworząc listę unikalnych kodów zawodów uwzględnia również te występujące w tabeli `w21`.
-   `przygotuj_tabele_posrednie()`:
    -   Sprawdza w poprawny sposób warunki, bez utworzenia których innych *tablic pośrednich* nie da się przygotować tablicy `p4`.

# MLASdaneAdm 0.2.1 (17.11.2021)

## Naprawione błędy

-   Domyślna wartość argumentu `okresyP2` funkcji `przygotuj_tabele_posrednie()` została zmieniona na opisującą grudnie od roku ukończenia szkoły do roku prowadzenia monitoringu (wcześniej okres ten był błędnie przesunięty o 1 rok wstecz).

# MLASdaneAdm 0.2.0 (3.11.2021)

## Nowy wskaźnik

-   W *tabeli pośredniej* `p3` tworzonej przez `przygotuj_tabele_posrednie()` tworzony jest nowy wskaźnik `nauka_szk_abs`, który wskazuje, czy w danym miesiącu dana osoba uczyła się jeszcze w szkole, jako absolwent której została objęta monitoringiem lub dany miesiąc zawierał się w okresie pomiędzy ukończeniem tej szkoły a podjęciem dalszej nauki w formie innej niż KKZ lub KUZ, najdalej w październiku roku zostania absolwentem. Odpowiednio zmodyfikowano też funkcję `wczytaj_tabele_posrednie()`.

# MLASdaneAdm 0.1.1 (18.10.2021)

## Sposób obliczania wskaźników

-   Przy wczytywaniu danych o składkach z pliku *W16.csv* `wczytaj_tabele_wejsciowe()` agreguje (w praktyce niezbyt liczne) duplikaty (id_abs, rok_abs, rok_skladka, mies_skladka, id_platnika, kod_zus) sumując podstawy składek i wybierając maksimum spośród CZY_RSA, zamiast wybierać jeden rekord z najwyższą podstawą składki (na ubezpieczenie chorobowe). Takie duplikaty opisują osoby, które miały u jednego pracodawcy w tym samym miesiącu kilka różnych umów (w praktyce to bywają umowy bardzo różnych typów).

## Organizacja pakietu

-   Zapis *tabel pośrednich* do relacyjnej bazy danych został wydzielony z `przygotuj_tabele_posrednie()` odrębnej funkcji `wczytaj_tabele_posrednie()`.

# MLASdaneAdm 0.1.0 (15.10.2021)

-   Pierwsza wersja pakietu. Opis podstawowych funkcjonalności znajduje się w pliku README.md.
