# MLASdaneAdm 2.0.0 (??.11.2026)

## Do zrobienia

-   Przetestować zgodność z danymi z wcześniejszych edycji.

## Zmiany w w strukturze tabel *wejściowych*

-   Dostosowanie funkcji `wczytaj_tabele_wejsciowe()` do rozszerzonego nowelizacją ustawy zakresu danych przetwarzanych w ramach monitoringu:
    -   Nowe kolumny w plikach wejściowych:
        -   `KRAJ_POCHODZENIA` w *W1.csv*,
        -   `FORMA_KSZTALC` i `NAUKA_POZA_SZKOLA` w *W2.csv*,
        -   `NAZWA_ZAW_KONT` w  *W3.csv*,
        -   `ROK_EGZ_ZAW` (zamiast `ROK_KWAL`), `SESJA_EGZ_ZAW`, `DOSTOSOWANIE_EGZ_ZAW`, `WYNIK_PISEMN_EGZ_ZAW` i `WYNIK_PRAKT_EGZ_ZAW` w *w8.csv* i *w10.csv*,
        -   `FORMA_STU` i `ID_PODMIOTU` w *W12.csv*,
        -   `DATA_OD_UBEZP`, `DATA_DO_UBEZP`, `WYMIAR_ZATR_LICZNIK`, `WYMIAR_ZATRUDNIENIA_MIANOWNIK` i `KOD_ZAW_PRACA` w *W16.csv*,
    -   Nowe testy poprawności:
        -   Wykluczanie z importu rekordów dotyczących absolwentów, dla których wykryto występowanie w danych jakichś oznak aktywności po dacie ich nominalnej śmierci (opisanej w *W14.csv*),
            -   Wedle wszelkiego prawdopodobieństwa są to osoby, których dane identyfikacyjne używane do łączenia między rejestrami nie są unikalne;
        -   Kontrole kompletności danych o wynikach egzaminu ósmoklasisty i (procentowych wyników) matury,
            -   Przyjęte progi skutkujące wywoływaniem ostrzeżeń są raczej konserwatywne i w przyszłości mogą wymagać korekt w konfrontacji z danymi z kolejnych edycji;
        -   Wyszukiwanie powtórzeń absolwento-roko-termino-przedmioto-poziomo-formy zdawania w *W28.csv*,
        -   Kontrolowanie kompetności *W30.csv* względem *W12.csv*,
        -   Wyszukiwanie zduplikowanych rekordów opisujących zatrudnienie na umowę o pracę w pełnym wymiarze w *W16.csv*.
    -   Nowe pliki z tabelami wejściowymi:
        -   *W27.csv* - wyniki procentowe egzaminu ósmoklasisty,
        -   *W28.csv* - wyniki procentowe matury,
        -   *W29.csv* - mapowanie kraju pochodzenia na (bardziej użyteczny analitycznie) region,
        -   *W30.csv* - słownik uczelni z POL-on:
            -   tworzony przy pomocy nowo dodanej funkcji `przygotuj_dane_do_w30()` (p. niżej),
        -   *W31.csv* - orzeczenia o niepełnosprawności z SIO,
        -   *W32.csv* - składki do KRUS,
        -   *W33.csv* - świadczenia z KRUS.
    -   Istotne zmiany w tworzonej bazie danych wykraczające poza te wynikające ze wspomnianego wyżej rozszerzenia zakresu danych:
        -   Nowa tabela *w810d* do przechowywania informacji o dostosowaniach na egzaminach zawodowych;
        -   Zmiany w tabeli *w16*:
            -   Nowa kolumna `LP` ujednoznaczniająca rekordy o tych samych kombinacja wartośći (`ID_ABS`, `ROK_ABS`, `ID_PLATNIKA`, `ROK_SKLADKA_MIES_SKLADKA`, `KOD_ZUS`), gdyż ze względu na informacje o wymiarze etatu i okresie przepracowanym w ramach miesiąca odstąpiono od prostej agregacji w ramach takich kombinacji wartości;
            -   Nowa kolumna `UDZIAL_DNI_ZATR_MIES`, obliczana na podstawie kolumn `DATA_OD_UBEZP`, `DATA_DO_UBEZP` z pliku *W16.csv* i umieszczana w bazie zamiast nich,
                -   Należy miec świadomość, że niektóre rekordy nie mają podanych wartości `DATA_OD_UBEZP`, `DATA_DO_UBEZP` - w zdecydowanej większości przypadków ze względu na to, że opisują specyficzne sytuacje (których nie klasyfikujemy jako prace), dla których nie mają one sensu, choć zdarzają się też, na szczęście nieliczne, po prostu braki danych - w związku z czym `UDZIAL_DNI_ZATR_MIES` jest dla nich brakiem danych;
                -   W specjalny sposób obsługiwany jest tu kod składki 1240 (urlop macierzyński), który ma przypisywany `UDZIAL_DNI_ZATR_MIES` równy 1, choć w danych nie ma nigdy podanych wartości `DATA_OD_UBEZP`, `DATA_DO_UBEZP` (ale chcemy, aby - z pewnymi zastrzeżeniami - uwzględniać rekordy opisujące te składki przy obliczaniu *pełnomiesięcznych* wynagrodzeń);
            -   Nowa kolumna `WYMIAR_ZATR`, obliczana na podstawie kolumn `WYMIAR_ZATR_LICZNIK`, `WYMIAR_ZATRUDNIENIA_MIANOWNIK` z pliku *W16.csv* i umieszczana w bazie zamiast nich;
                -   Dla kodów ZUS 1110, 1111, 1112, 1113, 1114, 1120 (wojsko), 1130 (Policja), 1131 (agencje wywiadu), 1132 (Straż Graniczna) i 1133 (Państwowa Straż Pożarna) jeśli w danych nie podano wymiaru etatu, to jest przyjmowane, że jest to pełen etat;
                -   Choć nominalnie również służby, zatrudnienie w których opisywane jest kodami 1134 (Służba Celno-Skarbowa), 1135 (CBA i kontrwywiad) i 1140 (Służba Więzienna), co do zasady zatrudniają w pełnym wymiarze godzin, jednak są tu dopuszczalne wyjątki, a wysokość składek, dla których brak jest danych o wymiarze, jest w danych wyraźnie niższa niż dla tych składek, dla których podano wartość wymiaru jako pełen etat (również przy kontroli szkoły), więc w przypadku tych kodów nie dokonuje się takiej imputacji.
-   Zasadniczą część kodu odpowiedzialnego za zapis tabel *wejściowych* do bazy zamknieto w (nieeksportowanej) funkcji `przygotuj_argumenty_do_zapisania_tabeli_wejsciowej()`, co umożliwia łatwe wariantowanie pomiędzy edycjami wcześniejszymi, a tymi począwszy od 2026.
-   Nowy argument `wersja`, pozwalający wskazać, w jakiej strukturze są dane, które mają być przetwarzane, dodano do funkcji:
    -   `wczytaj_tabele_wejsciowe()`,
    -   `porownaj_liczbe_wierszy_w_tabelach_wejsciowych()`,
    -   `tabele_wejsciowe()`
-   Funkcja `przygotuj_dane_do_w26()` uzyskała możliwość wykorzystania danych o szkołach eksportowanych w całości z hurtowni SIO, dzięki czemu można mieć w W26 dostęp do prawdziwych (a nie tylko sztucznie multiplikowanych; inna sprawa, że one się w czasie specjalnie nie zmieniają) danych o nazwach, lokalizacjach itp. szkół.
-   Nowa funkcja `przygotuj_dane_do_w30()`, pozwalająca przygotować zawartość tabeli W30 na podstawie plików *SUCZELNIA.csv*, *W12.csv* i *W25.csv* oraz informacji o uczelniach i ich filiach pozyskanych z publicznego API RAD-on (https://radon.nauka.gov.pl/dane/instytucje-systemu-szkolnictwa-wyzszego-i-nauki i https://radon.nauka.gov.pl/dane/filie-instytucji-systemu-szkolnictwa-wyzszego-i-nauki).
    -   Na podobieństwo tabeli W26, tabelę W30 zaprojektowano jako przechowującą informacje o instytucjo-latach akademickich;
-   Nowy plik *W21aneks.csv* zawierający brakujące przypisania kodów kwalifikacji do kodów zawodów (które udało się odtworzyć);
    -   `wczytaj_tabele_wejsciowe()` przekształca kody kwalifikacji z *W4.csv*, *W8.csv*, *W10.csv* i *W21.csv*, aby usunąć oboczności ich zapisu (zera wiodące w części cyfrowej kodu, dopisywanie kropki na końcu);
-   Adekwatnie zmodyfikowano przetwarzanie danych w toku tworzenia tabel *pośrednich*:
    -   Przy tworzeniu *p1* brane są pod uwagę tylko te rekrody z *w810*, które opisują uzyskanie certyfikatu; uwzględniono też zmiany nazw kolumn opisujących rok;
    -   Przy tworzeniu *p2* wykorzystywana jest nazwa zawodu kontynuacji, dostępna od edycji 2026 bezpośrednio w *w3*;
    -   Przy tworzeniu *p2* i *p3* uwzględniono, że większość charkaterystyk szkół może się zmieniać w czasie.
-   Przy wczytywaniu danych do bazy z pliku *W15.csv* pomijane są rekordy z brakami danych w kolumnie `TERYT` - i tak zawsze były one usuwane na samym początku przetwarzania danych o adresach podczas tworzenia tabel *pośrednich*.

## Zmiany w strukturze tabel pośrednich

-   Dla edycji od 2026 w *p1* dodano kolumny `id_ucz` i `kierunek_stu` opisujące odpowiednio id POL-on/RAD-on uczelni albo jej filii i kierunek studiów, na których uzyskano dyplom (studiów).
-   W *p2* dodano kolumnę `nazwa_zaw_kont`, opisującą nazwę zawodu kontynuacji (gdy `kod_zaw_kont` nie jest brakiem danych) **albo** nazwę kierunku studiów:
    -   Nazwy zawodów kontynuacji nauki:
        -   Dla edycji do 2025 włącznie są tworzone na podstawie mapowania stworzonego w oparciu o tabelę *w2*;
        -   Dla edycji od 2026 w odniesieniu do rekordów pochodzących z tabeli *w3* nazwa zawodu jest brana bezpośrednio stamtąd, a w odniesieniu do rekordów pochodzących z *w4* i *w5* jest tworzona na podstawie mapowania stworzonego w oparciu o tabele *w2* i *w3*.
    -   Nazwy kierunków studiów są dostępne od edycji 2026 (dla wcześniejszych edycji, w rekordach opisujących studia kolumna ta jest brakiem danych).
-   Dla edycji od 2026 w *p2* dodano nowe zmienne opisujące studia: formę, lokalizację (powiat) uczelni, nazwę uczelni/filii oraz kierunku studiów;
-   Przy tworzeniu *p2* i *p3* data zakończenia studiów jest brana z `data_do_stu` albo z `data_skresl_stu`, w zależności od tego, która jest adekwatna (a nie z samego `data_do_stu`) i dopiero następnie imputowana wartością `imputujDateKoncaStudiow`.
    -   To jest naprawiony błąd (z tego powodu jest też wspomniany w sekcji o błędach poniżej).
-   W *p3* dodano kolumny `matura_zdana` i `dyplom_zaw`, opisujące, czy w danym miesiącu ktoś miał już zdaną maturę/certyfikaty lub dyplomy zawodowe;
    -   `matura_zdana` obliczana jest dla absolwentów wszystkich szkół, ale `dyplom_zaw` tylko dla absolwentów szkół kształcących zawodowo (dla pozostałych jest brakiem danych);
    -   Przy konstruowaniu `dyplom_zaw` brane są pod uwagę certyfikaty/dyplomy **tylko w zawodzie wyuczonym danego absolwenta**;
    -   Przy konstruowaniu zmiennych używane są również rekordy opisujące uzyskanie matury *przed ukończeniem szkoły* (jako absolwent której dana osoba została objęta monitoringiem);
    -   Przy konstruowaniu zmiennych używane są tylko rekordy opisujące uzyskanie dyplomu zawodowego nie wcześniej niż w roku ukończeniem szkoły (jako absolwent której dana osoba została objęta monitoringiem);
        -   Obie powyższe reguły są zgodne z tymi stosowanymi przez `LOSYwskaznik::dodaj_wskazniki_dyplomy()`;
    -   Arbitralnie przyjmuje się, że tytuł czeladnika absolwenci uzyskują w czerwcu roku ukończenia szkoły.
-   Dla edycji od 2026 w *p3*:
    -   Dodano kolumnę `liczba_skladek` opisującą, na podstawie agregacji ilu różnych rekordów z *w16* powstał dany rekord w *p3*;
    -   Dodano kolumny opisujące wysokość wynagrodzeń skorygowaną o długość trwania okresu ubezpieczenia w danym miesiącu (`wynagrodzenie_mies` i `wynagrodzenie_mies_uop`) oraz - tylko dla umów o pracę - również o wymiar etatu (`wynagrodzenie_stawka`);
        -   Analogicznie do praktyki stosowanej od edycji 2025 w odniesieniu do zmiennych opisujących wynagrodzenia, wartości nowych zmiennych też są przycinane na wartościach kwintyli 0.005 i 0.995 swojego rozkładu w ramach przecięcia kohorty absolwentów i roku kalendarzowego (w tabeli zapisywane są również oryginalne zmienne, przechowujące wartości przed przycięciem);
        -   Przy obliczaniu wynagrodzeń skorygowanych o długość trwania okresu ubezpieczenia w danym miesiącu pomijane są rekordy, które nie miały w danych wejściowych podanych wartości `DATA_OD_UBEZP`, `DATA_DO_UBEZP`, **z wyjątkiem** tych opisujących urlop macierzyński (`KOD_ZUS` równe 1240), które wliczane są - z założeniem, że obejmowały pełen miesiąc - **jeśli tylko** są jedyną składką od danego płatnika w danym miesiącu opisującą sytuację, którą klasyfikujemy jako jakąś formę pracy; 
    -   Dodano kolumny opisującą wymiar etatu i długość zatrudnienia w ramach miesiąca:
        -   Zmienna `wymiar_zatr` zawiera kategoryzację opisującą przecięcie wymiaru etatu z liczbą miejsc zatrudnienia i jest uzupełniana przez `sr_udzial_dni_zatr` i `sr_udzial_dni_zatr_uop`, które pozwalają odfiltrować miesiące, kiedy ktoś nie pracował w tych samych miejscach przez cały miesiąc;
        -   Proponowany zestaw zmiennych i sposób ich konstrukcji może ulec zmienia po przeprowadzeniu konsultacji;
    -   W kolumnie `praca` dodano wartość 8, opisującą osoby ubezpieczone w KRUS, ale nieopłacające ubezpieczeń w ZUS (w przypadku zbiegu ubezpieczeń, ZUS nadpisuje KRUS)
    -   Dodano kolumnę `ubezp_krus` opisującą rolę osoby ubezpieczonej w KRUS;
        -   W stosunku do `rola_ubezp` z tabeli *w32* braki danych zostały zastąpione wartością 9;
    -   W kolumnach `choroba` i `wypadek` zostały uwzględnione dane z KRUS a wartość tych kolumn koduje teraz również źródło danych;
    -   Dodano kolumnę `teryt_pow_szk_kont` opisującę powiat kontynuacji nauki:
        -   Inna możliwa (choć rzadka) przyczyna występowania braków danych w tej zmiennej pomimo kontynuowania nauki, to studiowanie w znajdującej się za granicą filii polskiej uczelni;
    -   Kolumna `nauka_studia` koduje formę studiów (stacjonarne vs niestacjonarne);
    -   Dodano kolumnę `brak_danych_z_krus`, do kompletu z `brak_danych_z_zus`; 
    -   Wszystkie kolumny oprócz `id_abs`, `rok_abs`, `rok`, `miesiac`, `mies_od_ukoncz`, `okres`, `zmarl`, `brak_danych_z_zus` i `brak_danych_z_krus` zamieniane są na braki danych, jeśli `zmarl == TRUE`.
-   Tylko dla edycji 2026 w *p3* dodatkowo obliczana jest zmienna `status_bez_krus`, przechowująca status obliczony jak we wcześniejszych edycjach - na potrzeby łatwych porównań do raportu metodologicznego.
-   W *p4* dodano kolumnę `zmarl_mies_od_ukoncz` opisującą miesiąc zgonu absolwenta (i ogólniej pozwalającą zidentyfikować absowlentów, którzy zmarli, bez konieczności sięgania do *p3*):
    -   W praktyce wartości niebędące brakami danych występują w niej dopiero od edycji 2026.
-   Dla edycji od 2026 w *p4*:
    -   Dodano kolumny `kraj_pochodzenia`, `region_pochodzenia`, `forma_ksztalc`, `nauka_poza_szkola` i `absolwent_w_krus` opisujące odpowiednio kraj pochodzenia, formę kształcenia w szkołach policealnych i dla dorosłych, objęcie edukacją domową oraz to, czy jakiekolwiek dane dot. absolwenta wystąpiły w danych otrzymanych z KRUS:
        -   W przypadku nie-obcokrajowców `kraj_pochodzenia` jest pustym ciągiem znaków, ale zdarzają się też obcokrajowcy, dla których `kraj_pochodzenia` jest równy "POLSKA";
        -   Operacyjna definicja *nauki poza szkołą* (przyjęta przez CIE, podczas generowania *w2*) to realizowanie nauki poza szkołą przez co najmniej połowę okresu nauki w szkole, jako absolwent której dana osoba została objęta monitoringiem.
    -   Zmienna `zawod_sr_wynagrodzenie` obliczana jest na podatawie zmiennej `wynagrodzenie_stawka`.
    -   Zmienne zawierające wskaźniki średnich wynagrodzeń są obliczone na podstawie zmiennej `wynagrodzenie_stawka` i mają nazwy zmodyfikowane tak, aby na to wskazywały.
        -   Podniesiono zależność od *LOSYwskazniki* do wersji 0.8.0
-   Tylko dla edycji 2026 w *p4* obliczane są zarówno wskaźniki średnich wynagrodzeń oparte o `wynagrodzenie`, jak i oparte o `wynagrodzenie_stawka`.
-   Id pracodawcy w *p5* może być porównywane pomiędzy absolwentami (wcześniej było dodatkowo anonimizowane w ten sposób, żeby nie można było identyfikować tego samego pracodawcy między absolwentami).
-   Dla edycji od 2026 w *p5* (analogicznie jak do *p3*):
    -   Dodano kolumnę `liczba_skladek` opisującą, na podstawie agregacji ilu różnych rekordów z *w16* powstał dany rekord w *p5*;
    -   Dodano kolumny opisujące wysokość wynagrodzeń skorygowaną o długość trwania okresu ubezpieczenia w danym miesiącu (`wynagrodzenie_mies` i `wynagrodzenie_mies_uop`) oraz - tylko dla umów o pracę - również o wymiar etatu (`wynagrodzenie_stawka`);
    -   Dodano kolumny opisujące wymiar etatu (`wymiar_zatr`) i długość zatrudnienia w ramach miesiąca (`sr_udzial_dni_zatr` i `sr_udzial_dni_zatr_uop`).
-   W *p6* szkoły mogą już mieć (i miewają) różne dane w poszczególnych latach szkolnych.
    -   Uwaga, dotyczy to wszystkich danych poza typem szkoły!
-   Dla edycji od 2026 *p6* zawiera również dane uczelni, w których absolwenci kontynuowali naukę:
    -   Nowe kolumny: `id_ucz` - identyfikator uczelni/filii w POL-on, `podm_nadzrz_id` - identyfikator uczelni w POL-on dla rekordów opisujących filie uczelni (identyfikatory w POL-on są ciągami znaków, więc nie ma sensu przechowywać ich w tych samych kolumnach, co id RSPO szkół);
    -   Nowe kolumny: `typ_ucz` (akademicka vs zawodowa), `kraj_ucz` (mamy w danych również zagraniczne filie polskich uczelni), `teryt_pow_uczelni` (powiat lokalizacji uczelni - oddzielnie aby nie rezygnować z informacji na poziomie gminy dla szkół z RSPO), `rodzaj_gmi_ucz` (typ gminy/części gminy, jako uzupełnienie informacji o powiacie);
    -   Dane przypisane do dotychczasowych kolumn: `organ_prowadzacy_typ`, `typ_szk`, `typ_szk_sdd`, `szk_ma_abs`, `rok_szk` (uwaga, tu uznacza rok akademicki, rozpoczynający się w październiku, a nie we wrześniu!), `nazwa_szk`, `wojewodztwo_szk`, `powiat_szk`, `ulica_szk`, `pna`, `poczta`, `organ_prowadzacy_nazwa`, `miejsce_w_strukt`, `jedn_nadrz_typ`;
-   Nowe tabele pośrednie dla edycji od 2026:
    -   *p7* - wykonywane zawody po miesiącach (potencjalnie wiele rekordów opisujących absolwenta w jednym miesiącu):
        -   Zawiera zmienne opisujące (sumaryczną!) długość trwania okresu składkowego w ramach miesiąca (uwzględniającą każdą składkę z równą wagą i ważącą wymiarem zatrudnienia - jeśli jest dostępny) oraz PKD pracodawcy, ale nie wynagrodzenia;
        -   Należy mieć na uwadze, że przed 2022 r. dane o wykonywanym zawodzie są bardzo *dziurawe* (obowiązek ich raportowania wprowadzono w maju 2021 r., a nawet współcześnie często nie są podawane w przypadku umów-zleceń itp.).
    -   *p8* - wyniki punktowe egzaminów: zarówno ósmoklasisty, matury, jak i zawodowych:
        -   Zawiera również informacje o zwolnieniach, unieważnieniach itp.;
        -   W formie *długiej* - każdy przedmiot/część egzaminu tworzy oddzielny rekord;
        -   Zestaw wypełnionych kolumn różni się w pewnym stopniu w zależności od egzaminu.
    -   *p9* - informacje o niepełnosprawności:
        -   Pod uwagę brane są jedynie orzeczenia obowiązujące (choć częściowo) przed momentem ukończenia szkoły, jako absolwent której dana osoba została objęta monitoringiem;
        -   Przy obliczaniu łącznej długości trwania orzeczeń brany jest pod uwagę tylko okres do 30 czerwca roku ukończenia szkoły, jako absolwent której dana osoba została objęta monitoringiem.
        -   Przy obliczaniu łącznej długości trwania orzeczeń brany jest pod uwagę tylko okres od 1 września roku przypadającego na: (rok zostania absolwentem - nominalna liczba lat nauki w szkole danego typu).
        -   Łączny czas trwania orzeczeń *równy co najmniej 2 lata* to w praktyce co najmniej 660 dni.
        -   Informacje o dostosowaniach na egzaminach, uwzględniane są tylko z egzaminów, które odbyły się nie później niż w roku, w kótrym dana osoba została absolwentem.
-   Zmienieniono domyślną wartość argumentu `okresyP2` w `przygotuj_tabele_posrednie()`, aby dla edycji od 2026 pokrywała pełen *siedmioletni* okres obserwacji.

## Ulepszona diagnostyka

-   `wczytaj_tabele_wejsciowe()` kontroluje,
    -   czy wszystkie kody kwalifikacji pojawiające się w *W4.csv*, *W8.csv* i *W10.csv* mają przypisanie do kodów zawodów w *W21.csv*;
    -   czy wszystkie identyfikatory kierunków studiów pojawiające się w *W12.csv* mają przypisanie do dziedzin/dyscyplin w *W13.csv*.

## Nowe funkcje

-   `usun_nielegalne_zawody()` pozwala usunąć z tabel *pośrednich* (i nie tylko) absolwentów w zawodach, których formalnie nie można nauczać w szkole tego typu, jak przez nich ukończona;
    -   Na potrzeby wykorzystania tej funkcji do pakietu dodano zbiór danych `zawodyTypySzkol`, zawierający zestawienie *legalnych* kombinacji wartości zawód-typ szkoły;
-   `pobierz_dane_z_radonu()` pozwala pobrać dane z API RAD-onu i przekonać się, że niestety bez (nie zanonimizowanego) identyfikatora kierunku w danych z OPI nie jesteśmy w stanie w użyteczny sposób dołączyć do naszych danych o studiowaniu charakterystyk kierunków studiów z RAD-onu.

## Usprawnienia

-   `wczytaj_tabele_wejsciowe()` otrzymała opcjonalny argument `rokMonitoringu`, a w `przygotuj_tabele_posrednie()` istniejący wcześniej obowiązkowy argument `rokMonitoringu` stał się opcjonalny:
    -   W obu funkcjach domyślna wartość argumentu (brak danych) wskazuje, że jego wartość ma zostać zgadnięta na podstawie danych z *W1* (tak, jak do tej pory robiła ta `wczytaj_tabele_wejsciowe()`, przy czym oczywiście `przygotuj_tabele_posrednie()` zgaduje na podstawie danych wczytanych do bazy);
-   `przygotuj_tabele_posrednie()` aktywniej zwalnia pamięć;
-   `przygotuj_tabele_posrednie()` otrzymała nowy argument `zapisujPoKazdejTabeliPosredniej` pozwalający zapisywać na dysku stan tworzonych tabel *pośrednich* w miarę postępu procesu ich tworzenia - tak aby w razie, gdy proces *wybuchnie* (w szczególności w wyniku niedostatecznej ilości dostępnego RAMu) nie zostawać z niczym.

## Naprawione błędy

-   `data2rokszk()` poprawnie stosuje argument `max`; poprawiono też dokumentację tego argumentu (wskazując, że nadpisuje on wszystkie wartości wskazujące na późniejsze lata szkolne, a nie tylko te *nieskończone*);
    -   W efekcie `zidentyfikuj_szkolo_lata_w_danych()` uwzględnia, że ktoś mógł rozpocząć naukę w szkole (w której kontynuuje naukę) również w roku, w którym prowadzona jest dana edycja monitoringu (o ile zrobił to w semestrze letnim);
-   `wczytaj_tabele_wejsciowe()` potrafi sobie radzić z nieliczbowymi kodami zawodów;
-   `porownaj_liczbe_wierszy_w_tabelach_wejsciowych()` przechwytuje argumenty korzystając z `list(...)` zamiast `as.list(...)`;
-   `przygotuj_tabele_posrednie()` obliczając zmienną `zawod_sr_wynagrodzenie` (do tabeli *p4*) pomija zerowe wynagrodzenia;
-   Uzupełniono kilka miejsc z brakującym maskowanie `.data$` w wywołaniach funkcji pakietu *dplyr*;
-   `usun_duplikaty()` poprawnie raportuje liczbę rekordów usuniętych z *p4* (zamiast powtarzać informację o liczbie unikalnych absolwento-szkół, którzy zostali usunięci);
-   Przy tworzeniu *p2* i *p3* data zakończenia studiów jest brana z `data_do_stu` albo z `data_skresl_stu`, w zależności od tego, która jest adekwatna (a nie z samego `data_do_stu`) i dopiero następnie imputowana wartością `imputujDateKoncaStudiow`:
    -   To niestety oznacza, że we wszystkie dotychczas tworzonych tabelach *pośrednich* niesłusznie *przedłużano* (i to do końca okresu objętego monitoringiem) studiowanie absolwentom, którzy zostali skreśleni z listy studentów; z drugiej strony, z wyjątkiem danych dla kohorty *w pięć lat od ukończenia szkoły*, takie przypadki występowały bardzo rzadko.

# MLASdaneAdm 1.5.0 (21.11.2025)

## Naprawione błędy

-   Podniesiono zależność od pakietu *LOSYwskazniki* do wersji 0.2.0, w związku z czym kolumny `sr_wynagr_uop_nauka_r0_wrzgru` i `sr_wynagr_uop_bez_nauki_r0_wrzgru` tworzone w tabeli *pośredniej* P4 opisują **względne** wynagrodzenia (odniesione do średnich miesięcznych wynagrodzeń w danym roku w powiecie zamieszkania absolwenta) i są w końcu użyteczne do obliczania wskaźników na potrzeby raportów szkolnych.

## Nowe funkcje

-   `uzupelnij_wzgledne_wynagrodzenia()` pozwala uzupełnić braki danych w kolumn `sr_wynagr_uop_nauka_r0_wrzgru` i `sr_wynagr_uop_bez_nauki_r0_wrzgru` w tabeli *pośredniej* P4 na podstawie zaktualizowanego (od czasu tworzenia tabel *pośrednich*) pliku *wejściowego* W19.csv.
    -   Możliwe jest też użycie tej funkcji do uzupełnienia wartości kolumn `powiat_sr_wynagrodzenie` i `powiat_bezrobocie` w tabeli *pośredniej* P3.

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
