#' @title Przygotowywanie tabel "posrednich"
#' @description Na podstawie zawartych w bazie danych z tabel wejściowych
#' funkcja przygotowuje 6 tabel *pośrednich*, które zawierają zestawienia
#' wskaźników (bardziej) bezpośrednio użytecznych analitycznie. Aby zapisać tak
#' przygotowane tabele do relacyjnej bazy danych, należy następnie użyć funkcji
#' [wczytaj_tabele_posrednie()].
#' @param baza uchwyt połączenia do bazy lub lista argumentów do funkcji
#' [DBI::dbConnect()] umożliwiających nawiązanie połączenia z bazą
#' danych, w której mają zostać zapisane wczytywane dane
#' @param rokMonitoringu rok, w którym prowadzony jest monitoring (ma znaczenie
#' dla ustalenia adekwatnych przedziałów czasu, dla których mają zostać
#' obliczone wskaźniki, a na podstawie samych danych funkcja nie ma jak
#' go odgadnąć, nie narażając się na błędy)
#' @param okresOd opcjonalnie liczba całkowita - liczbę miesięcy, o którą
#' początek okresu objętego wskaźnikami w tabeli P3 ma być
#' **przesunięty względem** stycznia roku ukończenia szkoły (zostania
#' absolwentem); domyślnie 0 (tzn. pierwszym miesiącem uwzględnionym w P3 jest
#' styczeń roku ukończenia szkoły)
#' @param okresDo opcjonalnie liczba całkowita - identyfikator miesiąca w formie
#' `12*rok+numer_miesiaca_w_ramach_roku` (p. [rokmiesiac2okres()]), który ma
#' być ostatnim miesiącem objętym wskaźnikami w tabeli P3; domyślnie marzec roku
#' prowadzenia monitoringu
#' @param okresyP2 opcjonalnie wektor liczb całkowitych - identyfikatory
#' miesięcy w formie `12*rok+numer_miesiaca_w_ramach_roku`
#' (p. [rokmiesiac2okres()]), dla których mają zostać obliczone wskaźniki
#' zwracane w tabeli P2 (tj. wskaźniki dotyczące form kontynuacji nauki);
#' domyślnie grudzień każdego roku objętego monitoringiem
#' @param imputujDateKoncaSzkoly opcjonalnie data, która ma zostać użyta jako
#' data zakończenia nauki w szkołach, w których absolwenci kontynuują edukację,
#' jeśli w danych (tabeli W3) jest ona nieokreślona (tzn. absolwent cały czas
#' się w danej szkole uczy); domyślnie koniec roku szkolnego kończącego się
#' w roku prowadzenia monitoringu
#' @param imputujDateKoncaStudiow opcjonalnie data, która ma zostać użyta jako
#' data zakończenia nauki na studiach, na których absolwenci kontynuują
#' edukację, jeśli w danych (tabeli W12) jest ona nieokreślona (tzn. absolwent
#' cały czas tam studiuje); domyślnie koniec roku akademickiego kończącego się
#' w roku prowadzenia monitoringu
#' @param imputujDateKoncaAdresu opcjonalnie data, która ma zostać użyta jako
#' data końca obowiązywania danego adresu do korespondencji, zamieszkania lub
#' zameldowania (z danych ZUS), jeśli w danych (tabeli W15) jest ona
#' nieokreślona; domyślnie ostatni dzień maja roku prowadzenia monitoringu, tj.
#' koniec okresu objętego eksportem danych z ZUS
#' @param minDniEdukacjiWMiesiacu opcjonalnie liczba całkowita - minimalna
#' liczba dni nauki w miesiącu, wystarczająca aby wskaźniki opisujące status
#' kontynuacji edukacji w danym miesiącu w tabeli P3 wskazywały na kontynuację
#' nauki (a nie na jej brak); domyślnie 14
#' @param przygotujP1 opcjonalnie wartość logiczna - czy przygotować
#' tabelę *pośrednią* P1? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji)
#' @param przygotujP2 opcjonalnie wartość logiczna - czy przygotować
#' tabelę *pośrednią* P2? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji)
#' @param przygotujP3 opcjonalnie wartość logiczna - czy przygotować
#' tabelę *pośrednią* P3? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji)
#' @param przygotujP4 opcjonalnie wartość logiczna - czy przygotować
#' \emph{tabelę pośrednią} P4? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji); aby tabela P4 mogła zostać
#' przygotowana, konieczne jest również przygotowanie tabeli P3
#' @param przygotujP5 opcjonalnie wartość logiczna - czy przygotować
#' tabelę *pośrednią* P5? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji); aby tabela P5 mogła zostać
#' przygotowana, konieczne jest również przygotowanie tabeli P3
#' @param przygotujP6 opcjonalnie wartość logiczna - czy przygotować
#' tabelę *pośrednią* P6? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji);
#' @details Dane zawarte w bazie powinny obejmować **tylko jeden rok
#' prowadzenia monitoringu**, ale mogą obejmować **kilka różnych okresów od
#' ukończenia szkoły**.
#'
#' Wskaźniki dotyczące kontynuacji nauki w tabeli P3 nie odnoszą się do nauki
#' w szkole, jako absolwent której dana osoba została objęta monitoringiem,
#' a jedynie do *kontynuacji* nauki (w praktyce: nauki w innej szkole niż
#' ta, jako absolwent której dana osoba została objęta monitoringiem).
#'
#' W przypadku KKZ i KUZ przy kodowaniu wskaźników opisujących status
#' kontynuacji nauki w tabeli P3 lub branżę kontynuacji nauki w tabeli P2
#' zostały wzięte pod uwagę tylko kursy, które mają podaną datę zakończenia
#' (co do tych, które nie mają jej podanej, nie da się rzetelnie rozstrzygać,
#' czy zostały one ukończone, czy zarzucone).
#' @return lista ramek danych z przygotowanymi tabelami
#' @seealso [wczytaj_tabele_posrednie()]
#' @importFrom dplyr %>% .data across add_count arrange bind_rows case_when
#'                   collect copy_to distinct everything filter full_join
#'                   group_by join_by if_else inner_join left_join mutate n
#'                   order_by pull reframe rename right_join select
#'                   slice_min slice_max starts_with summarise tbl ungroup
#' @importFrom lubridate days month year
#' @importFrom DBI dbConnect dbExecute dbDisconnect
#' @export
przygotuj_tabele_posrednie <- function(
  baza, rokMonitoringu,
  okresOd = 0L, okresDo = rokmiesiac2okres(rokMonitoringu, 3L),
  okresyP2 = seq(rokmiesiac2okres(rokMonitoringu - 5L, 12L),
                 rokmiesiac2okres(rokMonitoringu - 1L, 12L), by = 6L),
  imputujDateKoncaSzkoly = as.Date(paste0(rokMonitoringu, "-08-31")),
  imputujDateKoncaStudiow = as.Date(paste0(rokMonitoringu, "-09-30")),
  imputujDateKoncaAdresu = as.Date(paste0(rokMonitoringu, "-05-31")),
  minDniEdukacjiWMiesiacu = 14L,
  przygotujP1 = TRUE, przygotujP2 = TRUE, przygotujP3 = TRUE,
  przygotujP4 = przygotujP3, przygotujP5 = TRUE, przygotujP6 = TRUE)
{
  stopifnot(is.list(baza) | inherits(baza, "DBIConnection"),
            is.numeric(rokMonitoringu), length(rokMonitoringu) == 1L,
            as.integer(rokMonitoringu) == rokMonitoringu,
            inherits(imputujDateKoncaSzkoly, "Date"),
            length(imputujDateKoncaSzkoly) == 1L,
            inherits(imputujDateKoncaStudiow, "Date"),
            length(imputujDateKoncaStudiow) == 1L,
            inherits(imputujDateKoncaAdresu, "Date"),
            length(imputujDateKoncaAdresu) == 1L,
            is.numeric(okresyP2), all(as.integer(okresyP2) == okresyP2),
            is.numeric(minDniEdukacjiWMiesiacu),
            length(minDniEdukacjiWMiesiacu) == 1L,
            as.integer(minDniEdukacjiWMiesiacu) == minDniEdukacjiWMiesiacu,
            minDniEdukacjiWMiesiacu > 0L,
            length(przygotujP1) == 1L, przygotujP1 %in% c(FALSE, TRUE),
            length(przygotujP2) == 1L, przygotujP2 %in% c(FALSE, TRUE),
            length(przygotujP3) == 1L, przygotujP3 %in% c(FALSE, TRUE),
            length(przygotujP4) == 1L, przygotujP4 %in% c(FALSE, TRUE),
            length(przygotujP5) == 1L, przygotujP5 %in% c(FALSE, TRUE),
            length(przygotujP6) == 1L, przygotujP6 %in% c(FALSE, TRUE))
  if (przygotujP4 & !przygotujP1) {
    stop("Tabela P4 nie może zostać przygotowana bez przygotowania tabeli P1.")
  }
  if (przygotujP4 & !przygotujP3) {
    stop("Tabela P4 nie może zostać przygotowana bez przygotowania tabeli P3.")
  }
  if (is.list(baza)) {
    con = do.call(dbConnect, args = baza)
  } else {
    con = baza
  }
  rokMonitoringu <- as.integer(rokMonitoringu)
  okresyP2 <- as.integer(okresyP2)
  minDniEdukacjiWMiesiacu <- as.integer(minDniEdukacjiWMiesiacu)
  on.exit({if (!inherits(baza, "DBIConnection")) dbDisconnect(con)})
  tabelePosrednie <- list()
  if (rokMonitoringu %in% (tbl(con, "w1") %>%
                           pull("rok_abs") %>%
                           unique())) {
    warning("Wartość argumentu `rokMonitoringu` występuje wśród wartości kolumny `rok_abs` tabeli z danymi wejściowymi W1. Czy na pewno wskazuje ona rok prowadzenia monitoringu?", immediate. = TRUE)
  }
  cat("\nStart: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")
  # P6 - bez względu na to, czy ma być zwrócona, czy nie
  szkW2 <- tbl(con, "w2") %>%
    pull("id_szk") %>%
    unique()
  t26 <- tbl(con, "w26b") %>%
    rename(typ_szk_rspo = "typ_szk") %>%
    mutate(szk_ma_abs = .data$id_szk %in% szkW2,
           typ_szk =
             case_when(.data$typ_szk_rspo == "Bednarska Szkoła Realna" ~ "Liceum ogólnokształcące",
                       .data$typ_szk_rspo == "Liceum ogólnokształcące" &
                         .data$kategoria_uczniow == "Dorośli" ~ "Liceum dla dorosłych",
                       .default = .data$typ_szk_rspo),
           typ_szk_sdd =
             ifelse(.data$specyfika != "brak specyfiki" &
                      .data$typ_szk != "Szkoła specjalna przysposabiająca do pracy",
                    paste0(.data$typ_szk, " - ", .data$specyfika),
                    .data$typ_szk)) %>%
    select("id_szk", "szk_ma_abs", "typ_szk", "typ_szk_sdd", "typ_szk_rspo",
           everything()) %>%
    left_join(tbl(con, "w26"),
              by = "id_szk")
  # Półfabrykaty do P1 i P2 ####################################################
  if (przygotujP1 | przygotujP2 | przygotujP4) {
    # mapowanie kodów zawodów na branże i ew. obszary z preferencją na przypisanie
    # do najnowszej wersji klasyfikacji
    t20 <- tbl(con, "w20") %>%
      select("kod_zaw", "branza", "wersja_klasyfikacji") %>%
      distinct() %>%
      group_by(.data$kod_zaw) %>%
      slice_max(.data$wersja_klasyfikacji, n = 1L) %>%
      ungroup() %>%
      select(-"wersja_klasyfikacji")
    # absolwenci z informacją o ew. zawodzie i dołączoną informacją o branży
    t2 <- tbl(con, "w2") %>%
      select("id_abs", "rok_abs", "kod_zaw") %>%
      distinct() %>%
      left_join(t20, by = "kod_zaw") %>%
      add_count(.data$id_abs, .data$rok_abs, .data$kod_zaw) %>%
      ungroup()
    if (!(t2 %>%
          summarise(noDuplicates = all(.data$n == 1L, na.rm = TRUE)) %>%
          pull("noDuplicates"))) {
      stop("Wykryto duplikaty w przypisaniu (zawodów) absolwentów do branż. Nie da się przygotować tabeli P1 ani P2.")
    }
    t2 <- t2 %>%
      select(-"n")
  }
  # P1 (dyplomy i świadectwa) ##################################################
  if (przygotujP1) {
    cat("Przygotowywanie tabeli P1...")
    # dyplomy czeladnika z dołączoną informacją o branży i informacjami
    # o zawodzie i branży absolwenta
    t6 <- t2 %>%
      inner_join(tbl(con, "w6") %>%
                   mutate(rodzaj_dyplomu = "tytuł czeladnika") %>%
                   rename(kod_zaw_dyplom = .data$kod_zaw_czel) %>%
                   left_join(t20 %>%
                               select(kod_zaw_dyplom = "kod_zaw",
                                      branza_dyplom = "branza"),
                             by = "kod_zaw_dyplom"),
                 by = c("id_abs", "rok_abs")) %>%
      select("id_abs", "rok_abs", "kod_zaw", "branza", "rodzaj_dyplomu",
             everything())
    # matury
    t7 <- t2 %>%
      inner_join(tbl(con, "w7") %>%
                   filter(.data$czy_zdana_matura,
                          !is.na(.data$data_swiad_matura)) %>%
                   group_by(.data$id_abs, .data$rok_abs) %>%
                   slice_min(order_by = .data$data_swiad_matura, n = 1L) %>%
                   ungroup() %>%
                   mutate(rodzaj_dyplomu = "matura",
                          rok = year(.data$data_swiad_matura),
                          miesiac = month(.data$data_swiad_matura)),
                 by = c("id_abs", "rok_abs")) %>%
      select("id_abs", "rok_abs", "kod_zaw", "branza", "rodzaj_dyplomu",
             "rok", "miesiac")
    # certyfikaty kwalifikacji
    t810 <- t2 %>%
      inner_join(tbl(con, "w810") %>%
                   mutate(rodzaj_dyplomu = "certyfikat kwalifikacji",
                          miesiac = if_else(.data$rok_kwal == year(.data$data_kwal),
                                            month(.data$data_kwal),
                                            8L)) %>%
                   left_join(tbl(con, "w21"), by = "kod_kwal") %>%
                   left_join(t20, by = "kod_zaw") %>%
                   rename(kod_zaw_dyplom = "kod_zaw", branza_dyplom = "branza",
                          rok = "rok_kwal"),
                 by = c("id_abs", "rok_abs")) %>%
      select("id_abs", "rok_abs", "kod_zaw", "branza", "rodzaj_dyplomu",
             "kod_kwal", "kod_zaw_dyplom", "branza_dyplom", "rok", "miesiac") %>%
      add_count(.data$id_abs, .data$rok_abs, .data$kod_zaw, .data$branza,
                .data$kod_kwal, .data$rodzaj_dyplomu, .data$rok, .data$miesiac) %>%
      collect()
    t810 <- bind_rows(
      t810 %>%
        filter(n == 1L) %>%
        ungroup() %>%
        select(-"n") %>%
        collect(),
      t810 %>%
        filter(.data$n > 1L) %>%
        group_by(.data$id_abs, .data$rok_abs, .data$kod_zaw, .data$branza,
                 .data$kod_kwal, .data$rodzaj_dyplomu, .data$rok, .data$miesiac) %>%
        summarise(kod_zaw_dyplom =
                    if_else(any(.data$kod_zaw_dyplom == .data$kod_zaw, na.rm = TRUE),
                            .data$kod_zaw[1], NA_integer_),
                  branza_dyplom =
                    if_else(any(.data$branza_dyplom == .data$branza, na.rm = TRUE),
                            .data$branza[1], NA_character_),
                  .groups = "drop") %>%
        collect()) %>%
      rename(dyplom_szczegoly = "kod_kwal")
    # dyplomy zawodowe
    t911 <- t2 %>%
      inner_join(tbl(con, "w911") %>%
                   mutate(rodzaj_dyplomu = "dyplom zawodowy",
                          miesiac = if_else(.data$rok_dyp == year(.data$data_dyp_zaw),
                                            month(.data$data_dyp_zaw),
                                            8L)) %>%
                   left_join(t20, by = "kod_zaw") %>%
                   rename(kod_zaw_dyplom = "kod_zaw", branza_dyplom = "branza",
                          rok = "rok_dyp"),
                 by = c("id_abs", "rok_abs")) %>%
      select("id_abs", "rok_abs", "kod_zaw", "branza", "rodzaj_dyplomu",
             "kod_zaw_dyplom", "branza_dyplom", "rok", "miesiac")
    # dyplomy ukończenia studiów
    t12d <- t2 %>%
      inner_join(tbl(con, "w12") %>%
                   filter(.data$czy_ukoncz_stu == 1L) %>%
                   mutate(rodzaj_dyplomu =
                            case_when(grepl("magister|lekarz",
                                            tolower(.data$tytul_zaw_stu)) ~ "dyplom magistra/lekarza",
                                      grepl("licencjat|inżynier",
                                            tolower(.data$tytul_zaw_stu)) ~ "dyplom licencjata/inżyniera",
                                      .data$tytul_zaw_stu == "Oficer dyplomowany" ~ "dyplom oficera",
                                      TRUE ~ .data$tytul_zaw_stu),
                          rok = year(.data$data_do_stu),
                          miesiac = month(.data$data_do_stu)) %>%
                   left_join(tbl(con, "w13") %>%
                               filter(.data$czy_dyscyplina_wiodaca),
                             by = "id_kierunku_stu") %>%
                   rename(dyplom_szczegoly = .data$tytul_zaw_stu,
                          dyscyplina_wiodaca = .data$dyscyplina),
                 by = c("id_abs", "rok_abs")) %>%
      select("id_abs", "rok_abs", "kod_zaw", "branza", "rodzaj_dyplomu",
             "dyplom_szczegoly", "rok", "miesiac", "dziedzina",
             "dyscyplina_wiodaca")
    # łączenie i wczytywanie do P1
    tabelePosrednie$p1 <- bind_rows(collect(t6),
                                    collect(t7),
                                    t810,
                                    collect(t911),
                                    collect(t12d)) %>%
      mutate(okres = rokmiesiac2okres(.data$rok, .data$miesiac),
             mies_od_ukoncz = .data$okres - rokmiesiac2okres(.data$rok_abs, 6L)) %>%
      group_by(.data$id_abs, .data$rok_abs, .data$rodzaj_dyplomu) %>%
      arrange(.data$id_abs, .data$rok_abs, .data$kod_zaw, .data$okres,
              .data$rodzaj_dyplomu) %>%
      # w stosunku do danych wejściowych ignoruję informację o profilu (praktyczny vs. ogólnoakademicki),
      # a zdarza się, że ludzie robią w tym samym okresie nominalnie dwa różne kierunku studiów,
      # jedne "praktyczne" a drugie "ogólnoakademickie"
      distinct() %>%
      mutate(lp_dyplom = seq_len(n()),
             rodzaj_dyplomu = factor(.data$rodzaj_dyplomu,
                                     unique(c("tytuł czeladnika",
                                              "certyfikat kwalifikacji",
                                              "dyplom zawodowy",
                                              "matura",
                                              "dyplom licencjata/inżyniera",
                                              "dyplom magistra/lekarza",
                                              "dyplom oficera"),
                                            sort(unique(.data$rodzaj_dyplomu))))) %>%
      select("id_abs", "rok_abs", "kod_zaw", "branza", "rok", "miesiac",
             "mies_od_ukoncz", "okres", everything()) %>%
      ungroup()
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P2 (branża/dziedzina kontynuacji kształcenia) ##############################
  if (przygotujP2) {
    cat("\nPrzygotowywanie tabeli P2...")
    okresyP2 <- tbl(con, "w1") %>%
      select("rok_abs") %>%
      distinct() %>%
      collect() %>%
      group_by(.data$rok_abs) %>%
      reframe(okres = okresyP2[okresyP2 > rokmiesiac2okres(.data$rok_abs, 6L)]) %>%
      mutate(rok = okres2rokmiesiac(.data$okres)$rok,
             miesiac = okres2rokmiesiac(.data$okres)$miesiac,
             mies_od_ukoncz = .data$okres - rokmiesiac2okres(.data$rok_abs, 6L))
    # szkoły objęte SIO
    t3c <- t2 %>%
      inner_join(tbl(con, "w3") %>%
                   rename(kod_zaw = "kod_zaw_kont",
                          id_szk = "id_szk_kont") %>%
                   left_join(t20, by = "kod_zaw") %>%
                   left_join(t26, by = "id_szk") %>%
                   select("id_abs", "rok_abs", "id_szk",
                          "data_od_szk_kont", "data_do_szk_kont",
                          typ_szk_kont = "typ_szk",
                          teryt_pow_kont = "teryt_gmi_szk",
                          kod_zaw_kont = "kod_zaw", branza_kont = "branza") %>%
                   mutate(data_do_szk_kont = if_else(is.na(.data$data_do_szk_kont),
                                                     imputujDateKoncaSzkoly,
                                                     .data$data_do_szk_kont),
                          teryt_pow_kont = 100*floor(.data$teryt_pow_kont / 1000),
                          forma_kont = "uczeń") %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      filter(!is.na(.data$branza_kont)) %>%
      mutate(okres_od_szk_kont = 12L*year(.data$data_od_szk_kont) + month(.data$data_od_szk_kont),
             okres_do_szk_kont = 12L*year(.data$data_do_szk_kont) + month(.data$data_do_szk_kont)) %>%
      inner_join(okresyP2,
                 by = join_by(x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_szk_kont, x$okres_do_szk_kont,
                                       y$okres, y$okres))) %>%
      select(-"data_od_szk_kont", -"data_do_szk_kont",
             -"okres_od_szk_kont", -"okres_do_szk_kont") %>%
      distinct() %>%
      mutate(zrodlo = "W3")
    # KKZ
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t4c <- t2 %>%
      inner_join(tbl(con, "w4") %>%
                   filter(!is.na(.data$data_do_kkz)) %>%
                   rename(kod_kwal = "kod_kwal_kkz",
                          id_szk = "id_szk_kont") %>%
                   left_join(tbl(con, "w21"), by = "kod_kwal") %>%
                   left_join(t20, by = "kod_zaw") %>%
                   left_join(t26, by = "id_szk") %>%
                   select("id_abs", "rok_abs", "id_szk",
                          "data_od_kkz", "data_do_kkz",
                          typ_szk_kont = "typ_szk",
                          teryt_pow_kont = "teryt_gmi_szk",
                          kod_zaw_kont = "kod_zaw", branza_kont = "branza") %>%
                   mutate(teryt_pow_kont = 100*floor(.data$teryt_pow_kont / 1000),
                          forma_kont = "KKZ") %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      filter(!is.na(.data$branza_kont)) %>%
      mutate(okres_od_kkz = 12L*year(.data$data_od_kkz) + month(.data$data_od_kkz),
             okres_do_kkz = 12L*year(.data$data_do_kkz) + month(.data$data_do_kkz)) %>%
      inner_join(okresyP2,
                 by = join_by(x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_kkz, x$okres_do_kkz,
                                       y$okres, y$okres))) %>%
      select(-"data_od_kkz", -"data_do_kkz", -"okres_od_kkz", -"okres_do_kkz") %>%
      distinct() %>%
      mutate(zrodlo = "W4")
    # KUZ
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t5c <- t2 %>%
      inner_join(tbl(con, "w5") %>%
                   filter(!is.na(.data$data_do_kuz)) %>%
                   rename(kod_zaw = "kod_zaw_kuz",
                          id_szk = "id_szk_kont") %>%
                   left_join(t20, by = "kod_zaw")  %>%
                   left_join(t26, by = "id_szk") %>%
                   select("id_abs", "rok_abs", "id_szk",
                          "data_od_kuz", "data_do_kuz",
                          typ_szk_kont = "typ_szk",
                          teryt_pow_kont = "teryt_gmi_szk",
                          kod_zaw_kont = "kod_zaw", branza_kont = "branza") %>%
                   mutate(teryt_pow_kont = 100*floor(.data$teryt_pow_kont / 1000),
                          forma_kont = "KUZ") %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      filter(!is.na(.data$branza_kont)) %>%
      mutate(okres_od_kuz = 12L*year(.data$data_od_kuz) + month(.data$data_od_kuz),
             okres_do_kuz = 12L*year(.data$data_do_kuz) + month(.data$data_do_kuz)) %>%
      inner_join(okresyP2,
                 by = join_by(x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_kuz, x$okres_do_kuz,
                                       y$okres, y$okres))) %>%
      select(-"data_od_kuz", -"data_do_kuz", -"okres_od_kuz", -"okres_do_kuz") %>%
      distinct() %>%
      mutate(zrodlo = "W5")
    # studia
    t12c <- t2 %>%
      inner_join(tbl(con, "w12") %>%
                   left_join(tbl(con, "w13") %>%
                               filter(.data$czy_dyscyplina_wiodaca),
                             by = "id_kierunku_stu") %>%
                   select("id_abs", "rok_abs", "data_od_stu", "data_do_stu",
                          dziedzina_kont = "dziedzina",
                          dyscyplina_wiodaca_kont = "dyscyplina") %>%
                   mutate(data_do_stu = if_else(is.na(.data$data_do_stu),
                                                imputujDateKoncaStudiow,
                                                .data$data_do_stu),
                          typ_szk_kont = "studia",
                          forma_kont = "student") %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      mutate(okres_od_stu = rokmiesiac2okres(year(.data$data_od_stu),
                                             month(.data$data_od_stu)),
             okres_do_stu = rokmiesiac2okres(year(.data$data_do_stu),
                                             month(.data$data_do_stu))) %>%
      inner_join(okresyP2,
                 by = join_by(x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_stu, x$okres_do_stu,
                                       y$okres, y$okres))) %>%
      select(-"data_od_stu", -"data_do_stu", -"okres_od_stu", -"okres_do_stu") %>%
      distinct() %>%
      mutate(zrodlo = "W12")
    # łączenie i wczytywanie do P2
    tabelePosrednie$p2 <- bind_rows(t3c, t4c, t5c) %>%
      select("id_abs", "rok_abs", "kod_zaw", "branza",
             "rok", "miesiac", "mies_od_ukoncz", "okres", id_szk_kont = "id_szk",
             "typ_szk_kont", "forma_kont", "teryt_pow_kont",
             "kod_zaw_kont", "branza_kont", "zrodlo") %>%
      bind_rows(t12c) %>%
      group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
      arrange(.data$id_abs, .data$rok_abs, .data$okres, .data$kod_zaw) %>%
      mutate(lp_kont = seq_len(n()),
             teryt_pow_kont = .data$teryt_pow_kont / 100L,
             forma_kont = factor(.data$forma_kont,
                                 unique(c("uczeń", "KKZ", "KUZ", "student"),
                                        sort(unique(.data$forma_kont))))) %>%
      ungroup()
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # Półfabrykaty do P3 i P5 ####################################################
  if (przygotujP3 | przygotujP5) {
    # składki ZUS
    t16 <- tbl(con, "w16") %>%
      mutate(podst_maks = if_else(.data$podst_chor > .data$podst_wypad,
                                  .data$podst_chor, .data$podst_wypad)) %>%
      mutate(podst_maks = if_else(.data$podst_maks > .data$podst_emer,
                                  .data$podst_maks, .data$podst_emer)) %>%
      mutate(podst_maks = if_else(.data$podst_maks > .data$podst_zdrow,
                                  .data$podst_maks, .data$podst_zdrow)) %>%
      mutate(podst_maks = as.numeric(.data$podst_maks)) %>% # pozbywanie się typu 'integer64'
      left_join(tbl(con, "w22") %>%
                  select("kod_zus", etat = "etat_ibe", netat = "netat_ibe",
                         samoz = "samoz_ela", inne = "inne_ibe", "mlodoc",
                         "bezrob_ibe", "bezrobotnystaz", "pomoc_spol",
                         "dziecko", "macierz", "wychow", "bierny_skladka"),
                by = "kod_zus")
  }
  # P3 (dane miesięczne) #######################################################
  if (przygotujP3) {
    cat("\nPrzygotowywanie tabeli P3 (może zająć około pół godziny)...")
    # wszystkie absolwento-miesiące
    # uwaga! początkiem zawsze jest styczeń rok_abs! końcem maj roku prowadzenia monitoringu
    wszystkieOkresy <- tbl(con, "w1") %>%
      select("rok_abs") %>%
      distinct() %>%
      collect() %>%
      group_by(.data$rok_abs) %>%
      reframe(okres = (rokmiesiac2okres(.data$rok_abs, 1L) + okresOd):okresDo) %>%
      mutate(rok = okres2rokmiesiac(.data$okres)$rok,
             miesiac = okres2rokmiesiac(.data$okres)$miesiac,
             mies_od_ukoncz = .data$okres - rokmiesiac2okres(.data$rok_abs, 6L))

    t1 <- tbl(con, "w1") %>%
      left_join(tbl(con, "w14") %>%
                  mutate(okres_zgonu = 12L*.data$rok_zgonu + .data$mies_zgonu) %>%
                  select("id_abs", "rok_abs", "okres_zgonu"),
                by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      dplyr::right_join(wszystkieOkresy,
                       by = "rok_abs") %>%
      mutate(zmarl = if_else(is.na(.data$okres_zgonu),
                             FALSE, .data$okres > .data$okres_zgonu)) %>%
      select(-"okres_zgonu")
    t1db <- copy_to(con, t1, overwrite = TRUE)
    # dane o kontynuacji nauki w szkołach objętych SIO przerobione na miesiące
    t3m <- tbl(con, "w3") %>%
      mutate(data_od_szk_kont =
               as.Date(.data$data_od_szk_kont + minDniEdukacjiWMiesiacu),
             data_do_szk_kont = as.Date(
               if_else(is.na(.data$data_do_szk_kont),
                       imputujDateKoncaSzkoly,
                       .data$data_do_szk_kont) - minDniEdukacjiWMiesiacu)) %>%
      rename(id_szk = "id_szk_kont") %>%
      left_join(t26, by = "id_szk") %>%
      select("id_abs", "rok_abs", typ_szk_kont = "typ_szk_sdd",
             "data_od_szk_kont", "data_do_szk_kont") %>%
      distinct() %>%
      collect()
    # dane o kontynuacji nauki na KKZ przerobione na miesiące
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t4m <- tbl(con, "w4") %>%
      filter(!is.na(.data$data_do_kkz)) %>%
      select("id_abs", "rok_abs", data_od_szk_kont = "data_od_kkz",
             data_do_szk_kont = "data_do_kkz") %>%
      mutate(typ_szk_kont = "KKZ",
             data_od_szk_kont = as.Date(.data$data_od_szk_kont + minDniEdukacjiWMiesiacu),
             data_do_szk_kont = as.Date(.data$data_do_szk_kont - minDniEdukacjiWMiesiacu)) %>%
      distinct() %>%
      collect()
    # dane o kontynuacji nauki na KUZ przerobione na miesiące
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t5m <- tbl(con, "w5") %>%
      filter(!is.na(.data$data_do_kuz)) %>%
      select("id_abs", "rok_abs", data_od_szk_kont = "data_od_kuz",
             data_do_szk_kont = "data_do_kuz") %>%
      mutate(typ_szk_kont = "KUZ",
             data_od_szk_kont = as.Date(.data$data_od_szk_kont + minDniEdukacjiWMiesiacu),
             data_do_szk_kont = as.Date(.data$data_do_szk_kont - minDniEdukacjiWMiesiacu)) %>%
      distinct() %>%
      collect()
    # dane o kontynuacji nauki na studiach przerobione na miesiące
    t12m <- tbl(con, "w12") %>%
      select("id_abs", "rok_abs", data_od_szk_kont = "data_od_stu",
             data_do_szk_kont = "data_do_stu") %>%
      mutate(typ_szk_kont = "studia",
             data_od_szk_kont = as.Date(.data$data_od_szk_kont + minDniEdukacjiWMiesiacu),
             data_do_szk_kont = as.Date(
               if_else(is.na(.data$data_do_szk_kont),
                       imputujDateKoncaStudiow,
                       .data$data_do_szk_kont) - minDniEdukacjiWMiesiacu)) %>%
      distinct() %>%
      collect()
    # laczenie
    t34512m <- bind_rows(t3m, t4m, t5m, t12m) %>%
      # tylko nauka, która trwała dłużej niż minDniEdukacjiWMiesiacu
      filter((.data$data_do_szk_kont - .data$data_od_szk_kont + minDniEdukacjiWMiesiacu) > 0) %>%
      mutate(okres_od_szk_kont = rokmiesiac2okres(year(.data$data_od_szk_kont),
                                                  month(.data$data_od_szk_kont)),
             okres_do_szk_kont = rokmiesiac2okres(year(.data$data_do_szk_kont),
                                                  month(.data$data_do_szk_kont))) %>%
      inner_join(wszystkieOkresy,
                 by = join_by(x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_szk_kont, x$okres_do_szk_kont,
                                       y$okres, y$okres))) %>%
      mutate(typ_szk_kont =
               factor(.data$typ_szk_kont, # poniżej radzenie sobie z sytuacją, że w jakimś z poniższych typów nikt jednak nie kontynuował nauki
                      levels = sort(unique(c(.data$typ_szk_kont,
                                             c("Branżowa szkoła I stopnia",
                                               "Branżowa szkoła I stopnia - specjalna",
                                               "Branżowa szkoła II stopnia",
                                               "Branżowa szkoła II stopnia - specjalna",
                                               "Technikum",
                                               "Technikum - specjalna",
                                               "Liceum dla dorosłych",
                                               "Liceum ogólnokształcące",
                                               "Liceum ogólnokształcące - specjalna",
                                               "Szkoła policealna",
                                               "Policealna szkoła plastyczna",
                                               "Policealna szkoła muzyczna",
                                               "Szkoła policealna - specjalna",
                                               "Liceum sztuk plastycznych",
                                               "Szkoła muzyczna II stopnia",
                                               "Szkoła muzyczna I stopnia",
                                               "Szkoła sztuki tańca",
                                               "Szkoła sztuki cyrkowej",
                                               "Inna szkoła artystyczna",
                                               "Szkoła specjalna przysposabiająca do pracy",
                                               "Kolegium Pracowników Służb Społecznych",
                                               "studia",
                                               "KKZ",
                                               "KUZ"))))),
             macierz_typ =
               matrix(diag(nlevels(.data$typ_szk_kont)),
                      nrow = nlevels(.data$typ_szk_kont),
                      dimnames = list(levels(.data$typ_szk_kont),
                                      levels(.data$typ_szk_kont)))[.data$typ_szk_kont, ]) %>%
      add_count(.data$id_abs, .data$rok_abs, .data$okres)
    t34512m <-
      bind_rows(t34512m %>% # dla wydajności
                  filter(.data$n == 1L) %>%
                  select("id_abs", "rok_abs", "okres", "macierz_typ"),
                t34512m %>%
                  filter(.data$n > 1L) %>%
                  group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
                  summarise(macierz_typ = matrix(colSums(.data$macierz_typ),
                                                 nrow = 1),
                            .groups = "drop")) %>%
      mutate(macierz_typ =
               matrix(.data$macierz_typ > 0L, nrow = n(),
                      dimnames = list(NULL, colnames(t34512m$macierz_typ)))) %>%
      mutate(nauka = 1L,
             nauka2 = as.integer(rowSums(
               .data$macierz_typ[, !(colnames(.data$macierz_typ) %in% c("KKZ", "KUZ"))]) > 0L),
             nauka_bs1st = case_when(
               .data$macierz_typ[, "Branżowa szkoła I stopnia"] ~ 1L,
               .data$macierz_typ[, "Branżowa szkoła I stopnia - specjalna"] ~ 2L,
               .default = 0L),
             nauka_bs2st = case_when(
               .data$macierz_typ[, "Branżowa szkoła II stopnia"] ~ 1L,
               .data$macierz_typ[, "Branżowa szkoła II stopnia - specjalna"] ~ 2L,
               .default = 0L),
             nauka_technikum = case_when(
               .data$macierz_typ[, "Technikum"] ~ 1L,
               .data$macierz_typ[, "Technikum - specjalna"] ~ 2L,
               .default = 0L),
             nauka_lo = case_when(
               .data$macierz_typ[, "Liceum dla dorosłych"] ~ 1L,
               .data$macierz_typ[, "Liceum ogólnokształcące"] ~ 2L,
               .data$macierz_typ[, "Liceum ogólnokształcące - specjalna"] ~ 3L,
               .default = 0L),
             nauka_spolic = case_when(
               .data$macierz_typ[, "Szkoła policealna"] ~ 1L,
               .data$macierz_typ[, "Policealna szkoła plastyczna"] ~ 2L,
               .data$macierz_typ[, "Policealna szkoła muzyczna"] ~ 3L,
               .data$macierz_typ[, "Szkoła policealna - specjalna"] ~ 4L,
               .default = 0L),
             nauka_artystyczna = case_when(
               .data$macierz_typ[, "Policealna szkoła plastyczna"] ~ 1L,
               .data$macierz_typ[, "Policealna szkoła muzyczna"] ~ 2L,
               .data$macierz_typ[, "Liceum sztuk plastycznych"] ~ 3L,
               .data$macierz_typ[, "Szkoła muzyczna II stopnia"] ~ 4L,
               .data$macierz_typ[, "Szkoła muzyczna I stopnia"] ~ 5L,
               .data$macierz_typ[, "Szkoła sztuki tańca"] ~ 6L,
               .data$macierz_typ[, "Szkoła sztuki cyrkowej"] ~ 7L,
               .data$macierz_typ[, "Inna szkoła artystyczna"] ~ 8L,
               .default = 0L),
             nauka_sspdp = case_when(
               .data$macierz_typ[, "Szkoła specjalna przysposabiająca do pracy"] ~ 1L,
               .default = 0L),
             nauka_kpsp = case_when(
               .data$macierz_typ[, "Kolegium Pracowników Służb Społecznych"] ~ 1L,
               .default = 0L),
             nauka_studia = as.integer(.data$macierz_typ[, "studia"]),
             nauka_kkz = as.integer(.data$macierz_typ[, "KKZ"]),
             nauka_kuz = as.integer(.data$macierz_typ[, "KUZ"])) %>%
      select(-"macierz_typ")
    # dane adresowe przerobione na miesiące
    # ignoruję ostatnią z zebranych cyfr TERYTu (miasto/wieś)
    # spośród konkurencyjnych TERYTów wybieram
    # - ten z adresu koresp. przed zamieszkania przed zameldowania,
    # - spośród adresów tego samego typu ten, który dotyczył najdłuższego okresu
    #   w ramach danego miesiąca
    t15 <- tbl(con, "w15") %>%
      mutate(data_adr_do =
               if_else(is.na(.data$data_adr_do) |
                         .data$data_adr_do > imputujDateKoncaAdresu,
                       imputujDateKoncaAdresu, .data$data_adr_do),
             teryt = 100L*floor(.data$teryt / 10)) %>%
      select(-"lp") %>%
      distinct() %>%
      mutate(okres_od_adr = 12L*year(.data$data_adr_od) + month(.data$data_adr_od),
             okres_do_adr = 12L*year(.data$data_adr_do) + month(.data$data_adr_do)) %>%
      inner_join(t1db, by = c("id_abs", "rok_abs")) %>%
      filter(.data$okres >= .data$okres_od_adr,
             .data$okres <= .data$okres_do_adr,
             !is.na(.data$teryt),
             .data$zmarl == FALSE) %>%
      # rozpisane w tak strasznej formie, żeby mogło wykonać się w bazie
      mutate(okres_pocz = as.Date(paste(floor((.data$okres - 1) / 12),
                                        .data$okres - 12L*(floor((.data$okres - 1) / 12)),
                                        "01", sep = "-")),
             okres_kon = as.Date(as.Date(paste(floor((.data$okres - 1) / 12),
                                               .data$okres - 12L*(floor((.data$okres - 1) / 12)),
                                               "01", sep = "-")) +
                                   months(1L) - days(1L))) %>%
      mutate(dni_okres = case_when(
        .data$data_adr_od >= .data$okres_pocz & .data$data_adr_do <= .data$okres_kon ~
          .data$data_adr_do - .data$data_adr_od,
        .data$data_adr_od >= .data$okres_pocz ~
          .data$okres_kon - .data$data_adr_od,
        .data$data_adr_do <= .data$okres_kon ~
          .data$data_adr_do - .data$okres_pocz,
        .default = .data$okres_kon - .data$okres_pocz)) %>%
      mutate(preferencja = case_when(.data$adres_typ == "koresp" ~ 300L,
                                     .data$adres_typ == "zam" ~ 200L,
                                     .data$adres_typ == "meld" ~ 100L,
                                     .default = 0L) + .data$dni_okres) %>%
      group_by(.data$id_abs, .data$rok_abs, .data$okres, .data$adres_typ,
               .data$teryt) %>%
      filter(.data$dni_okres == max(.data$dni_okres, na.rm = TRUE)) %>%
      group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
      filter(.data$preferencja == max(.data$preferencja, na.rm = TRUE)) %>%
      select("id_abs", "rok_abs", "okres", "teryt", "adres_typ", "dni_okres",
             "preferencja") %>%
      distinct()
    # w tak obrobionych absolwento-miesiącach wciąż zdarza się (choć rzadko!), że
    # trzeba wybrać spośród więcej niż jednego wiersza
    # ten wybór dokonywany jest poniżej
    # ponieważ doświadczenie 2021 r. pokazuje, że takie sytuacje zdarzają się
    # w ramach danego absolwenta tylko w pojedynczych miesiącach (choć trzeba
    # zaznaczyć, że w ogólności nie ma powodów, by nie mogło być inaczej),
    # wybór dokonywany jest na podstawie identyfikatora miesiąca (zmienna `okres`):
    # wybierany jest r-ty wg wartości liczbowej TERYT, gdzie r to powiększona o 1
    # reszta z dzielenia `okres` przez liczbę rekordów, spośród których należy wybrać
    t15 <- t15 %>%
      mutate(lp = order_by(.data$teryt, cumsum(.data$rok_abs) / .data$rok_abs) - 1L,
             n = n()) %>%
      filter(.data$lp == .data$okres %% .data$n) %>%
      mutate(lp = .data$lp + 1L) %>%
      select("id_abs", "rok_abs", "okres", "teryt") %>%
      ungroup() %>%
      left_join(tbl(con, "w19") %>%
                  mutate(okres = 12L*.data$rok + .data$miesiac) %>%
                  select(-"rok", -"miesiac") %>%
                  rename(teryt = "teryt_pow",
                         powiat_bezrobocie = "stopa_bezrobocia",
                         powiat_sr_wynagrodzenie = "sr_wynagrodzenia"),
                by = c("okres", "teryt"))
    # przerwy w opłacaniu składek
    t17 <- tbl(con, "w17") %>%
      mutate(okres_od_przerwa =
               12L*year(.data$data_od_przerwa) + month(.data$data_od_przerwa),
             okres_do_przerwa =
               12L*year(.data$data_do_przerwa) + month(.data$data_do_przerwa)) %>%
      inner_join(t1db, by = c("id_abs", "rok_abs")) %>%
      filter(.data$okres >= .data$okres_od_przerwa,
             .data$okres <= .data$okres_do_przerwa,
             .data$zmarl == FALSE) %>%
      left_join(tbl(con, "w23") %>%
                  select("kod", "bierny_zawodowo", "dziecko2", "wypadek",
                         "choroba", "choroba_macierz"),
                by = join_by(x$kod_przerwy == y$kod)) %>%
      group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
      summarise(przerwa = TRUE,
                dziecko2 = as.integer(any(.data$dziecko2, na.rm = TRUE)),
                wypadek = as.integer(any(.data$wypadek, na.rm = TRUE)),
                choroba = as.integer(any(.data$choroba, na.rm = TRUE)),
                choroba_macierz = as.integer(any(.data$choroba_macierz, na.rm = TRUE)),
                .groups = "drop")
    # składki ZUS
    t16m <- t16 %>%
      group_by(.data$id_abs, .data$rok_abs, .data$rok_skladka, .data$mies_skladka) %>%
      summarise(
        okres = 12L*.data$rok_skladka + .data$mies_skladka,
        praca = case_when(
          any(.data$etat, na.rm = TRUE) & any(.data$samoz, na.rm = TRUE) &
            any(.data$netat | .data$inne, na.rm = TRUE) ~ 7L, # zatrudnienie na podstawie umowy o pracę i działalność gospodarcza i zatrudnienie w innej formie
          any(.data$samoz, na.rm = TRUE) & any(.data$netat | .data$inne, na.rm = TRUE) ~ 6L, # działalność gospodarcza i zatrudnienie w innej formie
          any(.data$etat, na.rm = TRUE) & any(.data$netat | .data$inne, na.rm = TRUE) ~ 5L, # zatrudnienie na podstawie umowy o pracę i zatrudnienie w innej formie
          any(.data$etat, na.rm = TRUE) & any(.data$samoz, na.rm = TRUE) ~ 4L, # zatrudnienie na podstawie umowy o pracę i działalność gospodarcza
          any(.data$etat, na.rm = TRUE) ~ 1L, # zatrudnienie na podstawie umowy o pracę, brak innej formy,
          any(.data$samoz, na.rm = TRUE) ~ 2L, # działalność gospodarcza (samozatrudnienie), brak innej formy
          any(.data$netat | .data$inne, na.rm = TRUE) ~ 3L, # zatrudnienie w innej formie (umowa zlecenie, o dzieło, …), brak zatrudnienia na etacie ani działalności gospodarczej
          .default = 0L),
        mlodociany = as.integer(any(.data$mlodoc, na.rm = TRUE)),
        bezrobocie = as.integer(any(.data$bezrob_ibe, na.rm = TRUE)),
        bezrobocie_staz = if_else(any(.data$bezrob_ibe, na.rm = TRUE),
                                  as.integer(any(.data$bezrobotnystaz, na.rm = TRUE)),
                                  NA_integer_),
        dziecko = as.integer(any(.data$dziecko, na.rm = TRUE)),
        macierz = as.integer(any(.data$macierz, na.rm = TRUE)),
        wychow = as.integer(any(.data$wychow, na.rm = TRUE)),
        pomoc_spol = as.integer(any(.data$pomoc_spol, na.rm = TRUE)),
        emeryt_rencista = max(.data$emeryt_rencista, na.rm = TRUE),
        niepelnosprawny = max(.data$niepelnosprawny, na.rm = TRUE),
        wynagrodzenie = sum(.data$podst_maks, na.rm = TRUE),
        wynagrodzenie_uop = sum(.data$podst_maks[.data$etat], na.rm = TRUE),
        biernosc = any(.data$bierny_skladka, na.rm = TRUE),
        .groups = "drop")
    # straszny wskaźnik dot. młodocianych pracowników
    tMlodoc <- tbl(con, "w16") %>%
      mutate(okres = 12L*.data$rok_skladka + .data$mies_skladka) %>%
      select("id_abs", "rok_abs", "okres", "id_platnika", "kod_zus") %>%
      left_join(tbl(con, "w22") %>%
                  select("kod_zus", "mlodoc"),
                by = "kod_zus") %>%
      group_by(.data$id_abs, .data$rok_abs) %>%
      filter(any(.data$mlodoc, na.rm = TRUE)) %>%
      ungroup()
    tMlodoc <- tMlodoc %>%
      filter(.data$okres > 12L*.data$rok_abs + 6L) %>%
      left_join(tMlodoc %>%
                  filter(.data$mlodoc) %>%
                  select("id_abs", "rok_abs", "id_platnika") %>%
                  mutate(platnik_mlodoc = TRUE) %>%
                  distinct(),
                by = c("id_abs", "rok_abs", "id_platnika")) %>%
      left_join(tbl(con, "w22") %>%
                  select("kod_zus", etat = "etat_ibe"),
                by = "kod_zus") %>%
      group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
      summarise(mlodoc_byl = any(.data$mlodoc, na.rm = TRUE),
                mlodoc_ten_sam_platnik = any(.data$platnik_mlodoc, na.rm = TRUE),
                mlodoc_ten_sam_platnik_etat = any(.data$platnik_mlodoc & .data$etat,
                                                  na.rm = TRUE),
                .groups = "drop")
    # łączenie
    # materializacje tabel do R - bez niej backend ma tendencje do zawieszania się
    t15 <- collect(t15) # to trwa kilkanaście minut!
    t16m <- collect(t16m)
    t17 <- collect(t17)
    # tMlodoc nie należy materializować do R, bo dalszy kod tworzący p4 zakłada, że jest wciąż w bazie
    tabelePosrednie$p3 <- t1 %>%
      left_join(full_join(t16m, t17, by = c("id_abs", "rok_abs", "okres")) %>%
                  left_join(t15, by = c("id_abs", "rok_abs", "okres")) %>%
                  mutate(across(-c("id_abs", "rok_abs",
                                   "rok_skladka", "mies_skladka", "okres",
                                   "emeryt_rencista", "niepelnosprawny",
                                   "teryt", starts_with("wynagrodzenie"),
                                   starts_with("powiat")),
                                ~if_else(is.na(.), 0L, as.integer(.))),
                         brak_danych_z_zus = FALSE) %>%
                  mutate(dziecko = case_when(.data$dziecko %in% 1L ~ 1L,
                                             .data$dziecko2 %in% 1L ~ 2L,
                                             .default = 0L),
                         biernosc_zus =
                           as.integer(.data$biernosc %in% 1L | .data$przerwa %in% 1L)) %>%
                  select(-"dziecko2", -"przerwa", -"biernosc") %>%
                  rename(teryt_zam = "teryt") %>%
                  left_join(collect(tMlodoc),
                            by = c("id_abs", "rok_abs", "okres")),
                by = c("id_abs", "rok_abs", "okres")) %>%
      collect() %>%
      left_join(t34512m,
                by = c("id_abs", "rok_abs", "okres")) %>%
      mutate(teryt_zam = floor(.data$teryt_zam / 100),
             brak_danych_z_zus =
               if_else(is.na(.data$brak_danych_z_zus),
                       TRUE, .data$brak_danych_z_zus),
             across(starts_with("nauka"), ~if_else(is.na(.), 0L, as.integer(.))),
             nauka_szk_abs =
               if_else(.data$okres <= (12L*.data$rok_abs + 6L), 1L, 0L)) %>%
      group_by(.data$id_abs, .data$rok_abs) %>%
      mutate(nauka_szk_abs =
               if_else(.data$okres >= (12L*.data$rok_abs + 7L) &
                         .data$okres <= (12L*.data$rok_abs + 9L) &
                         .data$nauka2 == 0L &
                         # sztuczka z sum() żeby nie wybuchało, jeśli w danych
                         # brak obserwacji spełniającej warunek okres == (12L*rok_abs + 10L)
                         (sum(0L, cumsum(.data$nauka2)[.data$okres == (12L*.data$rok_abs + 10L)]) -
                            cumsum(.data$nauka2)) > 0L,
                       1L, .data$nauka_szk_abs)) %>%
      ungroup() %>%
      mutate(kont_mlodoc_prac =
               case_when(.data$mlodoc_byl %in% TRUE & .data$nauka == 0L &
                           !(.data$praca %in% 0) &
                           !(.data$mlodoc_ten_sam_platnik %in% TRUE) ~ 1L,
                         .data$mlodoc_byl %in% TRUE & .data$nauka == 0L &
                           !(.data$praca %in% 0) &
                           .data$mlodoc_ten_sam_platnik_etat %in% TRUE ~ 2L,
                         .data$mlodoc_byl %in% TRUE & .data$nauka == 0L &
                           !(.data$praca %in% 0) &
                           .data$mlodoc_ten_sam_platnik %in% TRUE ~ 3L,
                         .data$mlodoc_byl %in% TRUE & .data$nauka == 1L &
                           !(.data$praca %in% 0) &
                           !(.data$mlodoc_ten_sam_platnik %in% TRUE) ~ 4L,
                         .data$mlodoc_byl %in% TRUE & .data$nauka == 1L &
                           !(.data$praca %in% 0) &
                           .data$mlodoc_ten_sam_platnik_etat %in% TRUE ~ 5L,
                         .data$mlodoc_byl %in% TRUE & .data$nauka == 1L &
                           !(.data$praca %in% 0) &
                           .data$mlodoc_ten_sam_platnik %in% TRUE ~ 6L),
             status = case_when(
               (.data$nauka2 == 1L | .data$nauka_szk_abs == 1L) &
                 .data$praca > 0L  ~ "Nauka i praca",
               (.data$nauka2 %in% 1L | .data$nauka_szk_abs == 1L) &
                 .data$praca %in% c(0L, NA_integer_) ~ "Tylko nauka",
               .data$nauka2 == 0L & .data$nauka_szk_abs == 0L &
                 .data$praca > 0L ~ "Tylko praca",
               .data$bezrobocie == 1L & .data$nauka2 == 0L &
                 .data$nauka_szk_abs == 0L &
                  (.data$praca == 0L | is.na(.data$praca)) ~ "Bezrobocie",
               .default = "Brak danych o aktywności") %>%
               factor(c("Tylko nauka", "Nauka i praca", "Tylko praca",
                        "Bezrobocie", "Brak danych o aktywności")),
      ) %>%
      select(-"rok_skladka", -"mies_skladka", -"mlodoc_byl",
             -"mlodoc_ten_sam_platnik", -"mlodoc_ten_sam_platnik_etat") %>%
      select("id_abs", "rok_abs", "rok", "miesiac", "mies_od_ukoncz", "okres",
             "status", "zmarl", "brak_danych_z_zus", "praca", "mlodociany",
             "kont_mlodoc_prac", "bezrobocie":"nauka2",
             "nauka_szk_abs", everything())
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P4 (dane stałe w czasie i kilka fikuśnych wskaźników) ######################
  if (przygotujP4) {
    cat("\nPrzygotowywanie tabeli P4...")
    # średnie wynagrodzenia w zawodach dla danego rocznika
    tWynagrZawod <- tabelePosrednie$p3 %>%
      filter(!is.na(.data$praca),
             .data$praca > 0L,
             !is.na(.data$wynagrodzenie)) %>%
      left_join(collect(t2), by = c("id_abs", "rok_abs")) %>%
      group_by(.data$rok_abs, .data$kod_zaw) %>%
      summarise(zawod_sr_wynagrodzenie =
                  mean(as.numeric(.data$wynagrodzenie), na.rm = TRUE),
                .groups = "drop")
    # liczba pracodawców w okresie od lipca roku ukończenia szkoły do grudnia
    # roku poprzedzającego monitoring
    tPracodawcy <- tbl(con, "w16") %>%
      filter((12L*.data$rok_skladka + .data$mies_skladka) > (12L*.data$rok_abs + 6L),
             (12L*.data$rok_skladka + .data$mies_skladka) <= (12L*rokMonitoringu)) %>%
      left_join(tbl(con, "w22") %>%
                  select("kod_zus", etat = "etat_ibe", netat = "netat_ibe"),
                by = "kod_zus") %>%
      left_join(tabelePosrednie$p3 %>%
                  select("id_abs", "rok_abs", "okres", "nauka"),
                by = c("id_abs", "rok_abs"),
                copy = TRUE) %>% # robione w bazie ma mniejszy ślad pamięciowy
      group_by(.data$id_abs, .data$rok_abs) %>%
      summarise(l_prac_ucz_uop = n_distinct(.data$id_platnika[.data$nauka %in% 1L & .data$etat]),
                l_prac_nucz_uop = n_distinct(.data$id_platnika[.data$nauka %in% 0L & .data$etat]),
                l_prac_nucz_nuop = n_distinct(.data$id_platnika[.data$nauka %in% 0L & .data$netat]),
                .groups = "drop")
    # łączenie i zapis
    tabelePosrednie$p4 <- tbl(con, "w2") %>%
      left_join(t20, by = "kod_zaw") %>%
      left_join(tbl(con, "w25") %>%
                  rename(teryt_pow_szk = "teryt_pow",
                         nazwa_pow_szk = "powiat",
                         nazwa_woj_szk = "wojewodztwo",
                         nazwa_makroreg_szk = "makroregion",
                         nazwa_reg_szk = "region",
                         nazwa_podreg_szk = "podregion",
                         nts_podreg_szk = "nts"),
                by = "teryt_pow_szk") %>%
      left_join(tbl(con, "w24"),
                by = "kod_zaw") %>%
      left_join(tMlodoc %>%
                  select("id_abs", "rok_abs", "mlodoc_byl") %>%
                  filter(.data$mlodoc_byl) %>%
                  distinct(),
                by = c("id_abs", "rok_abs")) %>%
      select(-"typ_szk") %>%
      left_join(t26 %>%
                  select("id_szk", "typ_szk", szk_specjalna = "specyfika") %>%
                  mutate(szk_specjalna = .data$szk_specjalna == "specjalna") %>%
                  distinct(), # wiadomo, że zadziała, bo te kolumny pochodzą z w26b
                by = "id_szk") %>%
      mutate(teryt_pow_szk = .data$teryt_szk / 1000L,
             teryt_woj_szk = .data$teryt_szk / 100000L,
             mlodoc_byl = ifelse(is.na(.data$mlodoc_byl), FALSE, .data$mlodoc_byl),
             typ_szk_mlodoc = case_when(
               .data$typ_szk == "Branżowa szkoła I stopnia" & .data$mlodoc_byl ~
                 "Młodociani w Branżowej szkole I stopnia",
               .data$typ_szk == "Branżowa szkoła I stopnia" & !.data$mlodoc_byl ~
                 "Niemłodociani w Branżowej szkole I stopnia",
               .default = .data$typ_szk)) %>%
      select("id_abs", "rok_abs", "rok_ur", "plec",
             "id_szk", "mlodoc_byl", "typ_szk", "szk_specjalna", "typ_szk_mlodoc",
             "teryt_pow_szk", "nazwa_pow_szk", "teryt_woj_szk", "nazwa_woj_szk",
             "nazwa_makroreg_szk", "nazwa_reg_szk", "nazwa_podreg_szk", "nts_podreg_szk",
             "lp", "kod_zaw", "nazwa_zaw", "branza",
             "kod_isced", "grupa_isced", "podgrupa_isced", "nazwa_isced") %>%
      left_join(tPracodawcy, by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      left_join(tWynagrZawod, by = c("rok_abs", "kod_zaw")) %>%
      left_join(tabelePosrednie$p1 %>%
                  filter(.data$rodzaj_dyplomu %in% c("certyfikat kwalifikacji",
                                                     "dyplom zawodowy", "matura")) %>%
                  select("id_abs", "rok_abs") %>%
                  distinct() %>%
                  mutate(abs_w_cke = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      left_join(tabelePosrednie$p3 %>%
                  filter(.data$nauka %in% 1L & !(.data$nauka_studia %in% 1L)) %>%
                  select("id_abs", "rok_abs") %>%
                  distinct() %>%
                  mutate(abs_w_sio = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      left_join(tabelePosrednie$p3 %>%
                  filter(.data$nauka_studia %in% 1L) %>%
                  select("id_abs", "rok_abs") %>%
                  distinct() %>%
                  mutate(abs_w_polon = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      left_join(tabelePosrednie$p3 %>%
                  filter(!.data$brak_danych_z_zus) %>%
                  select("id_abs", "rok_abs") %>%
                  distinct() %>%
                  mutate(abs_w_zus = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      mutate(plec = factor(as.character(.data$plec), # pozbywanie się typu 'pq_plec'
                           unique(c("K", "M",
                                    sort(unique(.data$plec))))),
             typ_szk = factor(.data$typ_szk,
                              unique(c("Liceum ogólnokształcące",
                                       "Liceum dla dorosłych",
                                       "Technikum",
                                       "Branżowa szkoła II stopnia",
                                       "Szkoła policealna",
                                       "Branżowa szkoła I stopnia",
                                       "Szkoła specjalna przysposabiająca do pracy",
                                       sort(unique(.data$typ_szk))))),
             typ_szk_mlodoc = factor(.data$typ_szk_mlodoc,
                                     unique(c("Liceum ogólnokształcące",
                                              "Liceum dla dorosłych",
                                              "Technikum",
                                              "Branżowa szkoła II stopnia",
                                              "Szkoła policealna",
                                              "Młodociani w Branżowej szkole I stopnia",
                                              "Niemłodociani w Branżowej szkole I stopnia",
                                              "Szkoła specjalna przysposabiająca do pracy",
                                              sort(unique(.data$typ_szk))))),
             across(starts_with("abs_w_"), ~if_else(is.na(.), FALSE, .)),
             across(starts_with("l_prac_"), ~if_else(is.na(.), 0L, .)))
    # identyfikacja duplikatów
    tabelePosrednie$p4 <- tabelePosrednie$p4 %>%
      add_count(.data$id_abs, .data$rok_abs, .data$id_szk,
                name = "duplikat_w_szkole") %>%
      left_join(tabelePosrednie$p4 %>%
                  select("id_abs", "rok_abs", "id_szk") %>%
                  distinct() %>%
                  count(.data$id_abs, .data$rok_abs,
                        name = "duplikat_wiele_szkol"),
                by = c("id_abs", "rok_abs")) %>%
      mutate(across(c("duplikat_w_szkole", "duplikat_wiele_szkol"),
                    ~. > 1L)) %>%
      select("id_abs", "rok_abs", "duplikat_w_szkole", "duplikat_wiele_szkol",
             everything())
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P5 (absolwento-miesiąco-pracodawcy) ########################################
  if (przygotujP5) {
    cat("\nPrzygotowywanie tabeli P5...")
    tabelePosrednie$p5 <- t16 %>%
      group_by(.data$id_abs, .data$rok_abs, .data$id_platnika,
               .data$rok_skladka, .data$mies_skladka) %>%
      summarise(
        okres = 12L*.data$rok_skladka + .data$mies_skladka,
        forma_zatrudnienia = case_when(
          all(.data$kod_zus %in% c(1298L, 1299L), na.rm = TRUE) ~ 0L, # tu płatnikiem jest ZUS, wartości 0 zostaną niżej odfiltrowane
          any(.data$etat, na.rm = TRUE) ~ 1L,
          any(.data$samoz, na.rm = TRUE) ~ 2L,
          any(.data$netat, na.rm = TRUE) ~ 3L,
          .default = 0L), # te wartości zostaną niżej odfiltrowane
        mlodociany = as.integer(any(.data$mlodoc, na.rm = TRUE)),
        wynagrodzenie = sum(.data$podst_maks, na.rm = TRUE),
        wynagrodzenie_uop = sum(.data$podst_maks[.data$etat], na.rm = TRUE),
        .groups = "drop") %>%
      filter(.data$forma_zatrudnienia > 0L) %>%
      left_join(tbl(con, "w18") %>%
                  select("id_platnika", "pkd"),
                by = "id_platnika") %>%
      collect() %>%
      mutate(pkd = sub("^([[:digit:]]{2})[[:digit:]]{2}([[:alpha:]])$",
                       "\\1\\2", .data$pkd),
             mies_od_ukoncz = .data$okres - rokmiesiac2okres(.data$rok_abs, 6L)) %>%
      select("id_abs", "rok_abs", rok = "rok_skladka", miesiac = "mies_skladka",
             "mies_od_ukoncz", "okres", lp_pracod = "id_platnika",
             pkd_pracod = "pkd", "forma_zatrudnienia", "mlodociany",
             "wynagrodzenie", "wynagrodzenie_uop") %>%
      arrange(.data$id_abs, .data$rok_abs, .data$okres) %>%
      group_by(.data$id_abs, .data$rok_abs) %>%
      mutate(lp_pracod = as.numeric(factor(.data$lp_pracod,
                                           unique(.data$lp_pracod)))) %>%
      ungroup()
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  if (przygotujP6) {
    cat("\nTabela P6 przygotowana.")
    tabelePosrednie$p6 <- collect(t26) %>%
      mutate(organ_prowadzacy_teryt = .data$organ_prowadzacy_teryt / 100L)
  }
  # kończenie ##################################################################
  cat("\nKoniec: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")

  return(tabelePosrednie)
}
