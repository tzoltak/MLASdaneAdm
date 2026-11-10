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
#' go odgadnąć, nie narażając się na błędy) - domyślna wartość, tj. brak danych,
#' wskazuje, że ma on zostać zgadnięty z danych (jako maksimum z `rok_abs`
#' w tabeli *w1* powiększone o jeden)
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
#' @param przygotujP7 opcjonalnie wartość logiczna - czy przygotować
#' tabelę *pośrednią* P7? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji);
#' @param przygotujP8 opcjonalnie wartość logiczna - czy przygotować
#' tabelę *pośrednią* P9? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji);
#' @param przygotujP9 opcjonalnie wartość logiczna - czy przygotować
#' tabelę *pośrednią* P9? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji);
#' @param zapisujPoKazdejTabeliPosredniej wartość logiczna - jeśli `TRUE`, po
#' przygotowaniu każdej kolejnej tabeli *pośredniej* lista tabeli *pośrednich*
#' zostanie zapisana w katalogu roboczym w pliku *tabele-posrednie-temp.RData*;
#' pozwala to uniknąć frustracji związanych z ew. *wybuchaniem* procesu,
#' w szczególności w wyniku kłopotów z nie dość dużą ilością RAMu;
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
#' @importFrom dplyr %>% .data across add_count any_of arrange bind_rows case_when collect copy_to distinct everything filter full_join group_by join_by if_else inner_join lag lead left_join mutate n order_by pull reframe rename right_join select slice_min slice_max starts_with summarise tbl ungroup where
#' @importFrom lubridate days month year
#' @importFrom DBI dbConnect dbExecute dbDisconnect dbIsValid
#' @importFrom LOSYwskazniki dodaj_wskazniki_dyplomy dodaj_wskazniki_prace dodaj_wskazniki_kontynuacje
#' @export
przygotuj_tabele_posrednie <- function(
  baza, rokMonitoringu = NA_integer_,
  okresOd = 0L, okresDo = rokmiesiac2okres(rokMonitoringu, 3L),
  okresyP2 = seq(rokmiesiac2okres(rokMonitoringu - ifelse(rokMonitoringu < 2026L, 5L, 7L), 12L),
                 rokmiesiac2okres(rokMonitoringu - 1L, 12L), by = 6L),
  imputujDateKoncaSzkoly = as.Date(paste0(rokMonitoringu, "-08-31")),
  imputujDateKoncaStudiow = as.Date(paste0(rokMonitoringu, "-09-30")),
  imputujDateKoncaAdresu = as.Date(paste0(rokMonitoringu, "-05-31")),
  minDniEdukacjiWMiesiacu = 14L,
  przygotujP1 = TRUE, przygotujP2 = TRUE, przygotujP3 = TRUE,
  przygotujP4 = przygotujP3, przygotujP5 = TRUE,
  przygotujP6 = rokMonitoringu >= 2026L, przygotujP7 = rokMonitoringu >= 2026L,
  przygotujP8 = rokMonitoringu >= 2026L, przygotujP9 = rokMonitoringu >= 2026L,
  zapisujPoKazdejTabeliPosredniej = FALSE)
{
  stopifnot(is.list(baza) | inherits(baza, "DBIConnection"),
            is.numeric(minDniEdukacjiWMiesiacu),
            length(minDniEdukacjiWMiesiacu) == 1L,
            as.integer(minDniEdukacjiWMiesiacu) == minDniEdukacjiWMiesiacu,
            minDniEdukacjiWMiesiacu > 0L,
            length(przygotujP1) == 1L, przygotujP1 %in% c(FALSE, TRUE),
            length(przygotujP2) == 1L, przygotujP2 %in% c(FALSE, TRUE),
            length(przygotujP3) == 1L, przygotujP3 %in% c(FALSE, TRUE),
            length(przygotujP4) == 1L, przygotujP4 %in% c(FALSE, TRUE),
            length(przygotujP5) == 1L, przygotujP5 %in% c(FALSE, TRUE),
            length(zapisujPoKazdejTabeliPosredniej) == 1L,
            zapisujPoKazdejTabeliPosredniej %in% c(FALSE, TRUE))
  if (przygotujP3 & !przygotujP1) {
    stop("Tabela P3 nie może zostać przygotowana bez przygotowania tabeli P1.")
  }
  if (przygotujP4 & !przygotujP1) {
    stop("Tabela P4 nie może zostać przygotowana bez przygotowania tabeli P1.")
  }
  if (przygotujP4 & !przygotujP2) {
    stop("Tabela P4 nie może zostać przygotowana bez przygotowania tabeli P2.")
  }
  if (przygotujP4 & !przygotujP3) {
    stop("Tabela P4 nie może zostać przygotowana bez przygotowania tabeli P3.")
  }
  if (is.list(baza)) {
    con = do.call(dbConnect, args = baza)
  } else {
    stopifnot(dbIsValid(baza))
    con = baza
  }
  lataWDanych <- tbl(con, "w1") %>%
    pull("rok_abs") %>%
    unique() %>%
    sort()
  if (is.na(rokMonitoringu)) {
    rokMonitoringu <- max(lataWDanych) + 1L
  } else {
    stopifnot(is.numeric(rokMonitoringu), length(rokMonitoringu) == 1L,
              as.integer(rokMonitoringu) == rokMonitoringu)
    rokMonitoringu <- as.integer(rokMonitoringu)
    if (rokMonitoringu %in% lataWDanych) {
      warning("Wartość argumentu `rokMonitoringu` występuje wśród wartości kolumny `rok_abs` tabeli z danymi wejściowymi W1. Czy na pewno wskazuje ona rok prowadzenia monitoringu?", immediate. = TRUE)
    } else if (rokMonitoringu != (max(lataWDanych) + 1)) {
      warning("Wartość argumentu `rokMonitoringu` nie jest równa `max(w1$rok_abs) + 1`. Czy na pewno wskazuje ona rok prowadzenia monitoringu?", immediate. = TRUE)
    }
  }
  stopifnot(is.numeric(okresyP2), all(as.integer(okresyP2) == okresyP2),
            inherits(imputujDateKoncaSzkoly, "Date"),
            length(imputujDateKoncaSzkoly) == 1L,
            inherits(imputujDateKoncaStudiow, "Date"),
            length(imputujDateKoncaStudiow) == 1L,
            inherits(imputujDateKoncaAdresu, "Date"),
            length(imputujDateKoncaAdresu) == 1L,
            length(przygotujP6) == 1L, przygotujP6 %in% c(FALSE, TRUE),
            length(przygotujP7) == 1L, przygotujP7 %in% c(FALSE, TRUE),
            length(przygotujP8) == 1L, przygotujP8 %in% c(FALSE, TRUE),
            length(przygotujP9) == 1L, przygotujP9 %in% c(FALSE, TRUE))
  if ((przygotujP7 | przygotujP8 | przygotujP9) & rokMonitoringu < 2026L) {
    message("Tabele P7, P8 i P9 wymagają danych dostępnych dopiero od edycji monitoringu 2026, więc nie zostaną przygotowane.")
    przygotujP7 <- przygotujP8 <- przygotujP9 <- FALSE
  }
  okresyP2 <- as.integer(okresyP2)
  minDniEdukacjiWMiesiacu <- as.integer(minDniEdukacjiWMiesiacu)
  on.exit({if (!inherits(baza, "DBIConnection")) dbDisconnect(con)})

  tabelePosrednie <- list()
  cat("\nStart: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")
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
  # P6 - bez względu na to, czy ma być zwrócona, czy nie
  szkW2 <- tbl(con, "w2") %>%
    pull("id_szk") %>%
    unique()
  if (rokMonitoringu < 2026L) {
    t26 <- tbl(con, "w26b") %>%
      rename(typ_szk_rspo = "typ_szk") %>%
      mutate(szk_ma_abs = .data$id_szk %in% szkW2,
             typ_szk =
               case_when(.data$typ_szk_rspo == "Bednarska Szkoła Realna" ~ "Liceum ogólnokształcące",
                         .data$typ_szk_rspo == "Liceum ogólnokształcące" &
                           .data$kategoria_uczniow == "Dorośli" ~ "Liceum ogólnokształcące dla dorosłych",
                         .default = .data$typ_szk_rspo),
             typ_szk_sdd =
               ifelse(.data$specyfika != "brak specyfiki" &
                        .data$typ_szk != "Szkoła specjalna przysposabiająca do pracy",
                      paste0(.data$typ_szk, " - ", .data$specyfika),
                      .data$typ_szk),
             teryt_pow_szk = 100L*floor(.data$teryt_gmi_szk / 1000)) %>%
      select("id_szk", "szk_ma_abs", "typ_szk", "typ_szk_sdd", "typ_szk_rspo",
             everything()) %>%
      left_join(tbl(con, "w26"),
                by = "id_szk")
  } else {
    t26 <- tbl(con, "w26a") %>%
      left_join(tbl(con, "w26"),
                by = "id_szk") %>%
      rename(typ_szk_rspo = "typ_szk") %>%
      mutate(rok_szk = as.character(.data$rok_szk),
             szk_ma_abs = .data$id_szk %in% szkW2,
             typ_szk =
               case_when(.data$typ_szk_rspo == "Bednarska Szkoła Realna" ~ "Liceum ogólnokształcące",
                         .data$typ_szk_rspo == "Liceum ogólnokształcące" &
                           .data$kategoria_uczniow == "Dorośli" ~ "Liceum ogólnokształcące dla dorosłych",
                         .default = .data$typ_szk_rspo),
             typ_szk_sdd =
               ifelse(.data$specyfika != "brak specyfiki" &
                        .data$typ_szk != "Szkoła specjalna przysposabiająca do pracy",
                      paste0(.data$typ_szk, " - ", .data$specyfika),
                      .data$typ_szk),
             ulica = ifelse(is.na(.data$nr_budynku),
                            .data$ulica, paste(.data$ulica, .data$nr_budynku)),
             teryt_pow_szk = 100L*floor(.data$teryt_gmi_szk / 1000),
             wojewodztwo_szk = tolower(.data$wojewodztwo_szk)) %>%
      select("id_szk", "szk_ma_abs", "typ_szk", "typ_szk_sdd", "typ_szk_rspo",
             "rok_szk", "nazwa_szk", "teryt_gmi_szk", "teryt_pow_szk",
             everything(), -"nr_budynku")
    t30 <- left_join(tbl(con, "w30a"),
                     tbl(con, "w30"),
                     by = "id_podmiotu") %>%
      mutate(rok_akad = as.character(.data$rok_akad),
             miejsce_w_strukt = ifelse(.data$id_podmiotu == .data$id_uczelni,
                                       "uczelnia", "filia uczelni"),
             jedn_nadrz_typ = ifelse(.data$id_podmiotu == .data$id_uczelni,
                                     NA_character_, "Uczelnia"),
             id_uczelni = ifelse(.data$id_podmiotu == .data$id_uczelni,
                                 NA_character_, .data$id_uczelni),
             szk_ma_abs = FALSE,
             typ_szk = "Uczelnia",
             typ_szk_sdd = "Uczelnia",
             publicznosc = case_when(
               .data$rodzaj_uczelni %in% c("Uczelnia publiczna",
                                           "Filia uczelni publicznej") ~ "publiczna",
               .data$rodzaj_uczelni %in% c("Uczelnia niepubliczna",
                                           "Filia uczelni niepublicznej") ~ "niepubliczna",
               .data$rodzaj_uczelni %in% c("Uczelnia kościelna",
                                           "Filia uczelni kościelnej") ~ "kościelna")) %>%
      select("szk_ma_abs", id_ucz = "id_podmiotu", "typ_szk", "typ_szk_sdd",
             rok_szk = "rok_akad", nazwa_szk = "nazwa_podmiotu",
             typ_ucz = "typ_podmiotu", kraj_ucz = "kraj_podmiotu",
             teryt_pow_szk = "teryt_pow_podmiotu",
             wojewodztwo_szk = "wojewodztwo_podmiotu",
             powiat_szk = "powiat_podmiotu",
             rodzaj_gmi_ucz = "rodzaj_gminy_podmiotu", "ulica", "pna", "poczta",
             organ_prowadzacy_typ = "organ_nadzorujacy_typ",
             organ_prowadzacy_nazwa = "organ_nadzorujacy",
             "publicznosc",
             "miejsce_w_strukt", podm_nadrz_id = "id_uczelni", "jedn_nadrz_typ")
  }
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
  # zrzucanie daty zakończenia studiów do jednej kolumny #######################
  if (przygotujP1 | przygotujP2 | przygotujP3 | przygotujP4) {
    t12 <- tbl(con, "w12") %>%
      mutate(data_do_stu =
               case_when(.data$czy_ukoncz_stu %in% 1L ~ .data$data_do_stu,
                         .data$czy_ukoncz_stu %in% 2L ~ .data$data_skresl_stu,
                         .default = imputujDateKoncaStudiow))
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
    if ("rok_egz_zaw" %in% colnames(tbl(con, "w810"))) {
      t810 <- tbl(con, "w810") %>%
        rename(rok_kwal = "rok_egz_zaw")
    } else {
      t810 <- tbl(con, "w810")
    }
    t810 <- t2 %>%
      inner_join(t810 %>%
                   filter(!is.na(.data$dok_potw_kwal)) %>%
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
      inner_join(t12 %>%
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
             "dyscyplina_wiodaca", id_ucz = any_of("id_podmiotu"),
             any_of("kierunek_stu"))
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
    rm(t6, t7, t810, t911, t12d)
    if (zapisujPoKazdejTabeliPosredniej) save(tabelePosrednie, file = "tabele-posrednie-temp.RData")
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
    nazwyZawodow <- bind_rows(tbl(con, "w2") %>%
                                select("kod_zaw", "nazwa_zaw") %>%
                                distinct() %>%
                                collect(),
                              tbl(con, "w3") %>%
                                select(kod_zaw = "kod_zaw_kont",
                                       nazwa_zaw = any_of("nazwa_zaw_kont")) %>%
                                distinct() %>%
                                collect()) %>%
      distinct() %>%
      add_count(.data$kod_zaw, name = "lNazw") %>%
      filter(.data$lNazw == 1L, !is.na(.data$nazwa_zaw)) %>%
      select(-"lNazw")
    # szkoły objęte SIO
    t3c <- t2 %>%
      inner_join(tbl(con, "w3") %>%
                   rename(kod_zaw = "kod_zaw_kont") %>%
                   left_join(t20, by = "kod_zaw") %>%
                   select("id_abs", "rok_abs", "id_szk_kont",
                          "data_od_szk_kont", "data_do_szk_kont",
                          kod_zaw_kont = "kod_zaw", any_of("nazwa_zaw_kont"),
                          branza_kont = "branza") %>%
                   mutate(data_do_szk_kont = if_else(is.na(.data$data_do_szk_kont),
                                                     imputujDateKoncaSzkoly,
                                                     .data$data_do_szk_kont),
                          forma_kont = "uczeń") %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      mutate(okres_od_szk_kont = 12L*year(.data$data_od_szk_kont) + month(.data$data_od_szk_kont),
             okres_do_szk_kont = 12L*year(.data$data_do_szk_kont) + month(.data$data_do_szk_kont)) %>%
      collect() %>%
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
                   rename(kod_kwal = "kod_kwal_kkz") %>%
                   left_join(tbl(con, "w21"), by = "kod_kwal") %>%
                   left_join(t20, by = "kod_zaw") %>%
                   select("id_abs", "rok_abs", "id_szk_kont",
                          "data_od_kkz", "data_do_kkz",
                          kod_zaw_kont = "kod_zaw", branza_kont = "branza") %>%
                   mutate(forma_kont = "KKZ") %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      filter(!is.na(.data$branza_kont)) %>%
      mutate(okres_od_kkz = 12L*year(.data$data_od_kkz) + month(.data$data_od_kkz),
             okres_do_kkz = 12L*year(.data$data_do_kkz) + month(.data$data_do_kkz)) %>%
      collect() %>%
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
                   rename(kod_zaw = "kod_zaw_kuz") %>%
                   left_join(t20, by = "kod_zaw")  %>%
                   select("id_abs", "rok_abs", "id_szk_kont",
                          "data_od_kuz", "data_do_kuz",
                          kod_zaw_kont = "kod_zaw", branza_kont = "branza") %>%
                   mutate(forma_kont = "KUZ") %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      filter(!is.na(.data$branza_kont)) %>%
      mutate(okres_od_kuz = 12L*year(.data$data_od_kuz) + month(.data$data_od_kuz),
             okres_do_kuz = 12L*year(.data$data_do_kuz) + month(.data$data_do_kuz)) %>%
      collect() %>%
      inner_join(okresyP2,
                 by = join_by(x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_kuz, x$okres_do_kuz,
                                       y$okres, y$okres))) %>%
      select(-"data_od_kuz", -"data_do_kuz", -"okres_od_kuz", -"okres_do_kuz") %>%
      distinct() %>%
      mutate(zrodlo = "W5")
    # studia
    if (rokMonitoringu < 2026L) {
      t12c <- t2 %>%
        inner_join(t12 %>%
                     left_join(tbl(con, "w13") %>%
                                 filter(.data$czy_dyscyplina_wiodaca),
                               by = "id_kierunku_stu") %>%
                     select("id_abs", "rok_abs", "data_od_stu", "data_do_stu",
                            dziedzina_kont = "dziedzina",
                            dyscyplina_wiodaca_kont = "dyscyplina") %>%
                     mutate(typ_szk_kont = "Studia",
                            forma_kont = "student") %>%
                     distinct(),
                   by = c("id_abs", "rok_abs")) %>%
        collect()
    } else {
      t12c <- t2 %>%
        inner_join(t12 %>%
                     left_join(tbl(con, "w13") %>%
                                 filter(.data$czy_dyscyplina_wiodaca),
                               by = "id_kierunku_stu") %>%
                     select("id_abs", "rok_abs", "data_od_stu", "data_do_stu",
                            id_ucz_kont = "id_podmiotu",
                            nazwa_zaw_kont = "kierunek_stu",
                            dziedzina_kont = "dziedzina",
                            dyscyplina_wiodaca_kont = "dyscyplina",
                            forma_kont = "forma_stu") %>%
                     mutate(typ_szk_kont = case_when(
                              .data$forma_kont %in% "S" ~ "Studia stacjonarne",
                              .data$forma_kont %in% "N" ~ "Studia niestacjonarne"),
                            forma_kont = "student") %>%
                     distinct(),
                   by = c("id_abs", "rok_abs")) %>%
        collect()
    }
    t12c <- t12c %>%
      mutate(okres_od_stu = data2okres(.data$data_od_stu),
             okres_do_stu = data2okres(.data$data_do_stu)) %>%
      inner_join(okresyP2,
                 by = join_by(x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_stu, x$okres_do_stu,
                                       y$okres, y$okres))) %>%
      select(-"data_od_stu", -"data_do_stu", -"okres_od_stu", -"okres_do_stu") %>%
      distinct() %>%
      mutate(zrodlo = "W12")
    if (rokMonitoringu >= 2026L) {
      t12c <- t12c %>%
        mutate(rok_szk = okres2rokszk(.data$okres, akademicki = TRUE)) %>%
        left_join(t30 %>%
                    select(id_ucz_kont = "id_ucz", "rok_szk",
                           teryt_pow_kont = "teryt_pow_szk",
                           nazwa_szk_kont = "nazwa_szk") %>%
                    collect(),
                  by = c("id_ucz_kont", "rok_szk")) %>%
        select(-"rok_szk")
    }
    # łączenie i wczytywanie do P2
    tabelePosrednie$p2 <- bind_rows(t3c, t4c, t5c)
    if (!("nazwa_zaw_kont" %in% names(tabelePosrednie$p2))) {
      tabelePosrednie$p2$nazwa_zaw_kont <- NA_character_
    }
    tabelePosrednie$p2 <- tabelePosrednie$p2 %>%
      mutate(rok_szk = okres2rokszk(.data$okres, akademicki = FALSE)) %>%
      left_join(t26 %>%
                  mutate(teryt_pow_szk =
                           100L*floor(.data$teryt_gmi_szk / 1000L)) %>%
                  select(id_szk_kont = "id_szk", "rok_szk",
                         typ_szk_kont = "typ_szk",
                         teryt_pow_kont = "teryt_pow_szk",
                         nazwa_szk_kont = "nazwa_szk") %>%
                  collect(),
                by = c("id_szk_kont", "rok_szk")) %>%
      left_join(nazwyZawodow %>%
                  select(kod_zaw_kont = "kod_zaw",
                         nazwa_zaw_kont2 = "nazwa_zaw"),
                by = "kod_zaw_kont") %>%
      mutate(nazwa_zaw_kont = coalesce(.data$nazwa_zaw_kont,
                                       .data$nazwa_zaw_kont2)) %>%
      select("id_abs", "rok_abs", "kod_zaw", "branza",
             "rok", "miesiac", "mies_od_ukoncz", "okres", "id_szk_kont",
             "typ_szk_kont", "forma_kont", "teryt_pow_kont", "nazwa_szk_kont",
             "kod_zaw_kont", "nazwa_zaw_kont", "branza_kont", "zrodlo") %>%
      bind_rows(t12c) %>%
      group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
      arrange(.data$id_abs, .data$rok_abs, .data$okres, .data$kod_zaw) %>%
      mutate(lp_kont = seq_len(n()),
             teryt_pow_kont = .data$teryt_pow_kont / 100L,
             forma_kont = factor(.data$forma_kont,
                                 unique(c("uczeń", "KKZ", "KUZ", "student"),
                                        sort(unique(.data$forma_kont))))) %>%
      ungroup() %>%
      select("id_abs":"okres", "lp_kont", "zrodlo",
             "id_szk_kont", "id_ucz_kont", everything())
    rm(t3c, t4c, t5c, t12c)
    if (zapisujPoKazdejTabeliPosredniej) save(tabelePosrednie, file = "tabele-posrednie-temp.RData")
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # Półfabrykaty do P3 i P5 ####################################################
  if (przygotujP3 | przygotujP5 | przygotujP7) {
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
    if (rokMonitoringu >= 2026) {
      # rekordy opisujące urlop macierzyński, którym należy zmienić `udzial_dni_zatr_mies` na NA
      # (bo współwystępują z inną skladką opisującą pracę u tego samego pracodawcy)
      tMacierz <- t16 %>%
        filter(.data$kod_zus == 1240L) %>%
        semi_join(t16 %>%
                    filter(.data$kod_zus != 1240L,
                           .data$etat | .data$netat | .data$inne | .data$samoz),
                  by = c("id_abs", "rok_abs", "rok_skladka", "mies_skladka",
                         "id_platnika")) %>%
        select("id_abs", "rok_abs", "rok_skladka", "mies_skladka",
               "id_platnika", "kod_zus", "lp") %>%
        mutate(udzial_dni_zatr_mies_na_bd = TRUE)
      t16 <- t16 %>%
        left_join(tMacierz,
                  by = c("id_abs", "rok_abs", "rok_skladka", "mies_skladka",
                         "id_platnika", "kod_zus", "lp")) %>%
        mutate(udzial_dni_zatr_mies_na_bd = # backend bazodanowy nie lubi się z przyrównaniami do NA, nawet w formie %in%, więc należy je najpierw pozamieniać na FALSE
                 ifelse(is.na(.data$udzial_dni_zatr_mies_na_bd),
                        FALSE, .data$udzial_dni_zatr_mies_na_bd),
               udzial_dni_zatr_mies =
                 ifelse(.data$udzial_dni_zatr_mies_na_bd,
                        NA_real_,
                        .data$udzial_dni_zatr_mies),
               wymiar_zatr = ifelse(!.data$etat, NA_real_, .data$wymiar_zatr)) %>%
        select(-"udzial_dni_zatr_mies_na_bd")
    }
  }
  # P3 (dane miesięczne) #######################################################
  if (przygotujP3) {
    cat("\nPrzygotowywanie tabeli P3 (na danych z edycji ≥2026 może zająć 3-4 h ):")
    cat("\n- tworzenie wzorcowej struktury danych...")
    t1 <- tbl(con, "w1") %>%
      select(-any_of("kraj_pochodzenia")) %>%
      left_join(tbl(con, "w14") %>%
                  mutate(okres_zgonu = 12L*.data$rok_zgonu + .data$mies_zgonu) %>%
                  select("id_abs", "rok_abs", "okres_zgonu"),
                by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      right_join(wszystkieOkresy,
                 by = "rok_abs") %>%
      mutate(zmarl = if_else(is.na(.data$okres_zgonu),
                             FALSE, .data$okres > .data$okres_zgonu)) %>%
      select(-"okres_zgonu")
    t1db <- copy_to(con, t1, overwrite = TRUE)
    cat(" zakończone; ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"),
        "\n- przetwarzanie danych o kontynuowaniu nauki...", sep = "")
    # dane o kontynuacji nauki w szkołach objętych SIO przerobione na miesiące
    t3m <- tbl(con, "w3") %>%
      select("id_abs", "rok_abs", id_szk = "id_szk_kont",
             "data_od_szk_kont", "data_do_szk_kont") %>%
      mutate(data_od_szk_kont =
               as.Date(.data$data_od_szk_kont + minDniEdukacjiWMiesiacu),
             data_do_szk_kont = as.Date(
               if_else(is.na(.data$data_do_szk_kont),
                       imputujDateKoncaSzkoly,
                       .data$data_do_szk_kont) - minDniEdukacjiWMiesiacu),
             zrodlo = "W3") %>%
      collect()
    # dane o kontynuacji nauki na KKZ przerobione na miesiące
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t4m <- tbl(con, "w4") %>%
      filter(!is.na(.data$data_do_kkz)) %>%
      select("id_abs", "rok_abs", id_szk = "id_szk_kont",
             data_od_szk_kont = "data_od_kkz",
             data_do_szk_kont = "data_do_kkz") %>%
      mutate(data_od_szk_kont =
               as.Date(.data$data_od_szk_kont + minDniEdukacjiWMiesiacu),
             data_do_szk_kont =
               as.Date(.data$data_do_szk_kont - minDniEdukacjiWMiesiacu),
             zrodlo = "W4") %>%
      collect()
    # dane o kontynuacji nauki na KUZ przerobione na miesiące
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t5m <- tbl(con, "w5") %>%
      filter(!is.na(.data$data_do_kuz)) %>%
      select("id_abs", "rok_abs", id_szk = "id_szk_kont",
             data_od_szk_kont = "data_od_kuz",
             data_do_szk_kont = "data_do_kuz") %>%
      mutate(data_od_szk_kont =
               as.Date(.data$data_od_szk_kont + minDniEdukacjiWMiesiacu),
             data_do_szk_kont =
               as.Date(.data$data_do_szk_kont - minDniEdukacjiWMiesiacu),
             zrodlo = "W5") %>%
      collect()
    # laczenie - dane z SIO
    t345m <- bind_rows(t3m, t4m, t5m) %>%
      # tylko nauka, która trwała dłużej niż minDniEdukacjiWMiesiacu
      filter((.data$data_do_szk_kont - .data$data_od_szk_kont + minDniEdukacjiWMiesiacu) > 0) %>%
      mutate(okres_od_szk_kont = data2okres(.data$data_od_szk_kont),
             okres_do_szk_kont = data2okres(.data$data_do_szk_kont)) %>%
      inner_join(wszystkieOkresy,
                 by = join_by(x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_szk_kont, x$okres_do_szk_kont,
                                       y$okres, y$okres))) %>%
      mutate(rok_szk = okres2rokszk(.data$okres, akademicki = FALSE)) %>%
      left_join(t26 %>%
                  select("id_szk", "rok_szk", typ_szk_kont = "typ_szk_sdd",
                         teryt_pow_szk_kont = "teryt_pow_szk") %>%
                  collect(),
                by = c("id_szk", "rok_szk")) %>%
      mutate(typ_szk_kont = case_when(.data$zrodlo == "W4" ~ "KKZ",
                                      .data$zrodlo == "W5" ~ "KUZ",
                                      .default = .data$typ_szk_kont)) %>%
      select(-c("id_szk", "rok_szk", "zrodlo"))
    rm(t3m, t4m, t5m)
    # dane o kontynuacji nauki na studiach przerobione na miesiące
    if (rokMonitoringu < 2026L) {
      t12m <- t12 %>%
        select("id_abs", "rok_abs",
               data_od_szk_kont = "data_od_stu",
               data_do_szk_kont = "data_do_stu") %>%
        mutate(typ_szk_kont = "studia")
    } else {
      t12m <- t12 %>%
        select("id_abs", "rok_abs", id_ucz = "id_podmiotu",
               data_od_szk_kont = "data_od_stu",
               data_do_szk_kont = "data_do_stu",
               typ_szk_kont = "forma_stu") %>%
        mutate(typ_szk_kont =
                 case_when(.data$typ_szk_kont %in% "S" ~ "Studia stacjonarne",
                           .data$typ_szk_kont %in% "N" ~ "Studia niestacjonarne"))
    }
    t12m <- t12m %>%
      mutate(data_od_szk_kont = as.Date(.data$data_od_szk_kont + minDniEdukacjiWMiesiacu),
             data_do_szk_kont = as.Date(
               if_else(is.na(.data$data_do_szk_kont),
                       imputujDateKoncaStudiow,
                       .data$data_do_szk_kont) - minDniEdukacjiWMiesiacu)) %>%
      # tylko nauka, która trwała dłużej niż minDniEdukacjiWMiesiacu
      filter((.data$data_do_szk_kont - .data$data_od_szk_kont + minDniEdukacjiWMiesiacu) > 0) %>%
      mutate(okres_od_szk_kont = 12L*year(.data$data_od_szk_kont) + month(.data$data_od_szk_kont),
             okres_do_szk_kont = 12L*year(.data$data_do_szk_kont) + month(.data$data_do_szk_kont)) %>%
      select(-starts_with("data_")) %>%
      collect() %>%
      inner_join(wszystkieOkresy,
                 by = join_by(x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_szk_kont, x$okres_do_szk_kont,
                                       y$okres, y$okres)))
    if (rokMonitoringu >= 2026L) {
      t12m <- t12m %>%
        mutate(rok_szk = okres2rokszk(.data$okres, akademicki = TRUE)) %>%
        left_join(t30 %>%
                    select("id_ucz", "rok_szk",
                           teryt_pow_szk_kont = "teryt_pow_szk") %>%
                    collect(),
                  by = c("id_ucz", "rok_szk")) %>%
        select(-c("id_ucz", "rok_szk"))
    }
    # agregacja form kontynuacji nauki (5-10 minut na danych z edycji 2026)
    typySzkol <- c("Branżowa szkoła I stopnia",
                   "Branżowa szkoła I stopnia - specjalna",
                   "Branżowa szkoła II stopnia",
                   "Branżowa szkoła II stopnia - specjalna",
                   "Technikum",
                   "Technikum - specjalna",
                   "Liceum ogólnokształcące dla dorosłych",
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
                   "Studia stacjonarne",
                   "Studia niestacjonarne",
                   "KKZ",
                   "KUZ")
    t34512m <- bind_rows(t345m, t12m) %>%
      select("id_abs", "rok_abs", "okres", "typ_szk_kont") %>%
      distinct() %>%
      mutate(typ_szk_kont =
               factor(.data$typ_szk_kont, # poniżej radzenie sobie z sytuacją, że w jakimś z poniższych typów nikt jednak nie kontynuował nauki
                      levels = sort(unique(c(.data$typ_szk_kont, typySzkol)))),
             macierz_typ =
               matrix(diag(nlevels(.data$typ_szk_kont)),
                      nrow = nlevels(.data$typ_szk_kont),
                      dimnames = list(levels(.data$typ_szk_kont),
                                      levels(.data$typ_szk_kont)))[.data$typ_szk_kont, ]) %>%
      add_count(.data$id_abs, .data$rok_abs, .data$okres,
                name = "lFormKontynuacji")
    t34512m <-
      bind_rows(t34512m %>% # dla wydajności
                  filter(.data$lFormKontynuacji == 1L) %>%
                  select("id_abs", "rok_abs", "okres", "macierz_typ"),
                t34512m %>% # dla wydajności
                  filter(.data$lFormKontynuacji > 1L) %>%
                  group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
                  summarise(macierz_typ = matrix(colSums(.data$macierz_typ),
                                                 nrow = 1),
                            .groups = "drop")) %>%
      mutate(macierz_typ =
               matrix(.data$macierz_typ > 0L, nrow = n(),
                      dimnames = list(NULL, colnames(t34512m$macierz_typ))))
    # agregacja powiatów kontynuowania nauki (30-40 min. na danych z edycji 2026)
    t34512mp <- bind_rows(t345m, t12m) %>%
      select("id_abs", "rok_abs", "okres", "typ_szk_kont", "teryt_pow_szk_kont") %>%
      add_count(.data$id_abs, .data$rok_abs, .data$okres, name = "lKontynuacji")
    t34512mp <-
      bind_rows(t34512mp %>% # dla wydajności
                  filter(.data$lKontynuacji == 1L) %>%
                  select("id_abs", "rok_abs", "okres", "teryt_pow_szk_kont"),
                t34512mp %>% # dla wydajności
                  filter(.data$lKontynuacji > 1L) %>%
                  # preferuję najczęście występujący powiat wśród studiów
                  # stacjonarnych, w dalszej kolejności niestacjonarnych,
                  # w dalszej wśród jakichkolwiek szk\oł i w końcu wśród KUZ/KKZ
                  mutate(waga = case_when(
                    .data$typ_szk_kont == "Studia stacjonarne" ~ 1000000,
                    .data$typ_szk_kont %in% c("studia", "Studia niestacjonarne") ~ 10000,
                    .data$typ_szk_kont %in% c("KKZ", "KUZ") ~ 1,
                    .default = 100)) %>%
                  count(.data$id_abs, .data$rok_abs, .data$okres,
                        .data$teryt_pow_szk_kont,
                        wt = .data$waga, name = "waga") %>%
                  group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
                  slice_max(order_by = .data$waga, n = 1L, with_ties = TRUE) %>%
                  ungroup() %>%
                  # jeśli nie udało się rozstrzygnąć, to niech będziie brak danych
                  add_count(.data$id_abs, .data$rok_abs, .data$okres,
                            name = "liczbaPowiatow") %>%
                  filter(.data$liczbaPowiatow == 1L) %>%
                  select("id_abs", "rok_abs", "okres", "teryt_pow_szk_kont"))
    t34512m <- t34512m %>%
      left_join(t34512mp,
                by = c("id_abs", "rok_abs", "okres"))
    rm(t345m, t12m, t34512mp)
    # tworzenie docelowych zmiennych opisujących kontynuację nauki
    t34512m <- t34512m %>%
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
               .data$macierz_typ[, "Liceum ogólnokształcące dla dorosłych"] ~ 1L,
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
             nauka_studia2 = case_when(
               .data$macierz_typ[, "Studia stacjonarne"] ~ 1L,
               .data$macierz_typ[, "Studia niestacjonarne"] ~ 2L,
               .default = 0L),
             nauka_kkz = as.integer(.data$macierz_typ[, "KKZ"]),
             nauka_kuz = as.integer(.data$macierz_typ[, "KUZ"])) %>%
      select(-"macierz_typ")
    if (rokMonitoringu < 2026L) {
      t34512m <- t34512m %>%
        select(-"nauka_studia2")
    } else {
      t34512m <- t34512m %>%
        select(-"nauka_studia") %>%
        rename(nauka_studia = "nauka_studia2")
    }
    cat(" zakończone; ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"),
        "\n- przetwarzanie danych o dyplomach/certfikatach...", sep = "")
    # informacje o uzyskaniu dyplomów/certyfikatów
    tDyplZaw <- tabelePosrednie$p1 %>%
      filter(!is.na(.data$kod_zaw),
             .data$rodzaj_dyplomu %in% c("certyfikat kwalifikacji",
                                         "dyplom zawodowy",
                                         "tytuł czeladnika"),
             .data$rok >= .data$rok_abs | .data$rodzaj_dyplomu != "dyplom zawodowy",
             .data$kod_zaw_dyplom == .data$kod_zaw) %>%
      mutate(rodzaj_dyplomu = factor(.data$rodzaj_dyplomu,
                                     c("certyfikat kwalifikacji",
                                       "dyplom zawodowy",
                                       "tytuł czeladnika"),
                                     c("Tylko certyfikat kwalifikacji",
                                       "Dyplom zawodowy",
                                       "Świadectwo czeladnicze")),
             okres = ifelse(.data$rodzaj_dyplomu == "Świadectwo czeladnicze",
                            rokmiesiac2okres(.data$rok_abs, 6L),
                            .data$okres)) %>%
      arrange(.data$id_abs, .data$rok_abs, .data$okres) %>%
      group_by(.data$id_abs, .data$rok_abs) %>%
      mutate(dyplom_zaw = levels(.data$rodzaj_dyplomu)[cummax(as.integer(.data$rodzaj_dyplomu))]) %>%
      ungroup() %>%
      select("id_abs", "rok_abs", "okres", "dyplom_zaw")
    # informacje o zdaniu matury
    tMatura <- tabelePosrednie$p1 %>%
      filter(.data$rodzaj_dyplomu == "matura") %>%
      select("id_abs", "rok_abs", "okres", matura = "rodzaj_dyplomu") %>%
      distinct()
    cat(" zakończone; ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"),
        "\n- przetwarzanie danych z ZUS (i KRUS):", sep = "")
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
      inner_join(t1db,
                 by = join_by(x$id_abs == y$id_abs, x$rok_abs == y$rok_abs,
                              overlaps(x$okres_od_adr, x$okres_do_adr,
                                       y$okres, y$okres))) %>%
      filter(!is.na(.data$teryt),
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
                dane_z_zus_przerwy = TRUE,
                .groups = "drop")
    if (rokMonitoringu >= 2026L) {
      t33 <- tbl(con, "w33") %>%
        mutate(okres_od_swiadcz =
                 12L*year(.data$data_od_swiadcz) + month(.data$data_od_swiadcz),
               okres_do_swiadcz =
                 12L*year(.data$data_do_swiadcz) + month(.data$data_do_swiadcz)) %>%
        inner_join(t1db,
                   by = join_by(x$id_abs == y$id_abs, x$rok_abs == y$rok_abs,
                                overlaps(x$okres_od_swiadcz, x$okres_do_swiadcz,
                                         y$okres, y$okres))) %>%
        select("id_abs", "rok_abs", "okres", "rodzaj_swiadcz") %>%
        group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
        summarise(choroba_krus =
                    as.integer(any(.data$rodzaj_swiadcz %in% c(1L, 3L), na.rm = TRUE)),
                  wypadek_krus =
                    as.integer(any(.data$rodzaj_swiadcz %in% c(2L, 4L), na.rm = TRUE)),
                  dane_z_krus_swiadczenia = TRUE,
                  .groups = "drop")
      t17 <- t17 %>%
        full_join(t33,
                  by = c("id_abs", "rok_abs", "okres")) %>%
        mutate(wypadek = case_when(.data$wypadek == 1L & .data$wypadek_krus == 1L ~ 3L,
                                   .data$wypadek == 1L ~ 1L,
                                   .data$wypadek_krus == 1L ~ 2L,
                                   .default = 0L),
               choroba = case_when(.data$choroba == 1L & .data$choroba_krus == 1L ~ 3L,
                                   .data$choroba == 1L ~ 1L,
                                   .data$choroba_krus == 1L ~ 2L,
                                   .default = 0L)) %>%
        select(-c("wypadek_krus", "choroba_krus"))
    } else {
      t17 <- t17 %>%
        mutate(dane_z_krus_swiadczenia = FALSE)
    }
    # składki ZUS
    if (rokMonitoringu < 2026L) {
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
          dane_z_zus_skladki = TRUE,
          dane_z_krus_skladki = FALSE,
          .groups = "drop")
    } else {
      t32 <- tbl(con, "w32") %>%
        mutate(okres_od_ubezp =
                 12L*year(.data$data_od_ubezp) + month(.data$data_od_ubezp),
               okres_do_ubezp =
                 12L*year(.data$data_do_ubezp) + month(.data$data_do_ubezp)) %>%
        inner_join(t1db,
                   by = join_by(x$id_abs == y$id_abs, x$rok_abs == y$rok_abs,
                                overlaps(x$okres_od_ubezp, x$okres_do_ubezp,
                                         y$okres, y$okres))) %>%
        select("id_abs", "rok_abs", "okres", ubezp_krus = "rola_ubezp") %>%
        mutate(ubezp_krus =
                 ifelse(is.na(.data$ubezp_krus), 9L, .data$ubezp_krus)) %>%
        group_by(.data$id_abs, .data$rok_abs, .data$okres) %>%
        summarise(ubezp_krus = min(.data$ubezp_krus, na.rm = TRUE),
                  dane_z_krus_skladki = TRUE,
                  .groups = "drop")
      t16m <- t16 %>%
        group_by(.data$id_abs, .data$rok_abs, .data$rok_skladka, .data$mies_skladka) %>%
        summarise(
          okres = 12L*.data$rok_skladka + .data$mies_skladka,
          praca = case_when( # uwaga, kawałek dalej dodawany jest jeszcze kod 9 na podstawie danych z KRUS!
            any(.data$etat, na.rm = TRUE) & any(.data$samoz, na.rm = TRUE) &
              any(.data$netat | .data$inne, na.rm = TRUE) ~ 7L, # zatrudnienie na podstawie umowy o pracę i działalność gospodarcza i zatrudnienie w innej formie
            any(.data$samoz, na.rm = TRUE) & any(.data$netat | .data$inne, na.rm = TRUE) ~ 6L, # działalność gospodarcza i zatrudnienie w innej formie
            any(.data$etat, na.rm = TRUE) & any(.data$netat | .data$inne, na.rm = TRUE) ~ 5L, # zatrudnienie na podstawie umowy o pracę i zatrudnienie w innej formie
            any(.data$etat, na.rm = TRUE) & any(.data$samoz, na.rm = TRUE) ~ 4L, # zatrudnienie na podstawie umowy o pracę i działalność gospodarcza
            any(.data$etat, na.rm = TRUE) ~ 1L, # zatrudnienie na podstawie umowy o pracę, brak innej formy,
            any(.data$samoz, na.rm = TRUE) ~ 2L, # działalność gospodarcza (samozatrudnienie), brak innej formy
            any(.data$netat | .data$inne, na.rm = TRUE) ~ 3L, # zatrudnienie w innej formie (umowa zlecenie, o dzieło, …), brak zatrudnienia na etacie ani działalności gospodarczej
            .default = 0L), # poniżej będzie jeszcze do tego dodawana wartość 8L na oznaczenie rolników (którzy nie pracują w innej formie)
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
          wynagrodzenie_mies = sum(.data$podst_maks / .data$udzial_dni_zatr_mies,
                                   na.rm = TRUE),
          wynagrodzenie_mies_uop =
            sum(.data$podst_maks[.data$etat] / .data$udzial_dni_zatr_mies[.data$etat],
                na.rm = TRUE),
          wynagrodzenie_stawka =
            sum(.data$podst_maks / .data$udzial_dni_zatr_mies / .data$wymiar_zatr,
                na.rm = TRUE),
          liczba_skladek = n(),
          sr_udzial_dni_zatr = mean(.data$udzial_dni_zatr_mies, na.rm = TRUE),
          sr_udzial_dni_zatr_uop = mean(.data$udzial_dni_zatr_mies[.data$etat], na.rm = TRUE),
          wymiar_zatr = case_when(
            !any(.data$etat, na.rm = TRUE) ~ NA_integer_,
            n() == 1L & sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) == 1 ~ 1L, # zatrudnienie z jednego tytułu, w pełnym wymiarze
            sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) == 1 ~ 2L, # zatrudnienie z wielu tytułów na łącznie pełen etat
            n() == 1L & sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) < 1 & !all(is.na(.data$wymiar_zatr), na.rm = TRUE) ~ 3L, # zatrudnienie z jednego tytułu na mniej niż pełen etat
            sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) < 1 & !all(is.na(.data$wymiar_zatr), na.rm = TRUE) ~ 4L, # zatrudnienie z kilku tytułów na łącznie mniej niż pełen etat
            sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) > 1 ~ 5L, # zatrudnienie na łącznie ponad pełen etat
            .default = 0L),
          biernosc = any(.data$bierny_skladka, na.rm = TRUE),
          dane_z_zus_skladki = TRUE,
          .groups = "drop") %>%
        full_join(t32,
                  by = c("id_abs", "rok_abs", "okres")) %>%
        mutate(praca = ifelse((.data$praca == 0L | is.na(.data$praca)) &
                                !is.na(.data$ubezp_krus),
                              8L, .data$praca))
    }
    # straszny wskaźnik dot. młodocianych pracowników
    tMlodoc <- tbl(con, "w16") %>%
      mutate(okres = 12L*.data$rok_skladka + .data$mies_skladka) %>%
      select("id_abs", "rok_abs", "okres", "id_platnika", "kod_zus") %>%
      left_join(tbl(con, "w22") %>%
                  select("kod_zus", "mlodoc"),
                by = "kod_zus") %>%
      group_by(.data$id_abs, .data$rok_abs) %>%
      mutate(mlodoc_byl = any(.data$mlodoc, na.rm = TRUE)) %>%
      ungroup() %>%
      filter(.data$mlodoc_byl)
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
      mutate(platnik_mlodoc = ifelse(is.na(.data$platnik_mlodoc),
                                     FALSE, .data$platnik_mlodoc)) %>%
      group_by(.data$id_abs, .data$rok_abs, .data$mlodoc_byl, .data$okres) %>%
      summarise(mlodoc_ten_sam_platnik = any(.data$platnik_mlodoc, na.rm = TRUE),
                mlodoc_ten_sam_platnik_etat = any(.data$platnik_mlodoc &
                                                    .data$etat, na.rm = TRUE),
                .groups = "drop")
    # łączenie
    # materializacje tabel do R - bez niej backend ma tendencje do zawieszania się
    cat("\n  - materializowanie widoku z adresami zamieszkania...")
    t15 <- collect(t15) # na edycji 2026 to trwa około 2h!
    cat(" zakończone; ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"),
        "\n  - materializowanie widoku ze składkami...", sep = "")
    t16m <- collect(t16m) # na edycji 2026 trwa ~20 min.
    cat(" zakończone; ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"),
        "\n  - materializowanie widoku z przerwami w opłacaniu składek...", sep = "")
    t17 <- collect(t17) # na edycji 2026 trwa ~20 min.
    cat(" zakończone; ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"),
        "\n- łączenie danych ze sobą...", sep = "")
    # tMlodoc nie należy materializować do R, bo dalszy kod tworzący p4 zakłada, że jest wciąż w bazie
    tabelePosrednie$p3 <- t1 %>%
      left_join(full_join(t16m, t17, by = c("id_abs", "rok_abs", "okres")) %>%
                  left_join(t15, by = c("id_abs", "rok_abs", "okres")) %>%
                  mutate(across(c(starts_with("wynagrodzenie"),
                                  starts_with("sr_udzial_")), as.numeric),
                         across(c("liczba_skladek"), as.integer),
                         across(starts_with("dane_z_"),
                                ~if_else(is.na(.), FALSE, .)),
                         across(-c("id_abs", "rok_abs",
                                   "rok_skladka", "mies_skladka", "okres",
                                   "emeryt_rencista", "niepelnosprawny",
                                   "teryt", starts_with("wynagrodzenie"),
                                   "liczba_skladek", starts_with("sr_udzial_"),
                                   "wymiar_zatr", starts_with("powiat"),
                                   starts_with("dane_z_")),
                                ~if_else(is.na(.), 0L, as.integer(.))),
                         brak_danych_z_zus = !(.data$dane_z_zus_skladki |
                                                 .data$dane_z_zus_przerwy),
                         brak_danych_z_krus = !(.data$dane_z_krus_skladki |
                                                  .data$dane_z_krus_swiadczenia)) %>%
                  mutate(dziecko = case_when(.data$dziecko %in% 1L ~ 1L,
                                             .data$dziecko2 %in% 1L ~ 2L,
                                             .default = 0L),
                         biernosc_zus =
                           as.integer(.data$biernosc %in% 1L | .data$przerwa %in% 1L)) %>%
                  select(-"dziecko2", -"przerwa", -"biernosc",
                         -starts_with("dane_z_")) %>%
                  rename(teryt_zam = "teryt") %>%
                  left_join(collect(tMlodoc),
                            by = c("id_abs", "rok_abs", "okres")),
                by = c("id_abs", "rok_abs", "okres")) %>%
      left_join(t34512m,
                by = c("id_abs", "rok_abs", "okres")) %>%
      mutate(teryt_zam = floor(.data$teryt_zam / 100),
             across(starts_with("brak_danych_z_"), ~if_else(is.na(.), TRUE, .)),
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
               .data$zmarl ~ NA_character_,
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
                        "Bezrobocie", "Brak danych o aktywności"))) %>%
      select(-"rok_skladka", -"mies_skladka", -"mlodoc_byl",
             -"mlodoc_ten_sam_platnik", -"mlodoc_ten_sam_platnik_etat") %>%
      przytnij_wynagrodzenia("wynagrodzenie", "wynagrodzenie_uop",
                             zmGrupujace = c("rok_abs", "rok"),
                             q = c(0.005, 0.995), usun0 = TRUE,
                             zachowajOryginalne = TRUE) %>%
      left_join(t1 %>%
                  semi_join(tbl(con, "w2") %>%
                              filter(.data$typ_szk %in%
                                       c("Branżowa szkoła I stopnia",
                                         "Branżowa szkoła II stopnia",
                                         "Technikum",
                                         "Szkoła specjalna przysposabiająca do pracy")) %>%
                              select("id_abs", "rok_abs") %>%
                              distinct() %>%
                              collect(),
                            by = c("id_abs", "rok_abs")) %>%
                  left_join(tDyplZaw,
                             by = join_by(x$id_abs == y$id_abs, x$rok_abs == y$rok_abs,
                                          x$okres >= y$okres),
                             multiple = "last",
                             suffix = c("", "DyplZaw")) %>%
                  mutate(dyplom_zaw = factor(ifelse(is.na(.data$dyplom_zaw),
                                                    "brak",
                                                    .data$dyplom_zaw),
                                             c("dyplom zawodowy",
                                               "certyfikat kwalifikacji",
                                               "brak"),
                                             c("Dyplom zawodowy",
                                               "Tylko certyfikat kwalifikacji",
                                               "Brak certyfikatów i dyplomu"))) %>%
                  select("id_abs", "rok_abs", "okres", "dyplom_zaw"),
                by = c("id_abs", "rok_abs", "okres")) %>%
      left_join(t1 %>%
                  # semi_join(tbl(con, "w2") %>%
                  #             filter(.data$typ_szk %in%
                  #                      c("Bednarska Szkoła Realna",
                  #                        "Liceum ogólnokształcące",
                  #                        "Technikum")) %>%
                  #             select("id_abs", "rok_abs") %>%
                  #             distinct() %>%
                  #             collect(),
                  #           by = c("id_abs", "rok_abs")) %>%
                  left_join(tMatura,
                            by = join_by(x$id_abs == y$id_abs, x$rok_abs == y$rok_abs,
                                         x$okres >= y$okres),
                            suffix = c("", "Matura")) %>%
                  mutate(matura_zdana = as.integer(.data$matura %in% "matura")) %>%
                  select("id_abs", "rok_abs", "okres", "matura_zdana"),
                by = c("id_abs", "rok_abs", "okres")) %>%
      select("id_abs", "rok_abs", "rok", "miesiac", "mies_od_ukoncz", "okres",
             "status", "zmarl", "brak_danych_z_zus", any_of("brak_danych_z_krus"),
             "praca", "mlodociany", "kont_mlodoc_prac", "bezrobocie":"nauka2",
             "nauka_szk_abs", everything()) %>%
      mutate(across(-any_of(c("id_abs", "rok_abs", "rok", "miesiac",
                              "mies_od_ukoncz", "okres", "zmarl",
                              "brak_danych_z_zus", "brak_danych_z_krus")),
                    ~if_else(.data$zmarl, NA, .)),
             teryt_pow_szk_kont = .data$teryt_pow_szk_kont / 100L)
    rm(t1, tDyplZaw, tMatura, t34512m, t15, t16m, t17)
    stopifnot(nrow(tabelePosrednie$p3) ==
                nrow(distinct(select(tabelePosrednie$p3,
                                     "id_abs", "rok_abs", "okres"))))
    if (rokMonitoringu >= 2026L) {
      tabelePosrednie$p3 <- tabelePosrednie$p3 %>%
        przytnij_wynagrodzenia("wynagrodzenie_mies", "wynagrodzenie_mies_uop",
                               zmGrupujace = c("rok_abs", "rok"),
                               q = c(0.005, 0.995), usun0 = TRUE,
                               zachowajOryginalne = TRUE) %>%
        przytnij_wynagrodzenia("wynagrodzenie_stawka",
                               zmGrupujace = c("rok_abs", "rok"),
                               q = c(0.005, 0.995), usun0 = TRUE,
                               zachowajOryginalne = TRUE)
    } else {
      tabelePosrednie$p3 <- tabelePosrednie$p3 %>%
        select(-"brak_danych_z_krus")
    }
    if (rokMonitoringu == 2026L) {
      tabelePosrednie$p3 <- tabelePosrednie$p3 %>%
        mutate(status_bez_krus = case_when(
          .data$zmarl ~ NA_character_,
          (.data$nauka2 == 1L | .data$nauka_szk_abs == 1L) &
            .data$praca %in% (1L:7L)  ~ "Nauka i praca",
          (.data$nauka2 %in% 1L | .data$nauka_szk_abs == 1L) &
            .data$praca %in% c(0L, 8L, NA_integer_) ~ "Tylko nauka",
          .data$nauka2 == 0L & .data$nauka_szk_abs == 0L &
            .data$praca %in% (1L:7L) ~ "Tylko praca",
          .data$bezrobocie == 1L & .data$nauka2 == 0L &
            .data$nauka_szk_abs == 0L &
            .data$praca %in% c(0L, 8L, NA_integer_) ~ "Bezrobocie",
          .default = "Brak danych o aktywności") %>%
            factor(c("Tylko nauka", "Nauka i praca", "Tylko praca",
                     "Bezrobocie", "Brak danych o aktywności")))
    }
    if (zapisujPoKazdejTabeliPosredniej) save(tabelePosrednie, file = "tabele-posrednie-temp.RData")
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P4 (dane stałe w czasie i kilka fikuśnych wskaźników) ######################
  if (przygotujP4) {
    cat("\nPrzygotowywanie tabeli P4 (może zająć około 1 h)...")
    # średnie wynagrodzenia w zawodach dla danego rocznika
    if ("wynagrodzenie_mies" %in% names(tabelePosrednie$p3)) {
      zmWynagrodzenia <- "wynagrodzenie_stawka"
    } else {
      zmWynagrodzenia <- "wynagrodzenie"
    }
    tWynagrZawod <- tabelePosrednie$p3 %>%
      filter(!is.na(.data$praca),
             .data$praca > 0L,
             !is.na(.data$wynagrodzenie),
             .data$wynagrodzenie > 0) %>%
      left_join(collect(t2), by = c("id_abs", "rok_abs")) %>%
      group_by(.data$rok_abs, .data$kod_zaw) %>%
      summarise(across(all_of(zmWynagrodzenia),
                       list(zawod_sr_wynagrodzenie = ~mean(., na.rm = TRUE)),
                       .names = "{.fn}"),
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
      summarise(l_prac_ucz_uop = n_distinct(.data$id_platnika[.data$nauka %in% 1L & .data$etat], na.rm = TRUE),
                l_prac_nucz_uop = n_distinct(.data$id_platnika[.data$nauka %in% 0L & .data$etat], na.rm = TRUE),
                l_prac_nucz_nuop = n_distinct(.data$id_platnika[.data$nauka %in% 0L & .data$netat], na.rm = TRUE),
                .groups = "drop")
    # łączenie i zapis
    t1 <- tbl(con, "w1") %>%
      left_join(tbl(con, "w14") %>%
                  mutate(zmarl_mies_od_ukoncz =
                           (12L*.data$rok_zgonu + .data$mies_zgonu) -
                           (12L*.data$rok_abs + 6L)) %>%
                  select("id_abs", "rok_abs", "zmarl_mies_od_ukoncz"),
                by = c("id_abs", "rok_abs"))
    if (rokMonitoringu >= 2026L) {
      t1 <- t1 %>%
        left_join(tbl(con, "w29"),
                  by = "kraj_pochodzenia")
    }
    tabelePosrednie$p4 <- inner_join(t1,
                                     tbl(con, "w2"),
                                     by = c("id_abs", "rok_abs")) %>%
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
             any_of(c("kraj_pochodzenia", "region_pochodzenia")), "zmarl_mies_od_ukoncz",
             "id_szk", "mlodoc_byl", "typ_szk", "szk_specjalna", "typ_szk_mlodoc",
             "teryt_pow_szk", "nazwa_pow_szk", "teryt_woj_szk", "nazwa_woj_szk",
             "nazwa_makroreg_szk", "nazwa_reg_szk", "nazwa_podreg_szk", "nts_podreg_szk",
             "lp", "kod_zaw", "nazwa_zaw", "branza",
             "kod_isced", "grupa_isced", "podgrupa_isced", "nazwa_isced",
             any_of(c("forma_ksztalc", "nauka_poza_szkola"))) %>%
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
                  filter(.data$nauka %in% 1L & .data$nauka_studia %in% 0L) %>%
                  select("id_abs", "rok_abs") %>%
                  distinct() %>%
                  mutate(abs_w_sio = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      left_join(tabelePosrednie$p3 %>%
                  filter(!(.data$nauka_studia %in% 0L)) %>%
                  select("id_abs", "rok_abs") %>%
                  distinct() %>%
                  mutate(abs_w_polon = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      left_join(tabelePosrednie$p3 %>%
                  filter(!.data$brak_danych_z_zus) %>%
                  select("id_abs", "rok_abs") %>%
                  distinct() %>%
                  mutate(abs_w_zus = TRUE),
                by = c("id_abs", "rok_abs"))
    if (rokMonitoringu >= 2026L) {
      tabelePosrednie$p4 <- tabelePosrednie$p4 %>%
        left_join(tabelePosrednie$p3 %>%
                    filter(!.data$brak_danych_z_krus) %>%
                    select("id_abs", "rok_abs") %>%
                    distinct() %>%
                    mutate(abs_w_krus = TRUE),
                  by = c("id_abs", "rok_abs"))
    }
    tabelePosrednie$p4 <- tabelePosrednie$p4 %>%
      mutate(plec = factor(as.character(.data$plec), # pozbywanie się typu 'pq_plec'
                           unique(c("K", "M",
                                    sort(unique(.data$plec))))),
             typ_szk = factor(.data$typ_szk,
                              unique(c("Liceum ogólnokształcące",
                                       "Liceum ogólnokształcące dla dorosłych",
                                       "Technikum",
                                       "Branżowa szkoła II stopnia",
                                       "Szkoła policealna",
                                       "Branżowa szkoła I stopnia",
                                       "Szkoła specjalna przysposabiająca do pracy",
                                       sort(unique(.data$typ_szk))))),
             typ_szk_mlodoc = factor(.data$typ_szk_mlodoc,
                                     unique(c("Liceum ogólnokształcące",
                                              "Liceum ogólnokształcące dla dorosłych",
                                              "Technikum",
                                              "Branżowa szkoła II stopnia",
                                              "Szkoła policealna",
                                              "Młodociani w Branżowej szkole I stopnia",
                                              "Niemłodociani w Branżowej szkole I stopnia",
                                              "Szkoła specjalna przysposabiająca do pracy",
                                              sort(unique(.data$typ_szk))))),
             across(starts_with("abs_w_"), ~if_else(is.na(.), FALSE, .)),
             across(starts_with("l_prac_"), ~if_else(is.na(.), 0L, as.integer(.))),
             across(where(is.character), as.character)) # pozbywanie się enumerowanych typów z bazy
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
    # doliczanie wskaźników wykorzystywanych w raportach
    tabelePosrednie$p4 <- suppressMessages(
      tabelePosrednie$p4 %>%
        dodaj_wskazniki_dyplomy(tabelePosrednie$p1, maksMiesOdUkoncz = -3L,
                                rokMonitoringu = rokMonitoringu) %>%
        dodaj_wskazniki_kontynuacje(tabelePosrednie$p2,
                                    miesOdUkoncz = c(6L, 18L, 30L, 42L, 54L,
                                                     66L, 78L)) %>%
        select(-any_of(c("branza_kont_bsii18", "branza_kont_bsii30",
                         "branza_kont_bsii42", "branza_kont_bsii54",
                         "branza_kont_bsii66", "branza_kont_bsii78",
                         "dziedzina_kont30", "dziedzina_kont42",
                         "dyscyplina_kont30", "dyscyplina_kont42"))) %>%
        dodaj_wskazniki_prace(tabelePosrednie$p3,
                              zmWynagrodzenie = ifelse(rokMonitoringu < 2026L,
                                                       "wynagrodzenie",
                                                       "wynagrodzenie_stawka")))
    if (rokMonitoringu == 2026L) {
      tabelePosrednie$p4 <- tabelePosrednie$p4 %>%
        select(-c("praca_nauka_r0_wrzgru", "praca_bez_nauki_r0_wrzgru",
                  "bezrobocie_r0_wrzgru")) %>%
        dodaj_wskazniki_prace(tabelePosrednie$p3,
                              zmWynagrodzenie = "wynagrodzenie")
    }
    if (zapisujPoKazdejTabeliPosredniej) save(tabelePosrednie, file = "tabele-posrednie-temp.RData")
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # Zmniejszanie precyzji PKD ##################################################
  if (przygotujP5 | przygotujP7) {
    t18 <- tbl(con, "w18") %>%
      select("id_platnika", "pkd") %>%
      collect() %>%
      mutate(pkd = sub("^([[:digit:]]{2})[[:digit:]]{2}([[:alpha:]])$",
                       "\\1\\2", .data$pkd))
    t18 <- copy_to(con, t18, overwrite = TRUE)
  }
  # P5 (absolwento-miesiąco-pracodawcy) ########################################
  if (przygotujP5) {
    cat("\nPrzygotowywanie tabeli P5...")
    if (rokMonitoringu < 2026L) {
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
          .groups = "drop")
    } else {
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
          wynagrodzenie_mies = sum(.data$podst_maks / .data$udzial_dni_zatr_mies,
                                   na.rm = TRUE),
          wynagrodzenie_mies_uop =
            sum(.data$podst_maks[.data$etat] / .data$udzial_dni_zatr_mies[.data$etat],
                na.rm = TRUE),
          wynagrodzenie_stawka =
            sum(.data$podst_maks / .data$udzial_dni_zatr_mies / .data$wymiar_zatr,
                na.rm = TRUE),
          liczba_skladek = n(),
          sr_udzial_dni_zatr = mean(.data$udzial_dni_zatr_mies, na.rm = TRUE),
          sr_udzial_dni_zatr_uop = mean(.data$udzial_dni_zatr_mies[.data$etat], na.rm = TRUE),
          wymiar_zatr = case_when(
            !any(.data$etat, na.rm = TRUE) ~ NA_integer_,
            n() == 1L & sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) == 1 ~ 1L, # zatrudnienie z jednego tytułu, w pełnym wymiarze
            sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) == 1 ~ 2L, # zatrudnienie z wielu tytułów na łącznie pełen etat
            n() == 1L & sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) < 1 & !all(is.na(.data$wymiar_zatr), na.rm = TRUE) ~ 3L, # zatrudnienie z jednego tytułu na mniej niż pełen etat
            sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) < 1 & !all(is.na(.data$wymiar_zatr), na.rm = TRUE) ~ 4L, # zatrudnienie z kilku tytułów na łącznie mniej niż pełen etat
            sum(.data$wymiar_zatr[.data$etat], na.rm = TRUE) > 1 ~ 5L, # zatrudnienie na łącznie ponad pełen etat
            .default = 0L),
          .groups = "drop")
    }
    tabelePosrednie$p5 <- tabelePosrednie$p5 %>%
      filter(.data$forma_zatrudnienia > 0L) %>%
      left_join(t18,
                by = "id_platnika") %>%
      collect() %>%
      mutate(mies_od_ukoncz = .data$okres - rokmiesiac2okres(.data$rok_abs, 6L),
             across(where(is.numeric), as.numeric),
             liczba_skladek = as.integer(.data$liczba_skladek)) %>%
      select("id_abs", "rok_abs", rok = "rok_skladka", miesiac = "mies_skladka",
             "mies_od_ukoncz", "okres", "id_platnika",
             pkd_pracod = "pkd", "forma_zatrudnienia", "mlodociany",
             starts_with("wynagrodzenie"),
             any_of(c("liczba_skladek", "sr_udzial_dni_zatr",
                      "sr_udzial_dni_zatr_uop", "wymiar_zatr")))
    if (zapisujPoKazdejTabeliPosredniej) save(tabelePosrednie, file = "tabele-posrednie-temp.RData")
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P6 (dane szkół - w tym kontynuacji - i uczelni) ############################
  if (przygotujP6) {
    cat("\nTabela P6...")
    if (rokMonitoringu < 2026L) {
      tabelePosrednie$p6 <- collect(t26) %>%
        mutate(across(where(is.character), as.character)) # pozbywanie się typów zdefiniowanych w bazie
    } else {
      tabelePosrednie$p6 <- bind_rows(
        collect(t26) %>%
          mutate(across(where(is.character), as.character)), # pozbywanie się typów zdefiniowanych w bazie
        collect(t30) %>%
          mutate(across(where(is.character), as.character))) # pozbywanie się typów zdefiniowanych w bazie
    }
    tabelePosrednie$p6 <- tabelePosrednie$p6 %>%
      mutate(organ_prowadzacy_teryt = .data$organ_prowadzacy_teryt / 100L) %>%
      select("id_szk", any_of("id_ucz"), "szk_ma_abs":"typ_szk_rspo",
             starts_with("organ_rejestrujacy_"),
             "organ_sposob_ewidencjonowania", "rok_szk":"teryt_gmi_szk",
             "teryt_pow_szk", any_of(c("kraj_ucz", "rodzaj_gmi_ucz")),
             "wojewodztwo_szk":"poczta", starts_with("organ_prowadzacy_"),
             "publicznosc":"specyfika", any_of("typ_ucz"), everything())
    rm(t26, t30)
    if (zapisujPoKazdejTabeliPosredniej) save(tabelePosrednie, file = "tabele-posrednie-temp.RData")
    cat(" przygotowana. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P7 zawody wykonywane #######################################################
  if (przygotujP7) {
    cat("\nPrzygotowywanie tabeli P7...")
    tabelePosrednie$p7 <- tbl(con, "w2") %>%
      select("id_abs", "rok_abs", "kod_zaw") %>%
      distinct() %>%
      inner_join(t16,
                 by = c("id_abs", "rok_abs")) %>%
      filter(!is.na(.data$kod_zaw_praca)) %>%
      left_join(t18,
                by = "id_platnika") %>%
      group_by(.data$id_abs, .data$rok_abs, .data$kod_zaw, .data$rok_skladka,
               .data$mies_skladka, .data$kod_zaw_praca, .data$pkd) %>%
      summarise(wynagrodzenie_mies =
                  sum(.data$podst_maks / .data$udzial_dni_zatr_mies, na.rm = TRUE),
                udzial_dni_zatr_mies =
                  sum(.data$udzial_dni_zatr_mies, na.rm = TRUE),
                udzial_dni_zatr_mies_waz_wymiar =
                  sum(.data$udzial_dni_zatr_mies*.data$wymiar_zatr, na.rm = TRUE),
                .groups = "drop") %>%
      collect() %>%
      mutate(okres = 12L*.data$rok_skladka + .data$mies_skladka,
             mies_od_ukoncz = .data$okres - rokmiesiac2okres(.data$rok_abs, 6L)) %>%
      select("id_abs", "rok_abs", rok = "rok_skladka",
             miesiac = "mies_skladka", "mies_od_ukoncz", "okres",
             everything())
    if (zapisujPoKazdejTabeliPosredniej) save(tabelePosrednie, file = "tabele-posrednie-temp.RData")
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P8 wyniki procentowe egzaminow #############################################
  if (przygotujP8) {
    cat("\nPrzygotowywanie tabeli P8...")
    tabelePosrednie$p8 <- bind_rows(
      tbl(con, "w27") %>%
        collect() %>%
        rename_with(~sub("_e8$", "_egz", .)) %>%
        mutate(across(where(is.character), as.character), # pozbywanie się typów zdefiniowanych w bazie
               egzamin = "egzamin ósmoklasisty",
               forma_zdawania = "pisemna"),
      tbl(con, "w28") %>%
        collect() %>%
        rename_with(~sub("_matura$", "_egz", .)) %>%
        mutate(across(where(is.character), as.character), # pozbywanie się typów zdefiniowanych w bazie
               egzamin = "matura"),
      tbl(con, "w810") %>%
        left_join(tbl(con, "w810d") %>%
                    select("id_abs", "rok_abs", "rok_egz_zaw", "sesja_egz_zaw",
                           "kod_kwal") %>%
                    distinct() %>%
                    mutate(dostos_form_egz = TRUE),
                  by = c("id_abs", "rok_abs", "rok_egz_zaw", "sesja_egz_zaw",
                         "kod_kwal")) %>%
        select("id_abs", "rok_abs", rok_egz = "rok_egz_zaw",
               termin_egz = "sesja_egz_zaw", przedmiot_egz = "kod_kwal",
               "dostos_form_egz", pisemna = "wynik_pisemn_egz_zaw",
               praktyczna = "wynik_prakt_egz_zaw") %>%
        collect() %>%
        mutate(dostos_form_egz = .data$dostos_form_egz %in% TRUE,
               egzamin = "egzamin zawodowy") %>%
        pivot_longer(c("pisemna", "praktyczna"),
                     names_to = "forma_zdawania", values_to = "wynik_egz")
    ) %>%
      left_join(tbl(con, "w2") %>%
                  select("id_abs", "rok_abs", "kod_zaw") %>%
                  collect(),
                by = c("id_abs", "rok_abs")) %>%
      left_join(tbl(con, "w21") %>%
                  mutate(kwal_w_zaw_wyucz = TRUE) %>%
                  rename(przedmiot_egz = "kod_kwal") %>%
                  collect(),
                by = c("przedmiot_egz", "kod_zaw")) %>%
      mutate(stan_zdawania_egz = factor(.data$stan_zdawania_egz,
                                        c("Przystąpił", "Finalista", "Laureat",
                                          "Nieobecny",
                                          "Nie przeprowadzano egzaminu",
                                          "Unieważniony",
                                          "Zwolniony", "Przerwany")),
             poziom_egz = factor(.data$poziom_egz,
                                 c("P", "R", "D", "Z", "N"),
                                 c("podstawowy", "rozszerzony", "dwujęzyczny",
                                   "zestaw zadań w języku obcym", "ndt.")),
             kwal_w_zaw_wyucz =
               case_when(kwal_w_zaw_wyucz %in% TRUE ~ TRUE,
                         !(kod_zaw %in% c(NA_integer_, 10101L, 10102L)) &
                           egzamin == "egzamin zawodowy" ~ FALSE)) %>%
      select("id_abs", "rok_abs", "kod_zaw", "egzamin", "kwal_w_zaw_wyucz",
             "rok_egz":"przedmiot_egz", "poziom_egz",
             forma_zdawania_egz = "forma_zdawania", "stan_zdawania_egz",
             starts_with("dostos_"), everything())
    if (zapisujPoKazdejTabeliPosredniej) save(tabelePosrednie, file = "tabele-posrednie-temp.RData")
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P9 orzeczenia i dostosowania na egzaminach #################################
  if (przygotujP9) {
    cat("\nPrzygotowywanie tabeli P9...")
    t31 <- tbl(con, "w2") %>%
      select("id_abs", "rok_abs", "typ_szk") %>%
      distinct() %>%
      inner_join(tbl(con, "w31"),
                 by = c("id_abs", "rok_abs")) %>%
      mutate(data_min =
               as.Date(paste0(.data$rok_abs -
                                case_when(.data$typ_szk == "Bednarska Szkoła Realna" ~ 4L,
                                          .data$typ_szk == "Branżowa szkoła I stopnia" ~ 3L,
                                          .data$typ_szk == "Branżowa szkoła II stopnia" ~ 2L,
                                          .data$typ_szk == "Liceum ogólnokształcące" ~ 4L,
                                          .data$typ_szk == "Szkoła policealna" ~ 3L,
                                          .data$typ_szk == "Szkoła specjalna przysposabiająca do pracy" ~ 3L,
                                          .data$typ_szk == "Technikum" ~ 5L),
                              "-09-01")),
             orzeczenie_data_do =
               if_else(.data$orzeczenie_data_do > as.Date(paste0(.data$rok_abs, "-06-30")),
                       as.Date(paste0(.data$rok_abs, "-06-30")),
                       .data$orzeczenie_data_do)) %>%
      filter(.data$orzeczenie_data_do > .data$orzeczenie_data_od,
             .data$orzeczenie_data_do > .data$data_min) %>%
      mutate(orzeczenie_data_od =
               if_else(.data$orzeczenie_data_od < .data$data_min,
                       .data$data_min, .data$orzeczenie_data_od)) %>%
      collect() %>%
      add_count(.data$id_abs, .data$rok_abs, .data$typ_szk,
                across(starts_with("niepelnosprawnosc")),
                name = "lOrzeczen")
    t31 <- bind_rows(t31 %>%
                       filter(.data$lOrzeczen == 1L),
                     # zabezpieczanie się przed nakładającymi się okresami orzeczeń - na wszelki wypadek, bo w edycji 2026 nie stwierdzono ich występowania
                     t31 %>%
                       filter(.data$lOrzeczen > 1L) %>%
                       arrange(.data$id_abs, .data$rok_abs,
                               across(starts_with("niepelnosprawnosc")),
                               .data$orzeczenie_data_od) %>%
                       group_by(.data$id_abs, .data$rok_abs, .data$typ_szk,
                                across(starts_with("niepelnosprawnosc"))) %>%
                       mutate(orzeczenie_data_od2 =
                                if_else(.data$orzeczenie_data_od <
                                          lag(.data$orzeczenie_data_do,
                                              n = 1, default = as.Date("0001-01-01")),
                                        lead(.data$orzeczenie_data_do, n = 1),
                                        .data$orzeczenie_data_od),
                              orzeczenie_data_do2 =
                                if_else(.data$orzeczenie_data_do >
                                          lead(.data$orzeczenie_data_od,
                                               n = 1, default = as.Date("9999-12-31")),
                                        lead(.data$orzeczenie_data_od, n = 1),
                                        .data$orzeczenie_data_do)) %>%
                       ungroup() %>%
                       select(-c("orzeczenie_data_od", "orzeczenie_data_do")) %>%
                       rename(orzeczenie_data_od = "orzeczenie_data_od2",
                              orzeczenie_data_do = "orzeczenie_data_do2") %>%
                       distinct()) %>%
      mutate(orzeczenie_dlugosc = as.integer(difftime(.data$orzeczenie_data_do,
                                                      .data$orzeczenie_data_od,
                                                      units = "days")) + 1L) %>%
      group_by(.data$id_abs, .data$rok_abs, .data$typ_szk,
               across(starts_with("niepelnosprawnosc"))) %>%
      summarise(orzeczenia_dlugosc = sum(.data$orzeczenie_dlugosc, na.rm = TRUE),
                .groups = "drop") %>%
      mutate(across(where(is.logical), as.character),
             orzeczenia_min_2lata = .data$orzeczenia_dlugosc >= 660L) %>%
      pivot_longer(cols = starts_with("niepelnosprawnosc"),
                   names_to = "niepelnosprawnosc", values_to = "czy_jest") %>%
      filter(.data$czy_jest != "FALSE") %>%
      mutate(czy_sprzezona = grepl("_sprz[[:digit:]]+$", .data$niepelnosprawnosc) |
               .data$czy_jest == "z niepełnosprawnościami sprzężonymi",
             niepelnosprawnosc = case_when(
               .data$niepelnosprawnosc == "niepelnosprawnosc_sprz2" ~ "niewidomi",
               .data$niepelnosprawnosc == "niepelnosprawnosc_sprz3" ~ "słabowidzący",
               .data$niepelnosprawnosc == "niepelnosprawnosc_sprz4" ~ "niesłyszący",
               .data$niepelnosprawnosc == "niepelnosprawnosc_sprz5" ~ "słabosłyszący",
               .data$niepelnosprawnosc == "niepelnosprawnosc_sprz6" ~ "z niepełnosprawnością intelektualną w stopniu lekkim",
               .data$niepelnosprawnosc == "niepelnosprawnosc_sprz17" ~ "z niepełnosprawnością ruchową w tym z afazją",
               .data$niepelnosprawnosc == "niepelnosprawnosc_sprz18" ~ "z autyzmem w tym z zespołem Aspergera",
               .data$niepelnosprawnosc == "niepelnosprawnosc_sprz20" ~ "z niepełnosprawnością intelektualną w stopniu umiarkowanym",
               .data$niepelnosprawnosc == "niepelnosprawnosc_sprz21" ~ "z niepełnosprawnością intelektualną w stopniu znacznym",
               .default = .data$niepelnosprawnosc),
             niepelnosprawnosc = ifelse(.data$niepelnosprawnosc == "niepelnosprawnosc",
                                        .data$czy_jest, .data$niepelnosprawnosc)) %>%
      select(-"czy_jest")
    tabelePosrednie$p9 <- bind_rows(
      t31,
      bind_rows(tbl(con, "w27d") %>%
                  select("id_abs", "rok_abs", rok_egz = "rok_e8",
                         dostos_form_egz = "dostos_form_opis_e8",
                         dostos_war_egz = "dostos_war_e8") %>%
                  distinct() %>%
                  collect(),
                tbl(con, "w810d") %>%
                  filter(.data$dostosowanie_egz_zaw != "brak") %>%
                  mutate(dostos_war_egz = FALSE) %>%
                  select("id_abs", "rok_abs", rok_egz = "rok_egz_zaw",
                         dostos_form_egz = "dostosowanie_egz_zaw",
                         "dostos_war_egz") %>%
                  distinct() %>%
                  collect(),
                tbl(con, "w28d") %>%
                  select("id_abs", "rok_abs", rok_egz = "rok_matura",
                         dostos_form_egz = "dostos_form_opis_matura",
                         dostos_war_egz = "dostos_war_matura") %>%
                  distinct() %>%
                  collect()) %>%
        filter(.data$rok_egz <= .data$rok_abs) %>%
        group_by(.data$id_abs, .data$rok_abs, .data$rok_egz,
                 .data$dostos_form_egz) %>%
        summarise(dostos_war_egz = any(.data$dostos_war_egz, na.rm = TRUE),
                  .groups = "drop")
    )
    if (zapisujPoKazdejTabeliPosredniej) save(tabelePosrednie, file = "tabele-posrednie-temp.RData")
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # kończenie ##################################################################
  cat("\nKoniec: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")

  return(tabelePosrednie)
}
