#' @title Przygotowywanie tabel "posrednich"
#' @description Na podstawie zawartych w bazie danych z tabel wejściowych
#' funkcja przygotowuje 4 \emph{tablice pośrednie}, które zawierają zestawienia
#' wskaźników bezpośrednio użytecznych analitycznie. Aby zapisać tak
#' przygotowane tabele do relacyjnej bazy danych, należy następnie użyć funkcji
#' \code{\link{wczytaj_tabele_posrednie}}.
#' @param baza uchwyt połączenia do bazy lub lista argumentów do funkcji
#' \code{\link[DBI]{dbConnect}} umożliwiających nawiązanie połączenia z bazą
#' danych, w której mają zostać zapisane wczytwane dane
#' @param rokMonitoringu rok, w którym prowadzony jest monitoring (ma znaczenie
#' dla ustalenia adekwatnych przedziałów czasu, dla których mają zostać
#' obliczone wskaźniki, a na podstawie samych danych funkcja nie ma jak
#' go odgadnąć, nie narażając się na błędy)
#' @param okresOd opcjonalnie liczba całkowita - liczbę miesięcy, o którą
#' początek okresu objętego wskaźnikami w tabeli P3 ma być
#' \strong{przesunięty względem} stycznia roku ukończenia szkoły (zostania
#' absolwentem); domyślnie 0 (tzn. pierwszym miesiącem uwzględnionym w P3 jest
#' styczeń roku ukończenia szkoły)
#' @param okresDo opcjonalnie liczba całkowita - identyfikator miesiąca w formie
#' \code{12*rok+numer_miesiaca_w_ramach_roku}, który ma być ostatnim miesiącym
#' objętym wskaźnikami w tabeli P3; domyślnie maj roku prowadzenia monitoringu
#' @param okresyP2 opcjonalnie wektor liczb całkowitych - identyfikatory
#' miesięcy w formie \code{12*rok+numer_miesiaca_w_ramach_roku}, dla których
#' mają zostać obliczone wskaźniki zwracane w tabeli P2 (tj. wskaźniki dotyczące
#' branży/dziedziny/dysycypliny, w której absolwent kontynuuje naukę); domyślnie
#' grudzień każdego roku objętego monitoringiem
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
#' \emph{tabelę pośrednią} P1? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji)
#' @param przygotujP2 opcjonalnie wartość logiczna - czy przygotować
#' \emph{tabelę pośrednią} P2? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji)
#' @param przygotujP3 opcjonalnie wartość logiczna - czy przygotować
#' \emph{tabelę pośrednią} P3? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji)
#' @param przygotujP4 opcjonalnie wartość logiczna - czy przygotować
#' \emph{tabelę pośrednią} P4? (umożliwia wyłączenie przygotowania wybranej
#' tabeli w ramach danego wywołania funkcji); aby tabela P4 mogła zostać
#' przygotowana, konieczne jest również przygotowanie tabeli P3
#' @details Dane zawarte w bazie powinny obejmować \strong{tylko jeden rok
#' prowadzenia monitoringu}, ale mogą obejmować \strong{kilka różnych okresów od
#' ukończenia szkoły}.
#'
#' Wskaźniki dotyczące kontynuacji nauki w tabeli P3 nie odnoszą się do nauki
#' w szkole, jako absolwent której dana osoba została objęta monitoringiem,
#' a jedynie do \emph{kontynuacji} nauki (w praktyce: nauki w innej szkole niż
#' ta, jako absolwent której dana osoba została objęta monitoringiem).
#'
#' W przypadku KKZ i KUZ przy kodowaniu wskaźników opisujących status
#' konytynuacji nauki w tabeli P3 lub branżę kontynuacji nauki w tabeli P2
#' zostały wzięte pod uwagę tylko kursy, które mają podaną datę zakończenia
#' (co do tych, które nie mają jej podanej, nie da się rzetelnie rozstrzygać,
#' czy zostały one ukończone, czy zarzucone).
#' @return lista ramek danych z przygotowanymi tabelami
#' @seealso \code{\link{wczytaj_tabele_posrednie}}
#' @importFrom dplyr %>% across add_count arrange bind_rows case_when
#'                   collect copy_to distinct everything filter full_join
#'                   group_by if_else inner_join left_join mutate n pull rename
#'                   select slice_min slice_max starts_with summarise tbl
#'                   ungroup
#' @importFrom lubridate day month year
#' @importFrom DBI dbConnect dbExecute dbDisconnect
#' @export
przygotuj_tabele_posrednie <- function(
  baza, rokMonitoringu,
  okresOd = 0L, okresDo = 12L*rokMonitoringu + 5L,
  okresyP2 = 12*((rokMonitoringu - 5):(rokMonitoringu - 1)),
  imputujDateKoncaSzkoly = as.Date(paste0(rokMonitoringu, "-08-31")),
  imputujDateKoncaStudiow = as.Date(paste0(rokMonitoringu, "-09-30")),
  imputujDateKoncaAdresu = as.Date(paste0(rokMonitoringu, "-05-31")),
  minDniEdukacjiWMiesiacu = 14,
  przygotujP1 = TRUE, przygotujP2 = TRUE, przygotujP3 = TRUE,
  przygotujP4 = przygotujP3)
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
            length(przygotujP4) == 1L, przygotujP4 %in% c(FALSE, TRUE))
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
  cat("\nStart: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")

  # Półfabrykaty do P1 i P2 ####################################################
  if (przygotujP1 | przygotujP2) {
    # mapowanie kodów zawodów na branże i ew. obszary z preferencją na przypisanie
    # do branży (w sensie takiej z KZSB, w odróżnieniu od obszaru)
    t20 <- tbl(con, "w20") %>%
      select(kod_zaw, branza) %>%
      distinct() %>%
      group_by(kod_zaw) %>%
      slice_max(grepl("^branża", branza), n = 1L) %>%
      ungroup()
    # absolwenci z informacją o ew. zawodzie i dołączoną informacją o branży
    t2 <- tbl(con, "w2") %>%
      select(id_abs, rok_abs, kod_zaw) %>%
      distinct() %>%
      left_join(t20, by = "kod_zaw") %>%
      add_count(id_abs, rok_abs, kod_zaw) %>%
      ungroup()
    if (!(t2 %>%
          summarise(noDuplicates = all(n == 1L, na.rm = TRUE)) %>%
          pull(noDuplicates))) {
      stop("Wykryto duplikaty w przypisaniu (zawodów) absolwentów do branż. Nie da się przygotować tabeli P1 ani P2.")
    }
    t2 <- t2 %>%
      select(-n)
  }
  # P1 (dyplomy i świadectwa) ##################################################
  if (przygotujP1) {
    cat("Przygotowywanie tabeli P1...")
    # dyplomy czeladnika z dołączoną informacją o branży i informacjami
    # o zawodzie i branży absolwenta
    t6 <- t2 %>%
      inner_join(tbl(con, "w6") %>%
                   mutate(rodzaj_dyplomu = "tytuł czeladnika") %>%
                   rename(kod_zaw_dyplom = kod_zaw_czel) %>%
                   left_join(t20 %>%
                               select(kod_zaw_dyplom = kod_zaw,
                                      branza_dyplom = branza),
                             by = "kod_zaw_dyplom"),
                 by = c("id_abs", "rok_abs")) %>%
      select(id_abs, rok_abs, kod_zaw, branza, rodzaj_dyplomu, everything())
    # matury
    t7 <- t2 %>%
      inner_join(tbl(con, "w7") %>%
                   filter(czy_zdana_matura, !is.na(data_swiad_matura)) %>%
                   group_by(id_abs, rok_abs) %>%
                   slice_min(order_by = c(data_swiad_matura, rok_matura),
                             n = 1L) %>%
                   ungroup() %>%
                   mutate(rodzaj_dyplomu = "matura",
                          rok_dyplom = year(data_swiad_matura),
                          miesiac_dyplom = month(data_swiad_matura)),
                 by = c("id_abs", "rok_abs")) %>%
      select(id_abs, rok_abs, kod_zaw, branza, rodzaj_dyplomu,
             rok_dyplom, miesiac_dyplom)
    # certyfikaty kwalifikacji
    t810 <- t2 %>%
      inner_join(tbl(con, "w810") %>%
                   mutate(rodzaj_dyplomu = "certyfikat kwalifikacji",
                          miesiac_dyplom = if_else(rok_kwal == year(data_kwal),
                                                   month(data_kwal),
                                                   8L)) %>%
                   left_join(tbl(con, "w21"), by = "kod_kwal") %>%
                   left_join(t20, by = "kod_zaw") %>%
                   rename(kod_zaw_dyplom = kod_zaw, branza_dyplom = branza,
                          rok_dyplom = rok_kwal),
                 by = c("id_abs", "rok_abs")) %>%
      select(id_abs, rok_abs, kod_zaw, branza, rodzaj_dyplomu, kod_kwal,
             kod_zaw_dyplom, branza_dyplom, rok_dyplom, miesiac_dyplom) %>%
      add_count(id_abs, rok_abs, kod_zaw, branza, kod_kwal, rodzaj_dyplomu,
                rok_dyplom, miesiac_dyplom) %>%
      collect()
    t810 <- bind_rows(
      t810 %>%
        filter(n == 1L) %>%
        ungroup() %>%
        select(-n) %>%
        collect(),
      t810 %>%
        filter(n > 1L) %>%
        group_by(id_abs, rok_abs, kod_zaw, branza, kod_kwal, rodzaj_dyplomu,
                 rok_dyplom, miesiac_dyplom) %>%
        summarise(kod_zaw_dyplom =
                    case_when(any(kod_zaw_dyplom == kod_zaw, na.rm = TRUE) ~ kod_zaw),
                  branza_dyplom =
                    case_when(any(branza_dyplom == branza, na.rm = TRUE) ~ branza),
                  .groups = "drop") %>%
        collect()) %>%
      rename(dyplom_szczegoly = kod_kwal)
    # dyplomy zawodowe
    t911 <- t2 %>%
      inner_join(tbl(con, "w911") %>%
                   mutate(rodzaj_dyplomu = "dyplom zawodowy",
                          miesiac_dyplom = if_else(rok_dyp == year(data_dyp_zaw),
                                                   month(data_dyp_zaw),
                                                   8L)) %>%
                   left_join(t20, by = "kod_zaw") %>%
                   rename(kod_zaw_dyplom = kod_zaw, branza_dyplom = branza,
                          rok_dyplom = rok_dyp),
                 by = c("id_abs", "rok_abs")) %>%
      select(id_abs, rok_abs, kod_zaw, branza, rodzaj_dyplomu, kod_zaw_dyplom,
             branza_dyplom, rok_dyplom, miesiac_dyplom)
    # dyplomy ukończenia studiów
    t12d <- t2 %>%
      inner_join(tbl(con, "w12") %>%
                   filter(czy_ukoncz_stu == 1L) %>%
                   mutate(rodzaj_dyplomu =
                            case_when(grepl("magister|lekarz",
                                            tolower(tytul_zaw_stu)) ~ "dyplom magistra/lekarza",
                                      grepl("licencjat|inżynier",
                                            tolower(tytul_zaw_stu)) ~ "dyplom licencjata/inżyniera",
                                      TRUE ~ tytul_zaw_stu),
                          rok_dyplom = year(data_do_stu),
                          miesiac_dyplom = month(data_do_stu)) %>%
                   left_join(tbl(con, "w13") %>%
                               filter(czy_dyscyplina_wiodaca),
                             by = "id_kierunku_stu") %>%
                   rename(dyplom_szczegoly = tytul_zaw_stu,
                          dyscyplina_wiodaca = dyscyplina),
                 by = c("id_abs", "rok_abs")) %>%
      select(id_abs, rok_abs, kod_zaw, branza, rodzaj_dyplomu, dyplom_szczegoly,
             rok_dyplom, miesiac_dyplom, dziedzina, dyscyplina_wiodaca)
    # łączenie i wczytywanie do P1
    tabelePosrednie$p1 <- bind_rows(collect(t6),
                                    collect(t7),
                                    t810,
                                    collect(t911),
                                    collect(t12d)) %>%
      mutate(okres_dyplom = 12L*rok_dyplom + miesiac_dyplom) %>%
      group_by(id_abs, rok_abs, rodzaj_dyplomu) %>%
      arrange(id_abs, rok_abs, kod_zaw, okres_dyplom, rodzaj_dyplomu) %>%
      mutate(lp_dyplom = 1L:n()) %>%
      ungroup()
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P2 (branża/dziedzina kontynuacji kształcenia) ##############################
  if (przygotujP2) {
    cat("\nPrzygotowywanie tabeli P2...")
    # szkoły objęte SIO
    t3c <- t2 %>%
      inner_join(tbl(con, "w3") %>%
                   filter(!is.na(kod_zaw_kont)) %>%
                   rename(kod_zaw = kod_zaw_kont) %>%
                   left_join(t20, by = "kod_zaw") %>%
                   select(id_abs, rok_abs, data_od_szk_kont, data_do_szk_kont,
                          branza_kont = branza) %>%
                   mutate(data_do_szk_kont = if_else(is.na(data_do_szk_kont),
                                                     imputujDateKoncaSzkoly,
                                                     data_do_szk_kont)) %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      group_by(across(everything())) %>%
      summarise(okres_kont =
                  (12L*year(data_od_szk_kont) +
                     month(data_od_szk_kont)):(12L*year(data_do_szk_kont) +
                                                 month(data_do_szk_kont)),
                .groups = "drop") %>%
      filter(okres_kont %in% okresyP2, okres_kont > 12L*(rok_abs - 1L),
             !is.na(branza_kont)) %>%
      select(-data_od_szk_kont, -data_do_szk_kont) %>%
      distinct() %>%
      mutate(zrodlo = "W3")
    # KKZ
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t4c <- t2 %>%
      inner_join(tbl(con, "w4") %>%
                   filter(!is.na(data_do_kkz)) %>%
                   rename(kod_kwal = kod_kwal_kkz) %>%
                   left_join(tbl(con, "w21"), by = "kod_kwal") %>%
                   left_join(t20, by = "kod_zaw") %>%
                   select(id_abs, rok_abs, data_od_kkz, data_do_kkz,
                          branza_kont = branza) %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      group_by(across(everything())) %>%
      summarise(okres_kont = (12L*year(data_od_kkz) +
                                month(data_od_kkz)):(12L*year(data_do_kkz) +
                                                       month(data_do_kkz)),
                .groups = "drop") %>%
      filter(okres_kont %in% okresyP2, okres_kont > 12L*(rok_abs - 1L),
             !is.na(branza_kont)) %>%
      select(-data_od_kkz, -data_do_kkz) %>%
      distinct() %>%
      mutate(zrodlo = "W4")
    # KUZ
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t5c <- t2 %>%
      inner_join(tbl(con, "w5") %>%
                   filter(!is.na(data_do_kuz)) %>%
                   rename(kod_zaw = kod_zaw_kuz) %>%
                   left_join(t20, by = "kod_zaw") %>%
                   select(id_abs, rok_abs, data_od_kuz, data_do_kuz,
                          branza_kont = branza) %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      group_by(across(everything())) %>%
      summarise(okres_kont = (12L*year(data_od_kuz) +
                                month(data_od_kuz)):(12L*year(data_do_kuz) +
                                                       month(data_do_kuz)),
                .groups = "drop") %>%
      filter(okres_kont %in% okresyP2, okres_kont > 12L*(rok_abs - 1L),
             !is.na(branza_kont)) %>%
      select(-data_od_kuz, -data_do_kuz) %>%
      distinct() %>%
      mutate(zrodlo = "W5")
    # studia
    t12c <- t2 %>%
      inner_join(tbl(con, "w12") %>%
                   left_join(tbl(con, "w13") %>%
                               filter(czy_dyscyplina_wiodaca),
                             by = "id_kierunku_stu") %>%
                   select(id_abs, rok_abs, data_od_stu, data_do_stu,
                          dziedzina_kont = dziedzina,
                          dyscyplina_wiodaca_kont = dyscyplina) %>%
                   mutate(data_do_stu = if_else(is.na(data_do_stu),
                                                imputujDateKoncaStudiow,
                                                data_do_stu)) %>%
                   distinct(),
                 by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      group_by(across(everything())) %>%
      summarise(okres_kont = (12L*year(data_od_stu) +
                                month(data_od_stu)):(12L*year(data_do_stu) +
                                                       month(data_do_stu)),
                .groups = "drop") %>%
      filter(okres_kont %in% okresyP2, okres_kont > 12L*(rok_abs - 1L)) %>%
      select(-data_od_stu, -data_do_stu) %>%
      distinct()
    # łączenie i wczytywanie do P2
    tabelePosrednie$p2 <- bind_rows(t3c, t4c, t5c) %>%
      group_by(id_abs, rok_abs, kod_zaw, branza, okres_kont, branza_kont) %>%
      summarise(branza_kont_zrodlo = paste(zrodlo, collapse = ", "),
                .groups = "drop") %>%
      full_join(t12c,
                by = c("id_abs", "rok_abs", "kod_zaw", "branza", "okres_kont")) %>%
      group_by(id_abs, rok_abs, okres_kont) %>%
      arrange(id_abs, rok_abs, okres_kont, kod_zaw) %>%
      mutate(lp_kont = 1L:n()) %>%
      ungroup()
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P3 (dane miesięczne) #######################################################
  if (przygotujP3) {
    cat("\nPrzygotowywanie tabeli P3 (może zająć kilkanaście minut)...")
    # wszystkie absolwento-miesiące
    # uwaga! początkiem zawsze jest styczeń rok_abs! końcem maj roku prowadzenia monitoringu
    t1 <- tbl(con, "w1") %>%
      left_join(tbl(con, "w14") %>%
                  mutate(okres_zgonu = 12L*rok_zgonu + mies_zgonu) %>%
                  select(id_abs, rok_abs, okres_zgonu),
                by = c("id_abs", "rok_abs")) %>%
      collect() %>%
      group_by(across(everything())) %>%
      summarise(okres = (12L*rok_abs + 1L + okresOd):okresDo,
                .groups = "drop") %>%
      mutate(zmarl = if_else(is.na(okres_zgonu),
                             0L, as.integer(okres > okres_zgonu))) %>%
      select(-okres_zgonu)
    t1db <- copy_to(con, t1, overwrite = TRUE)
    # dane o kontynuacji nauki w szkołach objętych SIO przerobione na miesiące
    t3m <- tbl(con, "w3") %>%
      mutate(data_od_szk_kont =
               as.Date(data_od_szk_kont + minDniEdukacjiWMiesiacu),
             data_do_szk_kont = as.Date(
               if_else(is.na(data_do_szk_kont),
                       imputujDateKoncaSzkoly,
                       data_do_szk_kont) - minDniEdukacjiWMiesiacu)) %>%
      select(id_abs, rok_abs, typ_szk_kont,
             data_od_szk_kont, data_do_szk_kont) %>%
      distinct() %>%
      collect() %>%
      group_by(across(everything())) %>%
      summarise(okres_kont =
                  (12L*year(data_od_szk_kont) +
                     month(data_od_szk_kont)):(12L*year(data_do_szk_kont) +
                                                 month(data_do_szk_kont)),
                .groups = "drop") %>%
      filter(okres_kont > (12L*rok_abs + okresOd)) %>%
      select(-data_od_szk_kont, -data_do_szk_kont) %>%
      distinct()
    # dane o kontynuacji nauki na KKZ przerobione na miesiące
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t4m <- tbl(con, "w4") %>%
      filter(!is.na(data_do_kkz)) %>%
      select(id_abs, rok_abs, data_od_kkz, data_do_kkz) %>%
      mutate(data_od_kkz = as.Date(data_od_kkz + minDniEdukacjiWMiesiacu),
             data_do_kkz = as.Date(data_do_kkz - minDniEdukacjiWMiesiacu)) %>%
      distinct() %>%
      collect() %>%
      group_by(across(everything())) %>%
      summarise(okres_kont = (12L*year(data_od_kkz) +
                                month(data_od_kkz)):(12L*year(data_do_kkz) +
                                                       month(data_do_kkz)),
                .groups = "drop") %>%
      filter(okres_kont > (12L*rok_abs + okresOd)) %>%
      select(-data_od_kkz, -data_do_kkz) %>%
      distinct() %>%
      mutate(typ_szk_kont = "KKZ")
    # dane o kontynuacji nauki na KUZ przerobione na miesiące
    # w przypadku KKZ i KUZ mogę rozsądnie brać pod uwagę tylko te, dla których
    # znana jest data zakończenia - jej brak wcale nie musi oznaczać, że ktoś
    # dany kurs kontynuuje
    t5m <- tbl(con, "w5") %>%
      filter(!is.na(data_do_kuz)) %>%
      select(id_abs, rok_abs, data_od_kuz, data_do_kuz) %>%
      mutate(data_od_kuz = as.Date(data_od_kuz + minDniEdukacjiWMiesiacu),
             data_do_kuz = as.Date(data_do_kuz - minDniEdukacjiWMiesiacu)) %>%
      distinct() %>%
      collect() %>%
      group_by(across(everything())) %>%
      summarise(okres_kont = (12L*year(data_od_kuz) +
                                month(data_od_kuz)):(12L*year(data_do_kuz) +
                                                       month(data_do_kuz)),
                .groups = "drop") %>%
      filter(okres_kont > (12L*rok_abs + okresOd)) %>%
      select(-data_od_kuz, -data_do_kuz) %>%
      distinct() %>%
      mutate(typ_szk_kont = "KUZ")
    # dane o kontynuacji nauki na studiach przerobione na miesiące
    t12m <- tbl(con, "w12") %>%
      select(id_abs, rok_abs, data_od_stu, data_do_stu) %>%
      mutate(data_od_stu = as.Date(data_od_stu + minDniEdukacjiWMiesiacu),
             data_do_stu = as.Date(
               if_else(is.na(data_do_stu),
                       imputujDateKoncaStudiow,
                       data_do_stu) - minDniEdukacjiWMiesiacu)) %>%
      distinct() %>%
      collect() %>%
      group_by(across(everything())) %>%
      summarise(okres_kont = (12L*year(data_od_stu) +
                                month(data_od_stu)):(12L*year(data_do_stu) +
                                                       month(data_do_stu)),
                .groups = "drop") %>%
      filter(okres_kont > 12L*(rok_abs - 1L)) %>%
      select(-data_od_stu, -data_do_stu) %>%
      distinct() %>%
      mutate(typ_szk_kont = "studia")
    # laczenie
    t34512m <- bind_rows(t3m, t4m, t5m, t12m) %>%
      rename(okres = okres_kont) %>%
      group_by(id_abs, rok_abs, okres) %>%
      summarise(nauka = 1L,
                nauka2 = any(!(typ_szk_kont %in% c("KKZ", "KUZ"))),
                nauka_bs2st = any(typ_szk_kont %in% "Branżowa szkoła II stopnia"),
                nauka_lodd = any(typ_szk_kont %in% "Liceum ogólnokształcące"),
                nauka_spolic = any(typ_szk_kont %in%
                                     c("Szkoła policealna",
                                       "Policealna szkoła plastyczna",
                                       "Policealna szkoła plastyczna")),
                nauka_studia = any(typ_szk_kont %in% "studia"),
                nauka_kkz = any(typ_szk_kont %in% "KKZ"),
                nauka_kuz = any(typ_szk_kont %in% "KUZ"),
                .groups = "drop")
    # dane adresowe przerobione na miesiące
    # ignoruję ostatnią z zebranych cyfr TERYTu (miasto/wieś)
    # spośród konkurencyjnych TERYTów wybieram
    # - ten z adresu koresp. przed zamieszkania przed zameldowania,
    # - spośród adresów tego samego typu ten, który dotyczył najdłuższego okresu
    #   w ramach danego miesiąca
    t15 <- tbl(con, "w15") %>%
      mutate(data_adr_do =
               if_else(is.na(data_adr_do) | data_adr_do > imputujDateKoncaAdresu,
                       imputujDateKoncaAdresu, data_adr_do),
             teryt = 100L*floor(teryt / 10)) %>%
      select(-lp) %>%
      distinct() %>%
      mutate(okres_od_adr = 12L*year(data_adr_od) + month(data_adr_od),
             okres_do_adr = 12L*year(data_adr_do) + month(data_adr_do)) %>%
      inner_join(t1db, by = c("id_abs", "rok_abs")) %>%
      filter(okres >= okres_od_adr, okres <= okres_do_adr, !is.na(teryt),
             zmarl == 0L) %>%
      mutate(okres_pocz = as.Date(paste(floor((okres - 1) / 12),
                                        okres - 12L*(floor((okres - 1) / 12)),
                                        "01", sep = "-")),
             okres_kon = as.Date(as.Date(paste(floor((okres - 1) / 12),
                                               okres - 12L*(floor((okres - 1) / 12)),
                                               "01", sep = "-")) +
                                   months(1L) - days(1L))) %>%
      mutate(dni_okres = case_when(
        data_adr_od >= okres_pocz & data_adr_do <= okres_kon ~ data_adr_do - data_adr_od,
        data_adr_od >= okres_pocz ~ okres_kon - data_adr_od,
        data_adr_do <= okres_kon ~ data_adr_do - okres_pocz,
        TRUE ~ okres_kon - okres_pocz)) %>%
      mutate(preferencja = case_when(adres_typ == "koresp" ~ 300L,
                                     adres_typ == "zam" ~ 200L,
                                     adres_typ == "meld" ~ 100L,
                                     TRUE ~ 0L) + dni_okres) %>%
      group_by(id_abs, rok_abs, okres, adres_typ, teryt) %>%
      filter(dni_okres == max(dni_okres, na.rm = TRUE)) %>%
      group_by(id_abs, rok_abs, okres) %>%
      filter(preferencja == max(preferencja, na.rm = TRUE)) %>%
      select(id_abs, rok_abs, okres, teryt, adres_typ, dni_okres, preferencja) %>%
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
      mutate(lp = order_by(teryt, cumsum(rok_abs) / rok_abs) - 1L,
             n = n()) %>%
      filter(lp == okres %% n) %>%
      mutate(lp = lp + 1L) %>%
      select(id_abs, rok_abs, okres, teryt) %>%
      ungroup() %>%
      left_join(tbl(con, "w19") %>%
                  mutate(okres = 12L*rok + miesiac) %>%
                  select(-rok, -miesiac) %>%
                  rename(powiat_bezrobocie = stopa_bezrobocia,
                         powiat_sr_wynagrodzenie = sr_wynagrodzenia),
                by = c("okres", "teryt"))
    # przerwy w opłacaniu składek
    t17 <- tbl(con, "w17") %>%
      mutate(okres_od_przerwa =
               12L*year(data_od_przerwa) + month(data_od_przerwa),
             okres_do_przerwa =
               12L*year(data_do_przerwa) + month(data_do_przerwa)) %>%
      inner_join(t1db, by = c("id_abs", "rok_abs")) %>%
      filter(okres >= okres_od_przerwa, okres <= okres_do_przerwa, zmarl == 0L) %>%
      select(id_abs, rok_abs, okres) %>%
      distinct() %>%
      mutate(przerwa = TRUE)
    # składki ZUS
    t16 <- tbl(con, "w16") %>%
      mutate(podst_maks = if_else(podst_chor > podst_wypad,
                                  podst_chor, podst_wypad)) %>%
      mutate(podst_maks = if_else(podst_maks > podst_emer,
                                  podst_maks, podst_emer)) %>%
      mutate(podst_maks = if_else(podst_maks > podst_zdrow,
                                  podst_maks, podst_zdrow)) %>%
      left_join(tbl(con, "w22") %>%
                  select(kod_zus, etat = etat_ibe, netat = netat_ibe,
                         samoz = samoz_ela, inne = inne_ela, mlodoc,
                         bezrob_ela, bezrobotnystaz, dziecko, bierny_skladka),
                by = "kod_zus") %>%
      group_by(id_abs, rok_abs, rok_skladka, mies_skladka) %>%
      summarise(
        okres = 12L*rok_skladka + mies_skladka,
        praca = case_when(
          any(etat, na.rm = TRUE) & any(samoz, na.rm = TRUE) &
            any(netat | inne, na.rm = TRUE) ~ 7L,
          any(samoz, na.rm = TRUE) & any(netat | inne, na.rm = TRUE) ~ 6L,
          any(etat, na.rm = TRUE) & any(netat | inne, na.rm = TRUE) ~ 5L,
          any(etat, na.rm = TRUE) & any(samoz, na.rm = TRUE) ~ 4L,
          any(etat, na.rm = TRUE) ~ 1L,
          any(samoz, na.rm = TRUE) ~ 2L,
          any(netat | inne, na.rm = TRUE) ~ 3L,
          TRUE ~ 0L),
        mlodociany = as.integer(any(mlodoc, na.rm = TRUE)),
        praca_do_kont_mlodociany =
          as.integer(any(etat, na.rm = TRUE) | any(netat, na.rm = TRUE) |
                       any(kod_zus %in% 1250L, na.rm = TRUE)),
        bezrobocie = as.integer(any(bezrob_ela, na.rm = TRUE)),
        bezrobocie_staz = if_else(any(bezrob_ela, na.rm = TRUE),
                                  as.integer(any(bezrobotnystaz, na.rm = TRUE)),
                                  NA_integer_),
        dziecko = as.integer(any(dziecko, na.rm = TRUE)),
        wynagrodzenie = sum(podst_maks, na.rm = TRUE),
        wynagrodzenie_uop = sum(podst_maks[etat], na.rm = TRUE),
        biernosc = any(bierny_skladka, na.rm = TRUE),
        .groups = "drop")
    # straszny wskaźnik dot. młodocianych pracowników
    tMlodoc <- tbl(con, "w16") %>%
      mutate(okres = 12L*rok_skladka + mies_skladka) %>%
      select(id_abs, rok_abs, okres, id_platnika, kod_zus) %>%
      left_join(tbl(con, "w22") %>%
                  select(kod_zus, mlodoc),
                by = "kod_zus") %>%
      group_by(id_abs, rok_abs) %>%
      filter(any(mlodoc, na.rm = TRUE)) %>%
      ungroup()
    tMlodoc <- tMlodoc %>%
      filter(okres > 12L*rok_abs + 6L) %>%
      left_join(tMlodoc %>%
                  filter(mlodoc) %>%
                  select(id_abs, rok_abs, id_platnika) %>%
                  mutate(platnik_mlodoc = TRUE) %>%
                  distinct(),
                by = c("id_abs", "rok_abs", "id_platnika")) %>%
      group_by(id_abs, rok_abs, okres) %>%
      summarise(mlodoc_byl = any(mlodoc, na.rm = TRUE),
                mlodoc_ten_sam_platnik = any(platnik_mlodoc, na.rm = TRUE),
                .groups = "drop")
    # łączenie
    t15 <- collect(t15) # to trwa około 10 minut!
    t16 <- collect(t16)
    t17 <- collect(t17)
    tMlodoc <- collect(tMlodoc)
    tabelePosrednie$p3 <- t1 %>%
      left_join(t16 %>%
                  left_join(t17, by = c("id_abs", "rok_abs", "okres")) %>%
                  left_join(t15, by = c("id_abs", "rok_abs", "okres")) %>%
                  mutate(przerwa = if_else(is.na(przerwa), FALSE, przerwa),
                         status_nieustalony = 0L) %>%
                  mutate(biernosc =
                           as.integer((biernosc | przerwa) & praca %in% 0L)) %>%
                  select(-przerwa) %>%
                  rename(teryt_zam = teryt) %>%
                  left_join(tMlodoc, by = c("id_abs", "rok_abs", "okres")),
                by = c("id_abs", "rok_abs", "okres")) %>%
      left_join(t34512m, by = c("id_abs", "rok_abs", "okres")) %>%
      mutate(nauka_szk_abs =
               if_else(okres <= (12L*rok_abs + 6L), 1L, 0L)) %>%
      group_by(id_abs, rok_abs) %>%
      mutate(nauka_szk_abs =
               if_else(okres >= (12L*rok_abs + 7L) &
                         okres <= (12L*rok_abs + 9L) &
                         (cumsum(ifelse(okres < (12L*rok_abs + 7L) |
                                          okres > (12L*rok_abs + 9L),
                                        0L, 1L)) < 0L | nauka2 == 0) &
                         (cumsum(nauka2)[okres == (12L*rok_abs + 10L)] -
                            cumsum(nauka2)) > 0,
                       1L, nauka_szk_abs)) %>%
      ungroup() %>%
      mutate(status_nieustalony =
               if_else(is.na(status_nieustalony), 1L, status_nieustalony),
             across(starts_with("nauka"), ~if_else(is.na(.), 0L, as.integer(.))),
             mlodoc_ten_sam_platnik =
               if_else(is.na(mlodoc_ten_sam_platnik) & !is.na(mlodoc_byl),
                       FALSE, mlodoc_ten_sam_platnik)) %>%
      mutate(kont_mlodoc_prac =
               case_when(!is.na(mlodoc_byl) & nauka == 0L &
                           praca_do_kont_mlodociany & !mlodoc_ten_sam_platnik ~ 1L,
                         !is.na(mlodoc_byl) & nauka == 0L &
                           praca %in% c(1L, 4L, 5L, 7L) & mlodoc_ten_sam_platnik ~ 2L,
                         !is.na(mlodoc_byl) & nauka == 0L &
                           praca_do_kont_mlodociany & mlodoc_ten_sam_platnik ~ 3L,
                         !is.na(mlodoc_byl) & nauka == 1L &
                           praca %in% c(1L, 4L, 5L, 7L) & mlodoc_ten_sam_platnik ~ 5L,
                         !is.na(mlodoc_byl) & nauka == 1L ~ 4L)) %>%
      select(-rok_skladka, -mies_skladka, -praca_do_kont_mlodociany,
             -mlodoc_byl, -mlodoc_ten_sam_platnik) %>%
      select(id_abs:nauka2, nauka_szk_abs, everything())
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  # P4 (dane stałe w czasie i kilka fikuśnych wskaźników) ######################
  if (przygotujP4) {
    cat("\nPrzygotowywanie tabeli P4...")
    # średnie wynagrodzenia w zawodach dla danego rocznika
    tWynagrZawod <- tabelePosrednie$p3 %>%
      filter(!is.na(praca), praca > 0L, !is.na(wynagrodzenie)) %>%
      left_join(collect(t2), by = c("id_abs", "rok_abs")) %>%
      group_by(rok_abs, kod_zaw) %>%
      summarise(zawod_sr_wynagrodzenie =
                  mean(as.numeric(wynagrodzenie), na.rm = TRUE),
                .groups = "drop")
    # liczba pracodawców w okresie od lipca roku ukończenia szkoły do grudnia
    # roku poprzedzającego monitoring
    tPracodawcy <- tbl(con, "w16") %>%
      filter(12L*rok_skladka + mies_skladka > 12L*rok_abs + 6L,
             12L*rok_skladka + mies_skladka <= 12L*rokMonitoringu) %>%
      left_join(tbl(con, "w22") %>%
                  select(kod_zus, etat = etat_ibe, netat = netat_ibe),
                by = "kod_zus") %>%
      collect() %>%
      left_join(tabelePosrednie$p3 %>%
                  select(id_abs, rok_abs, okres, nauka),
                by = c("id_abs", "rok_abs")) %>%
      group_by(id_abs, rok_abs) %>%
      summarise(l_prac_ucz_uop = n_distinct(id_platnika[nauka %in% 1L & etat]),
                l_prac_nucz_uop = n_distinct(id_platnika[nauka %in% 0L & etat]),
                l_prac_nucz_nuop = n_distinct(id_platnika[nauka %in% 0L & netat]),
                .groups = "drop")
    # łączenie i zapis
    tabelePosrednie$p4 <- tbl(con, "w2") %>%
      mutate(teryt_pow_szk = 100L*floor(teryt_szk / 1000),
             teryt_woj_szk = 10000L*floor(teryt_szk / 100000)) %>%
      left_join(t20, by = "kod_zaw") %>%
      select(id_abs, rok_abs, rok_ur, plec, id_szk, typ_szk, teryt_pow_szk,
             teryt_woj_szk, lp, kod_zaw, nazwa_zaw, branza) %>%
      collect() %>%
      left_join(tPracodawcy, by = c("id_abs", "rok_abs")) %>%
      left_join(tWynagrZawod, by = c("rok_abs", "kod_zaw")) %>%
      left_join(tabelePosrednie$p1 %>%
                  filter(rodzaj_dyplomu %in% c("certyfikat kwalifikacji",
                                               "dyplom zawodowy", "matura")) %>%
                  select(id_abs, rok_abs) %>%
                  distinct() %>%
                  mutate(abs_w_cke = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      left_join(tabelePosrednie$p3 %>%
                  filter(nauka %in% 1L & !(nauka_studia %in% 1L)) %>%
                  select(id_abs, rok_abs) %>%
                  distinct() %>%
                  mutate(abs_w_sio = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      left_join(tabelePosrednie$p3 %>%
                  filter(nauka_studia %in% 1L) %>%
                  select(id_abs, rok_abs) %>%
                  distinct() %>%
                  mutate(abs_w_polon = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      left_join(tabelePosrednie$p3 %>%
                  filter(status_nieustalony %in% 0L) %>%
                  select(id_abs, rok_abs) %>%
                  distinct() %>%
                  mutate(abs_w_zus = TRUE),
                by = c("id_abs", "rok_abs")) %>%
      mutate(across(starts_with("abs_w_"), ~if_else(is.na(.), FALSE, .)),
             across(starts_with("l_prac_"), ~if_else(is.na(.), 0L, .)))
    cat(" zakończone. ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), sep = "")
  }
  cat("\nKoniec: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")

  return(tabelePosrednie)
}
