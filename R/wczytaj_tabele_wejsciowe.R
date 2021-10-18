#' @title Wczytywanie danych do bazy z plikow wejsciowych
#' @description Funkcja szuka w podanym folderze plików CSV z danymi
#' z \emph{tabel wejściowych}, wczytuje je, kontroluje ich spójność i zapisuje
#' do bazy.
#' @param baza uchwyt połączenia do bazy lub lista argumentów do funkcji
#' \code{\link[DBI]{dbConnect}} umożliwiających nawiązanie połączenia z bazą
#' danych, w której mają zostać zapisane wczytwane dane
#' @param folder opcjonalnie ścieżka do folderu zawierającego pliki CSV z danymi
#' (domyślnie używany jest aktywny katalog roboczy)
#' @param wczytajDoBazy opcjonalnie wartość logiczna - czy przeprowadzić zapis
#' do bazy? (jeśli \code{FALSE}, funkcja tylko wczyta pliki CSV i zaraportuje
#' wykryte w nich problemy)
#' @param zapiszProblemy opcjonalnie wartość logiczna - czy funkcja ma zapisywać
#' zestawienia wykrytych we wczytanych plikach problemów w formie plików CSV?
#' @details Dane wejściowe powinny obejmować \strong{tylko jeden rok prowadzenia
#' monitoringu}, ale mogą obejmować \strong{kilka różnych okresów od ukończenia
#' szkoły}.
#' @return \code{NULL}
#' @seealso \code{\link{tabele_wejsciowe}}
#' @importFrom DBI dbConnect dbExecute dbDisconnect
#' @importFrom dplyr %>% add_count anti_join arrange bind_rows case_when count
#'                   distinct filter group_by if_else left_join mutate n
#'                   n_distinct pull rename select semi_join slice summarise
#'                   ungroup
#' @importFrom tidyr pivot_wider
#' @export
wczytaj_tabele_wejsciowe = function(baza, folder = ".", wczytajDoBazy = TRUE,
                                    zapiszProblemy = TRUE) {
  stopifnot(is.list(baza) | inherits(baza, "DBIConnection"),
            dir.exists(folder),
            length(wczytajDoBazy) == 1L, wczytajDoBazy %in% c(FALSE, TRUE),
            length(zapiszProblemy) == 1L, zapiszProblemy %in% c(FALSE, TRUE))
  if (is.list(baza)) {
    con = do.call(dbConnect, args = baza)
  } else {
    con = baza
  }
  on.exit({if (!inherits(baza, "DBIConnection")) dbDisconnect(con)})
  dbExecute(con, "BEGIN;")
  tabeleWejsciowe = names(tabele_wejsciowe())
  tabeleWejsciowe <- setNames(paste0(sub("[\\/]$", "", folder), "/",
                                     tabeleWejsciowe, ".csv"),
                              tabeleWejsciowe)
  brakujacePliki <- tabeleWejsciowe[!file.exists(tabeleWejsciowe)]
  if (length(brakujacePliki) > 0L) {
    stop("W podanym folderze brak następujących plików: ",
         paste(sub(paste0(sub("[\\/]$", "", folder), "/"), "",
                   brakujacePliki, fixed = TRUE), collapse = ", "))
  }

  # wczytywanie plików CSV z danymi ############################################
  tabeleWejsciowe <- as.list(tabeleWejsciowe)
  brakujaceKolumny <- setNames(vector(mode = "list",
                                      length = length(tabeleWejsciowe)),
                               names(tabeleWejsciowe))
  for (i in 1L:length(tabeleWejsciowe)) {
    cat("Wczytywanie pliku", tabeleWejsciowe[[i]], "\n")
    tabeleWejsciowe[[i]] <- arrow::read_delim_arrow(tabeleWejsciowe[[i]],
                                                    delim = ";")
    brakujaceKolumny[[i]] <- setdiff(tabele_wejsciowe()[[names(tabeleWejsciowe)[i]]],
                                     names(tabeleWejsciowe[[i]]))
    if (length(brakujaceKolumny[[i]]) > 0L) {
      cat("  Brakuje wymaganych kolumn: '",
          paste0(brakujaceKolumny[[i]], collapse = "', '"), "'\n", sep = "")
    }
  }
  if (sum(sapply(brakujaceKolumny, length)) > 0L) {
    stop("Brak wymaganych kolumn w co najmniej jednym z wczytywanych plików (p. informacje powyżej).")
  }
  cat("\nPoczątek przetwarzania danych: ",
      format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")
  # identyfikowanie i wykluczanie ewidentnie zduplikowanych absolwentów ########
  cat("\nSzukanie zduplikowanych ID absolwenta...")
  duplikatyIdW2 <- tabeleWejsciowe$W2 %>%
    group_by(ID_ABS) %>%
    mutate(n_rok_ur = n_distinct(ROK_UR),
           n_plec = n_distinct(PLEC)) %>%
    ungroup() %>%
    filter(n_rok_ur > 1L | n_plec > 1L)
  duplikatyId <- duplikatyIdW2 %>%
    select(ID_ABS, ROK_UR, PLEC) %>%
    distinct() %>%
    mutate(ID_ABS_new = -1L * seq(1L:n()))
  cat(" zakończono.\n")
  if (nrow(duplikatyId) > 0L) {
    message("Wykryto ", nrow(duplikatyId), " ewidentnie zduplikowanych ID absolwenta (różne płcie lub lata urodzenia dla osób o tym samym ID).\nDane dotyczące tych ID zostaną wykluczone z importu.")
  }
  tabeleWejsciowe$W1 <- tabeleWejsciowe$W1 %>%
    left_join(duplikatyId, by = "ID_ABS") %>%
    mutate(ID_ABS = if_else(is.na(ID_ABS_new), ID_ABS, ID_ABS_new)) %>%
    select(-ID_ABS_new)
  tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
    left_join(duplikatyId, by = c("ID_ABS", "ROK_UR", "PLEC")) %>%
    mutate(ID_ABS = if_else(is.na(ID_ABS_new), ID_ABS, ID_ABS_new)) %>%
    select(-ID_ABS_new)
  # import danych do bazy ######################################################
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W1 (unikalni absolwenci - bez względu na szkołę i zawód)...")
    # W1 (unikalni absolwenci)
    dbExecute(con,
              "INSERT INTO w1 (id_abs, rok_abs) VALUES ($1, $2)",
              params = list(tabeleWejsciowe$W1$ID_ABS,
                            tabeleWejsciowe$W1$ROK_ABS))
    cat(" zakończony.")
    # Słownik typów szkół
    cat("\nZapis do bazy tabeli TYPY_SZKOL...")
    dbExecute(con,
              "INSERT INTO typy_szkol (typ_szk) VALUES ($1)",
              params = list(tabeleWejsciowe$STYPSZK$TYP_SZK_NAZWA))
    cat(" zakończony.")
  }
  ## W20a i W20 (słownik zawodów i przypisanie zawodów do branż) ###############
  tabeleWejsciowe$W20 <- tabeleWejsciowe$W20 %>%
    bind_rows(tabeleWejsciowe$W20aneks %>%
                anti_join(tabeleWejsciowe$W20 %>%
                            select(KOD_ZAW, BRANZA) %>%
                            filter(grepl(" branża ", BRANZA)),
                          by = c("KOD_ZAW", "BRANZA"))) %>%
    mutate(BRANZA = sub(paste0("^(", paste(unique(BRANZA_KOD), collapse = "|"),
                               ") - "),
                        "", BRANZA)) %>%
    distinct()
  tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
    mutate(KOD_ZAW = case_when(tolower(KOD_ZAW) %in% "e" ~ 10101L,
                               tolower(KOD_ZAW) %in% "i" ~ 10102L,
                               TRUE ~ suppressWarnings(as.integer(KOD_ZAW))))
  tabeleWejsciowe$W3 <- tabeleWejsciowe$W3 %>%
    mutate(KOD_ZAW_KONT = case_when(tolower(KOD_ZAW_KONT) %in% "e" ~ 10101L,
                                    tolower(KOD_ZAW_KONT) %in% "i" ~ 10102L,
                                    tolower(KOD_ZAW_KONT) %in% "p" ~ 10103L,
                                    TRUE ~ suppressWarnings(
                                      as.integer(KOD_ZAW_KONT))))
  tabeleWejsciowe$W11 <- tabeleWejsciowe$W11 %>%
    mutate(KOD_ZAW = case_when(tolower(KOD_ZAW) %in% "eksper." ~ 10101L,
                               TRUE ~ suppressWarnings(as.integer(KOD_ZAW))))
  tabeleWejsciowe$W20a <- bind_rows(select(tabeleWejsciowe$W20, KOD_ZAW),
                                select(tabeleWejsciowe$W2, KOD_ZAW),
                                select(tabeleWejsciowe$W3, KOD_ZAW = KOD_ZAW_KONT),
                                select(tabeleWejsciowe$W5, KOD_ZAW = KOD_ZAW_KUZ),
                                select(tabeleWejsciowe$W6, KOD_ZAW = KOD_ZAW_CZEL),
                                select(tabeleWejsciowe$W9, KOD_ZAW),
                                select(tabeleWejsciowe$W11, KOD_ZAW)) %>%
    distinct() %>%
    filter(!is.na(KOD_ZAW))
  zawodyBezMapowania <- tabeleWejsciowe$W20a %>%
    anti_join(tabeleWejsciowe$W20, by = "KOD_ZAW") %>%
    filter(!(KOD_ZAW %in% (10101L:10103L)))
  if (nrow(zawodyBezMapowania) > 0L) {
    message("\nWykryto następujące kody zawodów, dla których brak jest mapowania na branże:\n",
            paste(zawodyBezMapowania$KOD_ZAW, collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W20aneks.csv'.")
    if (zapiszProblemy) {
      write.csv2(zawodyBezMapowania, "kody-zawodów-bez-przypisania-do-branż.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabel W20 i W20a (kody zawodów i ich mapowanie na branże)...")
    dbExecute(con,
              "INSERT INTO w20a (kod_zaw) VALUES ($1)",
              params = list(tabeleWejsciowe$W20a$KOD_ZAW))
    dbExecute(con,
              "INSERT INTO w20 (kod_zaw, branza, branza_kod) VALUES ($1, $2, $3)",
              params = tabeleWejsciowe$W20 %>%
                filter(!is.na(BRANZA)) %>%
                select(KOD_ZAW, BRANZA, BRANZA_KOD) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  ## W2 (stałe w czasie charakterystyki absolwentów) ###########################
  tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
    left_join(tabeleWejsciowe$STYPSZK, by = "TYP_SZK") %>%
    select(-TYP_SZK) %>%
    rename(TYP_SZK = TYP_SZK_NAZWA) %>%
    group_by(ID_ABS, ROK_ABS, ID_SZK) %>%
    mutate(lp = 1L:n()) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W2 (absolwenci poszczególnych szkół w poszczególnych zawodach)...")
    dbExecute(con,
              "INSERT INTO w2 (id_abs, rok_abs, rok_ur, plec, id_szk, typ_szk,
                               teryt_szk, lp, kod_zaw, nazwa_zaw)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
              params = tabeleWejsciowe$W2 %>%
                select(ID_ABS, ROK_ABS, ROK_UR, PLEC, ID_SZK, TYP_SZK, TERYT_SZK,
                       lp, KOD_ZAW, NAZWA_ZAW) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  ### zestawienie szkół z absolwentami w wielu zawodach w tym samym roku #######
  multiAbsolwenci <- tabeleWejsciowe$W2 %>%
    anti_join(select(duplikatyId, ID_ABS), by = "ID_ABS") %>%
    group_by(ID_SZK, TYP_SZK, ROK_ABS, ID_ABS) %>%
    summarise(n_zaw = n(), .groups = "drop_last") %>%
    count(n_zaw) %>%
    mutate(max_n_zaw = max(n_zaw)) %>%
    ungroup() %>%
    mutate(n_zaw = factor(n_zaw, sort(unique(n_zaw)))) %>%
    pivot_wider(names_from = n_zaw, values_from = n, names_prefix = "n_zaw",
                names_sort = TRUE, values_fill = 0L) %>%
    filter(max_n_zaw > 1L)
  if (nrow(multiAbsolwenci) > 0L) {
    message("\nWykryto szkoły, w których te same osoby są absolwentami więcej niż jednego zawodu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych szkół zostanie zapisane w pliku 'szkoły-z-multiabsolwentami.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_pliki_w()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(multiAbsolwenci, "szkoły-z-multiabsolwentami.csv", row.names = FALSE,
                 fileEncoding = "UTF-8")
    }
  }
  multiAbsolwenciKont <- tabeleWejsciowe$W3 %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    group_by(ID_SZK_KONT, TYP_SZK_KONT, DATA_OD_SZK_KONT, ID_ABS) %>%
    summarise(n_zaw = n(), .groups = "drop_last") %>%
    count(n_zaw) %>%
    mutate(max_n_zaw = max(n_zaw)) %>%
    ungroup() %>%
    mutate(n_zaw = factor(n_zaw, sort(unique(n_zaw)))) %>%
    pivot_wider(names_from = n_zaw, values_from = n, names_prefix = "n_zaw",
                names_sort = TRUE, values_fill = 0L) %>%
    filter(max_n_zaw > 1L)
  if (nrow(multiAbsolwenciKont) > 0L) {
    message("\nWśród szkół, w których absolwenci kontynuują naukę wykryto takie, w których te same osoby są absolwentami więcej niż jednego zawodu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych szkół zostanie zapisane w pliku 'szkoły-z-multiabsolwentami-kontynuacja-nauki.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_pliki_w()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(multiAbsolwenciKont,
                 "szkoły-z-multiabsolwentami-kontynuacja-nauki.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  ## W3 (kontynuacja nauki w szkołach objętych SIO) ############################
  tabeleWejsciowe$W3 <- tabeleWejsciowe$W3 %>%
    left_join(tabeleWejsciowe$STYPSZK %>%
                rename(TYP_SZK_KONT = TYP_SZK),
              by = "TYP_SZK_KONT") %>%
    select(-TYP_SZK_KONT) %>%
    rename(TYP_SZK_KONT = TYP_SZK_NAZWA) %>%
    mutate(DATA_DO_SZK_KONT = ifelse(DATA_DO_SZK_KONT == "31-12-9999",
                                     NA,
                                     DATA_DO_SZK_KONT)) %>%
    mutate(DATA_OD_SZK_KONT = as.Date(DATA_OD_SZK_KONT, "%d-%m-%Y"),
           DATA_DO_SZK_KONT = as.Date(DATA_DO_SZK_KONT, "%d-%m-%Y"),
           CZY_UKONCZ_SZK_KONT = if_else(!is.na(DATA_DO_SZK_KONT) &
                                           CZY_UKONCZ_SZK_KONT %in% 0L,
                                         1L, CZY_UKONCZ_SZK_KONT)) %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    group_by(ID_ABS, ROK_ABS, ID_SZK_KONT, DATA_OD_SZK_KONT) %>%
    mutate(lp = 1L:n()) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W3 (kontynuacja nauki w szkołach objętych SIO)...")
    dbExecute(con,
              "INSERT INTO w3 (id_abs, rok_abs, id_szk_kont, typ_szk_kont,
                               data_od_szk_kont, lp, czy_ukoncz_szk_kont,
                               data_do_szk_kont, kod_zaw_kont)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
             params = tabeleWejsciowe$W3 %>%
               select(ID_ABS, ROK_ABS, ID_SZK_KONT, TYP_SZK_KONT, DATA_OD_SZK_KONT,
                      lp, CZY_UKONCZ_SZK_KONT, DATA_DO_SZK_KONT, KOD_ZAW_KONT) %>%
               as.list() %>%
               unname())
    cat(" zakończony.")
  }
  ## W21a i W21 (słownik kwalifikacji i przypisanie kwalifikacji do zawodów) ####
  tabeleWejsciowe$W21a <- bind_rows(select(tabeleWejsciowe$W21, KOD_KWAL),
                                select(tabeleWejsciowe$W4, KOD_KWAL = KOD_KWAL_KKZ),
                                select(tabeleWejsciowe$W8, KOD_KWAL),
                                select(tabeleWejsciowe$W10, KOD_KWAL)) %>%
    distinct() %>%
    filter(!is.na(KOD_KWAL))
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabel W21 i W21a (mapowanie kodów kwalifikacji na kody zawodów)...")
    dbExecute(con,
              "INSERT INTO w21a (kod_kwal) VALUES ($1)",
              params = list(tabeleWejsciowe$W21a$KOD_KWAL))
    dbExecute(con,
              "INSERT INTO w21 (kod_kwal, kod_zaw) VALUES ($1, $2)",
              params = tabeleWejsciowe$W21 %>%
                select(KOD_KWAL, KOD_ZAW) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  ## W4 (KKZ) ##################################################################
  tabeleWejsciowe$W4 <- tabeleWejsciowe$W4 %>%
    mutate(DATA_DO_KKZ = ifelse(DATA_DO_KKZ == "31-12-9999",
                                NA,
                                DATA_DO_KKZ)) %>%
    mutate(DATA_OD_KKZ = as.Date(DATA_OD_KKZ, "%d-%m-%Y"),
           DATA_DO_KKZ = as.Date(DATA_DO_KKZ, "%d-%m-%Y")) %>%
    group_by(ID_ABS, ROK_ABS, ID_SZK_KONT) %>%
    mutate(lp = 1L:n()) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W4 (uczestnictwo w KKZ)...")
    dbExecute(con,
              "INSERT INTO w4 (id_abs, rok_abs, id_szk_kont, lp, data_od_kkz,
                               data_do_kkz, kod_kwal_kkz)
               VALUES ($1, $2, $3, $4, $5, $6, $7)",
             params = tabeleWejsciowe$W4 %>%
               select(ID_ABS, ROK_ABS, ID_SZK_KONT, lp, DATA_OD_KKZ, DATA_DO_KKZ,
                      KOD_KWAL_KKZ) %>%
               as.list() %>%
               unname())
    cat(" zakończony.")
  }
  ## W5 (KUZ) ##################################################################
  tabeleWejsciowe$W5 <- tabeleWejsciowe$W5 %>%
    mutate(DATA_DO_KUZ = ifelse(DATA_DO_KUZ == "31-12-9999",
                                NA,
                                DATA_DO_KUZ)) %>%
    mutate(DATA_OD_KUZ = as.Date(DATA_OD_KUZ, "%d-%m-%Y"),
           DATA_DO_KUZ = as.Date(DATA_DO_KUZ, "%d-%m-%Y")) %>%
    group_by(ID_ABS, ROK_ABS, ID_SZK_KONT) %>%
    mutate(lp = 1L:n()) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W5 (uczestnictwo w KUZ)...")
    dbExecute(con,
              "INSERT INTO w5 (id_abs, rok_abs, id_szk_kont, lp, data_od_kuz,
                               data_do_kuz, kod_zaw_kuz)
               VALUES ($1, $2, $3, $4, $5, $6, $7)",
             params = tabeleWejsciowe$W5 %>%
               select(ID_ABS, ROK_ABS, ID_SZK_KONT, lp, DATA_OD_KUZ, DATA_DO_KUZ,
                      KOD_ZAW_KUZ) %>%
               as.list() %>%
               unname())
    cat(" zakończony.")
  }
  ## W6 (tytuły czeladnika) ####################################################
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W6 (tytuły czeladnika)...")
    dbExecute(con,
              "INSERT INTO w6 (id_abs, rok_abs, kod_zaw_czel) VALUES ($1, $2, $3)",
              params = tabeleWejsciowe$W6 %>%
                select(ID_ABS, ROK_ABS, KOD_ZAW_CZEL) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  ## W7 (matury) ###############################################################
  # uwaga! agreguję po absolwento-latach matury!
  tabeleWejsciowe$W7 <- tabeleWejsciowe$W7 %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(CZY_ZDANA_MATURA = as.logical(CZY_ZDANA_MATURA),
           DATA_SWIAD_MATURA = as.Date(DATA_SWIAD_MATURA, "%d-%m-%Y")) %>%
    add_count(ID_ABS, ROK_ABS, ROK_MATURA) %>%
    arrange(ID_ABS, ROK_ABS, ROK_MATURA,
            desc(CZY_ZDANA_MATURA), DATA_SWIAD_MATURA) %>%
    group_by(ID_ABS, ROK_ABS, ROK_MATURA) %>%
    slice(1L) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W7 (wyniki egzaminu maturalnego)...")
    dbExecute(con,
              "INSERT INTO w7 (id_abs, rok_abs, rok_matura, czy_zdana_matura,
                               data_swiad_matura) VALUES ($1, $2, $3, $4, $5)",
              params = tabeleWejsciowe$W7 %>%
                select(ID_ABS, ROK_ABS, ROK_MATURA, CZY_ZDANA_MATURA,
                       DATA_SWIAD_MATURA) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  ## W8 i W10 (świadectwa potwierdzające kwalifikacje) #########################
  # uwaga! agreguję po absolwento-kwalifikacjach! (wybierając pierwsze zdanie)
  # uwaga! DATA_KWAL może opisywać różne rzeczy w zależności od OKE (zapewne:
  # datę faktycznego potwierdzenia zdania egzaminu, umowną datę zdania egzaminu
  # lub datę odbioru certfikatu)
  tabeleWejsciowe$W8 <- tabeleWejsciowe$W8 %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_KWAL = as.Date(DATA_KWAL, "%d-%m-%Y"))
  tabeleWejsciowe$W10 <- tabeleWejsciowe$W10 %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_KWAL = as.Date(DATA_KWAL, "%d-%m-%Y"))
  multiKwal <- bind_rows(list(W8 = tabeleWejsciowe$W8,
                              W10 = tabeleWejsciowe$W10), .id = "table") %>%
    distinct() %>%
    group_by(ID_ABS, ROK_ABS, KOD_KWAL) %>%
    mutate(n = n(),
           rozne_tabele = n_distinct(table)) %>%
    ungroup() %>%
    filter(n > 1L)
  if (nrow(multiKwal) > 0L) {
    message("\nWykryto osoby, które kilkakrotnie zdawały egzaminy na tę samą kwalifikację.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych osób i kwalifikacji zostanie zapisane w pliku 'wielokrotne-kwalifikacje.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to osoby i kwalifikacje, uruchom funkcję `wczytaj_pliki_w()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(multiKwal,
                 "wielokrotne-kwalifikacje.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W810 (certyfikaty kwalifikacji)...")
    dbExecute(con,
              "INSERT INTO w810 (id_abs, rok_abs, kod_kwal, rok_kwal,
                                 dok_potw_kwal, data_kwal)
               VALUES ($1, $2, $3, $4, $5, $6)",
              params = bind_rows(tabeleWejsciowe$W8,
                                 tabeleWejsciowe$W10) %>%
                select(ID_ABS, ROK_ABS, KOD_KWAL, ROK_KWAL, DOK_POTW_KWAL,
                       DATA_KWAL) %>%
                distinct() %>%
                arrange(ID_ABS, ROK_ABS, KOD_KWAL, ROK_KWAL, DATA_KWAL) %>%
                group_by(ID_ABS, ROK_ABS, KOD_KWAL) %>%
                slice(1L) %>%
                ungroup() %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  ## W9 i W11 (dyplomy zawodowe) ###############################################
  # uwaga! agreguję po absolwento-zawodach!
  # uwaga! DATA_DYP_ZAW może opisywać różne rzeczy w zależności od OKE (zapewne:
  # datę faktycznego zdania ostatniego wymaganego egzaminu, umowną datę zdania
  # ostatniego wymaganego egzaminu lub datę odbioru dyplomu)
  tabeleWejsciowe$W9 <- tabeleWejsciowe$W9 %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_DYP_ZAW = as.Date(DATA_DYP_ZAW, "%d-%m-%Y"),
           ROK_DYP = as.numeric(format(DATA_DYP_ZAW, "%Y")))
  tabeleWejsciowe$W11 <- tabeleWejsciowe$W11 %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_DYP_ZAW = as.Date(DATA_DYP_ZAW, "%d-%m-%Y"),
           ROK_DYP = as.numeric(format(DATA_DYP_ZAW, "%Y")))
  multiDypZaw <- bind_rows(list(W9 = tabeleWejsciowe$W9,
                                W11 = tabeleWejsciowe$W11), .id = "table") %>%
    distinct() %>%
    group_by(ID_ABS, ROK_ABS, KOD_ZAW) %>%
    mutate(n = n(),
           rozne_tabele = n_distinct(table)) %>%
    filter(n > 1L)
  if (nrow(multiDypZaw) > 0L) {
    message("\nWykryto osoby, które kilkakrotnie uzyskały dyplom w tym samym zawodzie.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych osób i zawodów zostanie zapisane w pliku 'wielokrotne-dyplomy.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to osoby i zawody, uruchom funkcję `wczytaj_pliki_w()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(multiDypZaw,
                 "wielokrotne-dyplomy.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W911 (dyplomy zawodowe)...")
    dbExecute(con,
              "INSERT INTO w911 (id_abs, rok_abs, kod_zaw, rok_dyp, dok_potw_dyp,
                                 data_dyp_zaw) VALUES ($1, $2, $3, $4, $5, $6)",
              params = bind_rows(tabeleWejsciowe$W9,
                                 tabeleWejsciowe$W11) %>%
                select(ID_ABS, ROK_ABS, KOD_ZAW, ROK_DYP, DOK_POTW_DYP,
                       DATA_DYP_ZAW) %>%
                distinct() %>%
                arrange(ID_ABS, ROK_ABS, KOD_ZAW, ROK_DYP, DATA_DYP_ZAW) %>%
                group_by(ID_ABS, ROK_ABS, KOD_ZAW) %>%
                slice(1L) %>%
                ungroup() %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  ## W13 (przypisanie kierunków studiów do dziedzin i dyscyplin) ###############
  tabeleWejsciowe$W13 <- tabeleWejsciowe$W13 %>%
    mutate(CZY_DYSCYPLINA_WIODACA = as.logical(CZY_DYSCYPLINA_WIODACA))
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabel W13 i W13a (mapowanie kierunków studiów na dziedziny i dyscypliny)...")
    dbExecute(con,
              "INSERT INTO w13a (id_kierunku_stu) VALUES ($1)",
              params = list(unique(c(tabeleWejsciowe$W12 %>%
                                       filter(!is.na(ID_KIERUNKU_STU)) %>%
                                       pull(ID_KIERUNKU_STU),
                                     tabeleWejsciowe$W13$ID_KIERUNKU_STU))))
    dbExecute(con,
              "INSERT INTO w13 (id_kierunku_stu, dziedzina, dyscyplina,
                            czy_dyscyplina_wiodaca) VALUES ($1, $2, $3, $4)",
              params = tabeleWejsciowe$W13 %>%
                select(ID_KIERUNKU_STU, DZIEDZINA, DYSCYPLINA,
                       CZY_DYSCYPLINA_WIODACA) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  ## W12 (studiowanie) #########################################################
  # uwaga! agreguję po absolwento-kierunko-datach_rozpoczęcia! (w ramach tego
  # samego kierunku, rozpoczętego z tą samą datą można np. zmieniać tryb - i nie
  # tylko tryb - co generuje oddzielne rekordy w pliku przysyłanym przez OPI)
  tabeleWejsciowe$W12 <- tabeleWejsciowe$W12 %>%
    filter(!is.na(ID_KIERUNKU_STU)) %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_OD_STU = as.Date(DATA_OD_STU, "%d-%m-%Y"),
           DATA_DO_STU = as.Date(DATA_DO_STU, "%d-%m-%Y"),
           DATA_SKRESL_STU = as.Date(DATA_SKRESL_STU, "%d-%m-%Y")) %>%
    arrange(ID_ABS, ROK_ABS, ID_KIERUNKU_STU, DATA_OD_STU,
            desc(DATA_DO_STU), CZY_UKONCZ_STU, desc(DATA_SKRESL_STU)) %>%
    group_by(ID_ABS, ROK_ABS, ID_KIERUNKU_STU, DATA_OD_STU) %>%
    slice(1) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W12 (kontynuacja nauki na studiach)...")
    dbExecute(con,
              "INSERT INTO w12 (id_abs, rok_abs, id_kierunku_stu, kierunek_stu,
                                profil_stu, data_od_stu, czy_ukoncz_stu,
                                data_do_stu, tytul_zaw_stu, data_skresl_stu)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
           params = tabeleWejsciowe$W12 %>%
             select(ID_ABS, ROK_ABS, ID_KIERUNKU_STU, KIERUNEK_STU, PROFIL_STU,
                    DATA_OD_STU, CZY_UKONCZ_STU, DATA_DO_STU, TYTUL_ZAW_STU,
                    DATA_SKRESL_STU) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W14 (ew. zgony) ###########################################################
  tabeleWejsciowe$W14 <- tabeleWejsciowe$W14 %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    filter(!is.na(ROK_ZGONU))
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W14 (ew. zgony - z danych ZUS)...")
    dbExecute(con,
              "INSERT INTO w14 (id_abs, rok_abs, rok_zgonu, mies_zgonu)
               VALUES ($1, $2, $3, $4)",
           params = tabeleWejsciowe$W14 %>%
             select(ID_ABS, ROK_ABS, ROK_ZGONU, MIES_ZGONU) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W15 (zmiany adresów - z ZUS) ##############################################
  # uwaga, nie ma żadnej gwarancji, że dane ZUS o adresach są spójne
  # i wyczerpujące i generalnie nie należy tego od nich oczekiwać
  tabeleWejsciowe$W15 <- tabeleWejsciowe$W15 %>%
    filter(!is.na(ADRES_TYP)) %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_ADR_OD = as.Date(DATA_ADR_OD, "%d-%m-%Y"),
           DATA_ADR_DO = as.Date(DATA_ADR_DO, "%d-%m-%Y")) %>%
    arrange(ID_ABS, ROK_ABS, ADRES_TYP, DATA_ADR_OD, DATA_ADR_OD) %>%
    group_by(ID_ABS, ROK_ABS, ADRES_TYP, DATA_ADR_OD) %>%
    mutate(lp = 1L:n()) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W15 (dane adresowe z ZUS)...")
    dbExecute(con,
              "INSERT INTO w15 (id_abs, rok_abs, adres_typ, data_adr_od, lp,
                            data_adr_do, teryt)
               VALUES ($1, $2, $3, $4, $5, $6, $7)",
           params = tabeleWejsciowe$W15 %>%
             select(ID_ABS, ROK_ABS, ADRES_TYP, DATA_ADR_OD, lp, DATA_ADR_DO,
                    TERYT) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W18 (informacje o płatnikach) #############################################
  # uwaga! agreguję po płatnikach!
  tabeleWejsciowe$W18 <- tabeleWejsciowe$W18 %>%
    group_by(ID_PLATNIKA, ROK_WYREJ, MIES_WYREJ) %>%
    summarise(PKD = setdiff(unique(PKD), NA_character_), .groups = "drop")
  brakujacyPlatnicy <- bind_rows(select(tabeleWejsciowe$W16, ID_PLATNIKA),
                                 select(tabeleWejsciowe$W17, ID_PLATNIKA)) %>%
    distinct() %>%
    filter(!is.na(ID_PLATNIKA)) %>%
    anti_join(tabeleWejsciowe$W18, by = "ID_PLATNIKA")
  tabeleWejsciowe$W18 <- tabeleWejsciowe$W18 %>%
    bind_rows(brakujacyPlatnicy)
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W18 (dane o pracodawcach z ZUS)...")
    dbExecute(con,
              "INSERT INTO w18 (id_platnika, pkd, rok_wyrej, mies_wyrej)
               VALUES ($1, $2, $3, $4)",
           params = tabeleWejsciowe$W18 %>%
             select(ID_PLATNIKA, PKD, ROK_WYREJ, MIES_WYREJ) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W22 (mapowanie kodów tytułów składek ZUS) #################################
  tabeleWejsciowe$W22 <- tabeleWejsciowe$W22 %>%
    mutate(across(-c(KOD, OPIS), as.logical))
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W22 (mapowanie kodów składek ZUS na kategorie użyteczne analitycznie)...")
    dbExecute(con,
              "INSERT INTO w22 (kod_zus, opis, etat_ela, etat_ibe, skladka_szac_wynag,
                                netat_ibe, bierny_skladka, netat_ela, zlec_ela,
                                bezrob_ela, student_ela, zagranic_ela, prawnik_ela,
                                samoz_ela, nspraw_ela, rolnik_ela, rentemer_ela,
                                mundur_ela, dziecko, etatnokid, netatnokid_ela,
                                samoznokid_ela, inne_ela, macierzynski_ela,
                                dziecko_pracownik_ela, dziecko_samozatrudnienie_ela,
                                dziecko_zlecenie_ela, dziecko_bezpracy_ela,
                                wychowawczy_opieka, mlodoc, benepomspol,
                                bezrobotnystaz)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                   $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
                   $27, $28, $29, $30, $31, $32)",
           params = tabeleWejsciowe$W22 %>%
             select(KOD, OPIS, ETAT_ELA, ETAT_IBE, SKLADKA_SZAC_WYNAG, NETAT_IBE,
                    BIERNY_SKLADKA, NETAT_ELA, ZLEC_ELA, BEZROB_ELA, STUDENT_ELA,
                    ZAGRANIC_ELA, PRAWNIK_ELA, SAMOZ_ELA, NSPRAW_ELA, ROLNIK_ELA,
                    RENTEMER_ELA, MUNDUR_ELA, DZIECKO, ETATNOKID, NETATNOKID_ELA,
                    SAMOZNOKID_ELA, INNE_ELA, MACIERZYNSKI_ELA,
                    DZIECKO_PRACOWNIK_ELA, DZIECKO_SAMOZATRUDNIENIE_ELA,
                    DZIECKO_ZLECENIE_ELA, DZIECKO_BEZPRACY_ELA,
                    WYCHOWAWCZY_OPIEKA, MLODOC, BENEPOMSPOL, BEZROBOTNYSTAZ) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W16 (składki) #############################################################
  # uwaga! agreguję po absolwento-okreso-płatniko-kodach!
  tabeleWejsciowe$W16 <- tabeleWejsciowe$W16 %>%
    filter(!is.na(KOD_ZUS),
           KOD_ZUS != 90000L) %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(KOD_ZUS = floor(KOD_ZUS / 100),
           ROK_SKLADKA = as.integer(substr(OKRES_ROZL, 7L, 10L)),
           MIES_SKLADKA = as.integer(substr(OKRES_ROZL, 4L, 5L)),
           CZY_30 = as.logical(CZY_30),
           CZY_RSA = as.logical(CZY_RSA)) %>%
    add_count(ID_ABS, ROK_ABS, ROK_SKLADKA, MIES_SKLADKA, ID_PLATNIKA, KOD_ZUS)
  tabeleWejsciowe$W16 <- bind_rows(
    tabeleWejsciowe$W16 %>%
      filter(n == 1) %>%
      select(-n),
    tabeleWejsciowe$W16 %>%
      filter(n > 1) %>%
      select(-n) %>%
      group_by(ID_ABS, ROK_ABS, ROK_SKLADKA, MIES_SKLADKA, ID_PLATNIKA, KOD_ZUS) %>%
      summarise(across(c(CZY_30, CZY_RSA), any, na.rm = TRUE),
                across(starts_with("PODST_"), sum, na.rm = TRUE),
                .groups = "drop"))
  kodyZusBezMapowania <- tabeleWejsciowe$W16 %>%
    select(KOD_ZUS) %>%
    anti_join(tabeleWejsciowe$W22 %>%
                rename(KOD_ZUS = KOD),
              by = "KOD_ZUS")
  if (nrow(kodyZusBezMapowania) > 0L) {
    message("\nWykryto następujące kody składek ZUS, dla których brak jest mapowania na użyteczne analitycznie kategorie:\n",
            paste(kodyZusBezMapowania$KOD_ZUS, collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W22.csv'.")
    if (zapiszProblemy) {
      write.csv2(zawodyBezMapowania, "kody-ZUS-niewystępujące-w-W22.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W16 (miesięczne dane o składkach ZUS)...")
    dbExecute(con,
              "INSERT INTO w16 (id_abs, rok_abs, rok_skladka, mies_skladka,
                                id_platnika, kod_zus, podst_chor, podst_wypad,
                                podst_emer, podst_zdrow, czy_30, czy_rsa)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
           params = tabeleWejsciowe$W16 %>%
             select(ID_ABS, ROK_ABS, ROK_SKLADKA, MIES_SKLADKA, ID_PLATNIKA,
                    KOD_ZUS, PODST_CHOR, PODST_WYPAD, PODST_EMER, PODST_ZDROW,
                    CZY_30, CZY_RSA) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W23 (mapowanie kodów przerw w opłacaniu składek ZUS) ######################
  tabeleWejsciowe$W23 <- tabeleWejsciowe$W23 %>%
    mutate(across(-c(KOD, OPIS), as.logical))
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W23 (mapowanie kodów przerw w opłacaniu składek ZUS na kategorie użyteczne analitycznie)...")
    dbExecute(con,
              "INSERT INTO w23 (kod, opis, bierny_zawodowo)
               VALUES ($1, $2, $3)",
           params = tabeleWejsciowe$W23 %>%
             select(KOD, OPIS, BIERNY_ZAWODOWO) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W17 (przerwy w opłacaniu składek) #########################################
  tabeleWejsciowe$W17 <- tabeleWejsciowe$W17 %>%
    filter(!is.na(KOD_PRZERWY)) %>%
    filter(!(is.na(DATA_OD_PRZERWA) | is.na(DATA_DO_PRZERWA))) %>%
    semi_join(tabeleWejsciowe$W1, by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_OD_PRZERWA = as.Date(DATA_OD_PRZERWA, "%d-%m-%Y"),
           DATA_DO_PRZERWA = as.Date(DATA_DO_PRZERWA, "%d-%m-%Y")) %>%
    distinct()
  kodyPrzerwBezMapowania <- tabeleWejsciowe$W17 %>%
    select(KOD_PRZERWY) %>%
    anti_join(tabeleWejsciowe$W23 %>%
                rename(KOD_PRZERWY = KOD),
              by = "KOD_PRZERWY")
  if (nrow(kodyPrzerwBezMapowania) > 0L) {
    message("\nWykryto następujące kody przerw w opłacaniu składek ZUS, dla których brak jest mapowania na użyteczne analitycznie kategorie:\n",
            paste(kodyPrzerwBezMapowania$KOD_PRZERWY, collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W23.csv'.")
    if (zapiszProblemy) {
      write.csv2(zawodyPrzerwMapowania, "kody-przerw-ZUS-niewystępujące-w-W23.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W17 (przerwy w opłacaniu składek ZUS)...")
    dbExecute(con,
              "INSERT INTO w17 (id_abs, rok_abs, id_platnika, data_od_przerwa,
                                kod_przerwy, data_do_przerwa)
               VALUES ($1, $2, $3, $4, $5, $6)",
           params = tabeleWejsciowe$W17 %>%
             select(ID_ABS, ROK_ABS, ID_PLATNIKA, DATA_OD_PRZERWA, KOD_PRZERWY,
                    DATA_DO_PRZERWA) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W19 (wskaźniki z BDL) #####################################################
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W19 (wskaźniki rynku pracy w powiatach z BDL GUS)...")
    dbExecute(con,
              "INSERT INTO w19 (rok, miesiac, teryt, stopa_bezrobocia,
                            sr_wynagrodzenia)
               VALUES ($1, $2, $3, $4, $5)",
           params = tabeleWejsciowe$W19 %>%
             select(ROK, MIESIAC, TERYT, STOPA_BEZROBOCIA, SR_WYNAGRODZENIA) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  dbExecute(con, "COMMIT;")
  cat("\nKoniec: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")
  invisible(NULL)
}
