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
#' @param usunAbsWKilkuZaw czy absolwenci, którzy ukończyli kilka różnych
#' zawodów w tej samej szkole, \strong{niebędącej szkołą policealną} powinni
#' zostać wykluczeni z importu?
#' @param plikLogu opcjonalnie nazwa pliku, do którego ma być zapisany log
#' wywołania funkcji
#' @details Dane wejściowe powinny obejmować \strong{tylko jeden rok prowadzenia
#' monitoringu}, ale mogą obejmować \strong{kilka różnych okresów od ukończenia
#' szkoły}.
#' @return \code{NULL}
#' @seealso \code{\link{tabele_wejsciowe}}
#' @importFrom stats setNames
#' @importFrom utils write.csv2
#' @importFrom DBI dbConnect dbExecute dbDisconnect
#' @importFrom dplyr %>% add_count anti_join arrange bind_rows case_when count
#'                   distinct filter group_by if_all if_else left_join matches
#'                   mutate n n_distinct pull rename select semi_join slice
#'                   summarise ungroup
#' @importFrom tidyr pivot_wider
#' @importFrom lubridate year
#' @export
wczytaj_tabele_wejsciowe = function(baza, folder = ".", wczytajDoBazy = TRUE,
                                    zapiszProblemy = TRUE,
                                    usunAbsWKilkuZaw = TRUE, plikLogu = NULL) {
  stopifnot(is.list(baza) | inherits(baza, "DBIConnection"),
            dir.exists(folder),
            length(wczytajDoBazy) == 1L, wczytajDoBazy %in% c(FALSE, TRUE),
            length(zapiszProblemy) == 1L, zapiszProblemy %in% c(FALSE, TRUE),
            length(usunAbsWKilkuZaw) == 1L, usunAbsWKilkuZaw %in% c(FALSE, TRUE))
  if (!is.null(plikLogu)) {
    stopifnot(is.character(plikLogu), length(plikLogu) == 1L,
              !is.na(plikLogu),
              "Istnieje już plik o nazwie takiej jak ta podana argumentem `plikLogu` - usuń go z dysku lub podaj inną nazwę pliku." =
                !file.exists(plikLogu))
    cat("Komunikaty o ewentualnych problemach w danych zostaną zapisane do pliku '",
        plikLogu, "', ale nie zostaną wyświetlone na konsoli. Otwórz ten plik, aby je obejrzeć.\n\n",
        sep = "")
    plikLogu <- file(plikLogu, open = "wt")
    sink(plikLogu, type = "message")
  }
  if (is.list(baza)) {
    con <- do.call(dbConnect, args = baza)
  } else {
    con <- baza
  }
  on.exit({
    if (!inherits(baza, "DBIConnection")) dbDisconnect(con)
    sink(NULL, type = "message")
    })
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
  for (i in seq_along(tabeleWejsciowe)) {
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
  # kody szkół, dla których brak mapowania #####################################
  nieznaneKodySzkol <- full_join(tabeleWejsciowe$W2 %>%
                                   count(TYP_SZK, name = "W2"),
                                 tabeleWejsciowe$W3 %>%
                                   rename(TYP_SZK = TYP_SZK_KONT) %>%
                                   count(TYP_SZK, name = "W3"),
                                 by = "TYP_SZK") %>%
    anti_join(tabeleWejsciowe$STYPSZK,
              by = "TYP_SZK") %>%
    mutate(W2 = ifelse(is.na(W2), 0, W2),
           W3 = ifelse(is.na(W3), 0, W3)) %>%
    arrange(-W2, -W3)
  if (nrow(nieznaneKodySzkol) > 0) {
    stop("Wykryto kody szkół, dla których w pliku 'STYPSZK.csv' brak zdefiniowanego mapowania na nazwę typu szkoły:\n",
         paste(apply(nieznaneKodySzkol, 1,
                     function(x) {
                       paste0("- ", x["TYP_SZK"], ":",
                              ifelse(x["W2"] > 0,
                                     paste0(" ", x["W2"], " (W2)"), ""),
                              ifelse(x["W3"] > 0,
                                     paste0(" ", x["W3"], " (W3)"), ""),
                              ";") %>%
                         return()
                     }), collapse = "\n"),
         "\nUzupełnij mapowania w pliku 'STYPSZK.csv'.")
  }
  # identyfikowanie i wykluczanie absolwentów w W1, których brak w W2 ##########
  brakujacyW2 <- tabeleWejsciowe$W1 %>%
    anti_join(tabeleWejsciowe$W2,
              by = c("ID_ABS", "ROK_ABS"))
  if (nrow(brakujacyW2) > 0L) {
    message("Wykryto ", nrow(brakujacyW2),
            " absolwentów w pliku 'W1.csv', dla których brak jest danych w pliku 'W2.csv'. Zostaną oni wykluczeni z importu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych absolwentów zostanie zapisane w pliku 'absolwenci-w-W1-ale-nie-w-W2.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, którzy to absolwenci, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(brakujacyW2, "absolwenci-w-W1-ale-nie-w-W2.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
    tabeleWejsciowe$W1 <- tabeleWejsciowe$W1 %>%
      semi_join(tabeleWejsciowe$W2,
                by = c("ID_ABS", "ROK_ABS"))
  }
  # identyfikowanie i wykluczanie ewidentnie zduplikowanych absolwentów ########
  cat("\nSzukanie zduplikowanych ID absolwenta...")
  duplikatyIdW2 <- tabeleWejsciowe$W2 %>%
    left_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    group_by(ID_ABS) %>%
    mutate(n_rok_ur = n_distinct(ROK_UR),
           n_plec = n_distinct(PLEC)) %>%
    ungroup() %>%
    filter(n_rok_ur > 1L | n_plec > 1L)
  duplikatyId <- duplikatyIdW2 %>%
    select(ID_ABS, ROK_ABS) %>%
    distinct()
  cat(" zakończono.\n")
  if (nrow(duplikatyId) > 0L) {
    message("Wykryto ", nrow(duplikatyId), " ewidentnie zduplikowanych ID absolwenta (różne płcie lub lata urodzenia dla osób o tym samym ID).\nDane dotyczące tych ID zostaną wykluczone z importu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych ID absolwenta zostanie zapisane w pliku 'duplikaty-ID-absolwenta.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to ID absolwenta, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(duplikatyIdW2, "duplikaty-ID-absolwenta.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
    tabeleWejsciowe$W1 <- tabeleWejsciowe$W1 %>%
      anti_join(duplikatyId,
                by = c("ID_ABS", "ROK_ABS"))
    tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
      anti_join(duplikatyId,
                by = c("ID_ABS", "ROK_ABS"))
  }
  # sprawdzanie kompletności W25 (mapowanie TERYTów powiatów) ##################
  tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
    mutate(TERYT_POW_SZK = 100L*as.integer(floor(TERYT_SZK / 1000L)))
  brakujacePowiaty <- tabeleWejsciowe$W2 %>%
    pull(TERYT_POW_SZK) %>%
    unique() %>%
    setdiff(tabeleWejsciowe$W25$TERYT_POW)
  if (length(brakujacePowiaty) > 0L) {
    stop("\nWykryto następujące TERYTy powiatów (określone na podstawie wartości kolumny 'TERYT_SZK' w pliku 'W2.csv'), które nie występują w kolumnie 'TERYT_POW' w pliku 'W25.csv':\n",
            paste(brakujacePowiaty, collapse = ", "),
            ".\nUzupełnienij zawartość pliku 'W25.csv'.")
  }
  if (anyNA(tabeleWejsciowe$W25) || any(tabeleWejsciowe$W25 == '')) {
    stop("Dane w pliku 'W25.csv' nie mogą zawierać braków danych ani pustych nazw jednostek.")
  }
  # identyfikowanie i ew. usuwanie absolwentów w wielu zawodach w tej samej szkole w tym samym roku ####
  multiAbsolwenci <- tabeleWejsciowe$W2 %>%
    anti_join(select(duplikatyId, ID_ABS), by = "ID_ABS") %>%
    group_by(ID_SZK, TYP_SZK, ROK_ABS, ID_ABS) %>%
    summarise(n_zaw = n(), .groups = "drop_last")
  multiAbsolwenciSzkoly <- multiAbsolwenci %>%
    count(n_zaw) %>%
    mutate(max_n_zaw = max(n_zaw)) %>%
    ungroup() %>%
    mutate(n_zaw = factor(n_zaw, sort(unique(n_zaw)))) %>%
    pivot_wider(names_from = n_zaw, values_from = n, names_prefix = "n_zaw",
                names_sort = TRUE, values_fill = 0L) %>%
    filter(max_n_zaw > 1L)
  multiAbsolwenci <- multiAbsolwenci %>%
    ungroup() %>%
    filter(n_zaw > 1L, TYP_SZK != 19L)
  if (nrow(multiAbsolwenci) > 0L) {
    message("\nWykryto ", nrow(multiAbsolwenci), " osoby, które są absolwentami więcej niż jednego zawodu w tej samej szkole, niebędącej szkołą policealną.",
            ifelse(usunAbsWKilkuZaw, "\nZostaną one wykluczone z importu.", ""))
    if (usunAbsWKilkuZaw) {
      tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
        anti_join(multiAbsolwenci,
                  by = c("ID_ABS", "ROK_ABS", "ID_SZK"))
      tabeleWejsciowe$W1 <- tabeleWejsciowe$W1 %>%
        semi_join(tabeleWejsciowe$W2,
                  by = c("ID_ABS", "ROK_ABS"))
    }
  }
  # identyfikacja zawodów przypisanych do więcej niż jednej branży #############
  tabeleWejsciowe$W20 <- tabeleWejsciowe$W20 %>%
    bind_rows(tabeleWejsciowe$W20aneks %>%
                anti_join(tabeleWejsciowe$W20 %>%
                            select(KOD_ZAW, BRANZA) %>%
                            filter(grepl(" branża ", BRANZA)),
                          by = c("KOD_ZAW", "BRANZA"))) %>%
    mutate(BRANZA = sub(paste0("^(", paste(unique(BRANZA_KOD), collapse = "|"),
                               ") - "),
                        "", BRANZA),
           WERSJA_KLASYFIKACJI = if_else(BRANZA_KOD == "ndt.",
                                         3L, nchar(BRANZA_KOD))) %>%
    distinct() %>%
    filter(!is.na(BRANZA), BRANZA != '')
  zawodyPrzypisaneDoWieluBranz <- tabeleWejsciowe$W20 %>%
    add_count(KOD_ZAW, WERSJA_KLASYFIKACJI) %>%
    filter(n > 1L) %>%
    group_by(KOD_ZAW, WERSJA_KLASYFIKACJI) %>%
    summarise(BRANZE = paste(BRANZA, collapse = ", "),
              KODY_BRANZ = paste(BRANZA_KOD, collapse = ", "),
              .groups = "drop")
  if (nrow(zawodyPrzypisaneDoWieluBranz) > 0L) {
    zawodyPrzypisaneDoWieluBranz <-
      stop("W pliku 'W20.csv' (ew. w połączeniu z plikie 'W20aneks.csv') wykryto zawody przypisane do więcej niż jednej branży w ramach tej samej wersji klasyfikacji branżowej:\n",
           paste0("- ", zawodyPrzypisaneDoWieluBranz$KOD_ZAW, ": ",
                  zawodyPrzypisaneDoWieluBranz$BRANZE, " (",
                  zawodyPrzypisaneDoWieluBranz$KODY_BRANZ, ")",
                  collapse = ",\n"), ".")
  }
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
    # W25 (słownik powiatów)
    cat("\nZapis do bazy tabeli W25...")
    dbExecute(con,
              "INSERT INTO W25 (teryt_pow, powiat, wojewodztwo, nts, makroregion,
                                region, podregion)
               VALUES ($1, $2, $3, $4, $5, $6, $7)",
              params = tabeleWejsciowe$W25 %>%
                select(TERYT_POW, POWIAT, WOJEWODZTWO, NTS, MAKROREGION,
                       REGION, PODREGION) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
    cat("\nZapis do bazy tabeli W19 (wskaźniki rynku pracy w powiatach z BDL GUS)...")
    # W19 (wskaźniki z BDL)
    dbExecute(con,
              "INSERT INTO w19 (rok, miesiac, teryt_pow, stopa_bezrobocia,
                            sr_wynagrodzenia)
               VALUES ($1, $2, $3, $4, $5)",
              params = tabeleWejsciowe$W19 %>%
                select(ROK, MIESIAC, TERYT, STOPA_BEZROBOCIA, SR_WYNAGRODZENIA) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  ## W20a i W20 (słownik zawodów i przypisanie zawodów do branż) ###############
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
    mutate(KOD_ZAW = case_when(tolower(KOD_ZAW) %in% c("eksper.", "eksp01") ~ 10101L,
                               TRUE ~ suppressWarnings(as.integer(KOD_ZAW))))
  tabeleWejsciowe$W20a <- list(W20 = select(tabeleWejsciowe$W20, KOD_ZAW),
                               W2 = select(tabeleWejsciowe$W2, KOD_ZAW),
                               W3 = select(tabeleWejsciowe$W3, KOD_ZAW = KOD_ZAW_KONT),
                               W5 = select(tabeleWejsciowe$W5, KOD_ZAW = KOD_ZAW_KUZ),
                               W6 = select(tabeleWejsciowe$W6, KOD_ZAW = KOD_ZAW_CZEL),
                               W9 = select(tabeleWejsciowe$W9, KOD_ZAW),
                               W11 = select(tabeleWejsciowe$W11, KOD_ZAW),
                               W21 = select(tabeleWejsciowe$W21, KOD_ZAW))
  nieliczboweKodyZawodow <- tabeleWejsciowe$W20a %>%
    lapply(function(x) {return(class(x$KOD_ZAW))}) %>%
    setdiff("integer")
  komunikatONieliczbowychKodachZawodów <-
    vector(mode = "character", length = length(nieliczboweKodyZawodow))
  names(komunikatONieliczbowychKodachZawodów) <- names(nieliczboweKodyZawodow)
  for (i in names(nieliczboweKodyZawodow)) {
    tabeleWejsciowe[[i]] <- tabeleWejsciowe[[i]] %>%
      mutate(across(starts_with("KOD_ZAW"), list(ORYG = identity)),
             across(c(starts_with("KOD_ZAW"), -ends_with("_ORYG")),
                    ~suppressWarnings(as.integer(KOD_ZAW_ORYG))))
    komunikatONieliczbowychKodachZawodów[i] <-
      paste0("- W pliku '", i, ".csv':\n  '",
             paste(tabeleWejsciowe[[i]] %>%
                     filter(if_all(c(starts_with("KOD_ZAW"), -ends_with("_ORYG")),
                                   is.na),
                            if_all(matches("^KOD_ZAW.*_ORYG$"),
                                   Negate(is.na))) %>%
                     pull(KOD_ZAW_ORYG) %>%
                     unique(),
                   collapse = "', "), "';")
    tabeleWejsciowe[[i]] <- tabeleWejsciowe[[i]] %>%
      select(-matches("^KOD_ZAW.*_ORYG$"))
    tabeleWejsciowe$W20a[[i]] <- tabeleWejsciowe[[i]] %>%
      select(starts_with("KOD_ZAW"))
  }
  tabeleWejsciowe$W20a <- tabeleWejsciowe$W20a %>%
    bind_rows(.id = "tabela") %>%
    distinct() %>%
    filter(!is.na(KOD_ZAW))
  zawodyBezMapowania <- tabeleWejsciowe$W20a %>%
    anti_join(tabeleWejsciowe$W20, by = "KOD_ZAW") %>%
    filter(!(KOD_ZAW %in% c(100001L, 10101L:10103L))) %>%
    group_by(KOD_ZAW) %>%
    summarise(tabele = paste(tabela, collapse = ", "),
              .groups = "drop")
  tabeleWejsciowe$W20a <- tabeleWejsciowe$W20a %>%
    select(KOD_ZAW) %>%
    distinct()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabel W20 i W20a (kody zawodów i ich mapowanie na branże)...")
    dbExecute(con,
              "INSERT INTO w20a (kod_zaw) VALUES ($1)",
              params = list(tabeleWejsciowe$W20a$KOD_ZAW))
    dbExecute(con,
              "INSERT INTO w20 (kod_zaw, wersja_klasyfikacji, branza, branza_kod)
               VALUES ($1, $2, $3, $4)",
              params = tabeleWejsciowe$W20 %>%
                filter(!is.na(BRANZA)) %>%
                select(KOD_ZAW, WERSJA_KLASYFIKACJI, BRANZA, BRANZA_KOD) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  if (length(nieliczboweKodyZawodow) > 0) {
    message("\nWykryto następujące kody zawodów, niebędące liczbami całkowitymi:\n",
            paste(komunikatONieliczbowychKodachZawodów, collapse = "\n"))
  }
  if (nrow(zawodyBezMapowania) > 0L) {
    message("\nWykryto następujące kody zawodów, dla których brak jest mapowania na branże:\n",
            paste(zawodyBezMapowania %>%
                    mutate(KOD_ZAW = paste0(KOD_ZAW, " (", tabele, ")")) %>%
                    pull(KOD_ZAW),
                  collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W20aneks.csv'.")
    if (zapiszProblemy) {
      write.csv2(zawodyBezMapowania,
                 "kody-zawodów-bez-przypisania-do-branż.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  ## W2 (stałe w czasie charakterystyki absolwentów) ###########################
  tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
    left_join(tabeleWejsciowe$STYPSZK, by = "TYP_SZK") %>%
    select(-TYP_SZK) %>%
    rename(TYP_SZK = TYP_SZK_NAZWA) %>%
    group_by(ID_ABS, ROK_ABS, ID_SZK) %>%
    mutate(lp = seq_len(n())) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W2 (absolwenci poszczególnych szkół w poszczególnych zawodach)...")
    dbExecute(con,
              "INSERT INTO w2 (id_abs, rok_abs, rok_ur, plec, id_szk, typ_szk,
                               teryt_szk, teryt_pow_szk, lp, kod_zaw, nazwa_zaw)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
              params = tabeleWejsciowe$W2 %>%
                select(ID_ABS, ROK_ABS, ROK_UR, PLEC, ID_SZK, TYP_SZK, TERYT_SZK,
                       TERYT_POW_SZK, lp, KOD_ZAW, NAZWA_ZAW) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  if (nrow(multiAbsolwenciSzkoly) > 0L) {
    message("\nWykryto szkoły (włączając szkoły policealne), w których niektóre osoby są absolwentami więcej niż jednego zawodu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych szkół zostanie zapisane w pliku 'szkoły-z-multiabsolwentami.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(multiAbsolwenciSzkoly, "szkoły-z-multiabsolwentami.csv", row.names = FALSE,
                 fileEncoding = "UTF-8")
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
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    group_by(ID_ABS, ROK_ABS, ID_SZK_KONT, DATA_OD_SZK_KONT) %>%
    mutate(lp = seq_len(n())) %>%
    ungroup()
  ### zestawienie z kontynuacją w wielu zawodach w tej samej szkole ############
  multiAbsolwenciKont <- tabeleWejsciowe$W3 %>%
    group_by(ID_SZK_KONT, TYP_SZK_KONT, DATA_OD_SZK_KONT, ID_ABS) %>%
    summarise(n_zaw = n(), .groups = "drop_last") %>%
    count(n_zaw) %>%
    mutate(max_n_zaw = max(n_zaw)) %>%
    ungroup() %>%
    mutate(n_zaw = factor(n_zaw, sort(unique(n_zaw)))) %>%
    pivot_wider(names_from = n_zaw, values_from = n, names_prefix = "n_zaw",
                names_sort = TRUE, values_fill = 0L) %>%
    filter(max_n_zaw > 1L)
  # nauka w szkole, jako absolwent której ktoś został objety monitoringiem
  # pojawiająca się w danych o kontynuowaniu nauki
  # (spolic. z założenia wykluczane z zestawienia)
  kontynuacjaWsteczna <- inner_join(tabeleWejsciowe$W2 %>%
                                      select(ID_ABS, ROK_ABS, ID_SZK, TYP_SZK,
                                             KOD_ZAW) %>%
                                      distinct(),
                                    tabeleWejsciowe$W3 %>%
                                      select(ID_ABS, ROK_ABS, ID_SZK_KONT,
                                             DATA_OD_SZK_KONT, DATA_DO_SZK_KONT,
                                             KOD_ZAW_KONT) %>%
                                      distinct(),
                                    by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(dlNauki = case_when(TYP_SZK == "Branżowa szkoła I stopnia" ~ 3,
                               TYP_SZK == "Branżowa szkoła II stopnia" ~ 2,
                               TYP_SZK == "Technikum" & ROK_ABS < 2024 ~ 4,
                               TYP_SZK == "Liceum ogólnokształcące" & ROK_ABS < 2023 ~ 3,
                               TYP_SZK == "Technikum" ~ 5,
                               TYP_SZK == "Liceum ogólnokształcące" ~ 4,
                               TYP_SZK == "Szkoła specjalna przysposabiająca do pracy" ~ 3,
                               TYP_SZK == "Bednarska Szkoła Realna" & ROK_ABS < 2024 ~ 4,
                               TYP_SZK == "Bednarska Szkoła Realna" ~ 5)) %>%
    filter(ID_SZK_KONT == ID_SZK, KOD_ZAW_KONT %in% KOD_ZAW,
           TYP_SZK != "Szkoła policealna",
           year(DATA_OD_SZK_KONT) >= (ROK_ABS - dlNauki),
           DATA_DO_SZK_KONT <= as.Date(paste0(ROK_ABS, "-08-31"))) %>%
    select(-dlNauki)
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
  if (nrow(multiAbsolwenciKont) > 0L) {
    message("\nWśród szkół, w których absolwenci kontynuują naukę wykryto takie, w których te same osoby są absolwentami więcej niż jednego zawodu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych szkół zostanie zapisane w pliku 'szkoły-z-multiabsolwentami-kontynuacja-nauki.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(multiAbsolwenciKont,
                 "szkoły-z-multiabsolwentami-kontynuacja-nauki.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  if (nrow(kontynuacjaWsteczna) > 0L) {
    message("\nWśród danych o kontynuacji nauki zidentyfikowano rekordy, które wedle wszelkiego prawdopodobieństwa opisują naukę w szkole (i zawodzie), jako absolwent której dana osoba została objęta monitoringiem.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych rekordów zostanie zapisane w pliku 'nauka-w-szkole-ktorej-absolwentem-w-kontynuacji-nauki.csv'.\nUwaga, z założenia nie obejmuje ono szkół policealnych, może też nie obejmować uczniów, którzy powtarzali klasę i pewnie jeszcze jakichś innych mniej typowych przypadków.",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(kontynuacjaWsteczna,
                 "nauka-w-szkole-ktorej-absolwentem-w-kontynuacji-nauki.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
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
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_DO_KKZ = ifelse(DATA_DO_KKZ == "31-12-9999",
                                NA,
                                DATA_DO_KKZ)) %>%
    mutate(DATA_OD_KKZ = as.Date(DATA_OD_KKZ, "%d-%m-%Y"),
           DATA_DO_KKZ = as.Date(DATA_DO_KKZ, "%d-%m-%Y")) %>%
    group_by(ID_ABS, ROK_ABS, ID_SZK_KONT) %>%
    mutate(lp = seq_len(n())) %>%
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
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_DO_KUZ = ifelse(DATA_DO_KUZ == "31-12-9999",
                                NA,
                                DATA_DO_KUZ)) %>%
    mutate(DATA_OD_KUZ = as.Date(DATA_OD_KUZ, "%d-%m-%Y"),
           DATA_DO_KUZ = as.Date(DATA_DO_KUZ, "%d-%m-%Y")) %>%
    group_by(ID_ABS, ROK_ABS, ID_SZK_KONT) %>%
    mutate(lp = seq_len(n())) %>%
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
  tabeleWejsciowe$W6 <- tabeleWejsciowe$W6 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    count(ID_ABS, ROK_ABS, KOD_ZAW_CZEL)
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
  if (any(tabeleWejsciowe$W6$n > 1)) {
    message("\nWśród danych o dyplomach czeladnika (W6) znaleziono powtórzone rekordy.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych szkół zostanie zapisane w pliku 'wielokrotni-czeladnicy.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(tabeleWejsciowe$W6 %>%
                   filter(n > 1),
                 "wielokrotni-czeladnicy.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  ## W7 (matury) ###############################################################
  # uwaga! agreguję po absolwento-latach matury!
  tabeleWejsciowe$W7 <- tabeleWejsciowe$W7 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
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
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_KWAL = as.Date(DATA_KWAL, "%d-%m-%Y"))
  tabeleWejsciowe$W10 <- tabeleWejsciowe$W10 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_KWAL = as.Date(DATA_KWAL, "%d-%m-%Y"))
  multiKwal <- bind_rows(list(W8 = tabeleWejsciowe$W8,
                              W10 = tabeleWejsciowe$W10), .id = "table") %>%
    distinct() %>%
    group_by(ID_ABS, ROK_ABS, KOD_KWAL) %>%
    mutate(n = n(),
           rozne_tabele = n_distinct(table)) %>%
    ungroup() %>%
    filter(n > 1L)
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
  if (nrow(multiKwal) > 0L) {
    message("\nWykryto osoby, które kilkakrotnie zdawały egzaminy na tę samą kwalifikację.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych osób i kwalifikacji zostanie zapisane w pliku 'wielokrotne-kwalifikacje.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to osoby i kwalifikacje, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(multiKwal,
                 "wielokrotne-kwalifikacje.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  ## W9 i W11 (dyplomy zawodowe) ###############################################
  # uwaga! agreguję po absolwento-zawodach!
  # uwaga! DATA_DYP_ZAW może opisywać różne rzeczy w zależności od OKE (zapewne:
  # datę faktycznego zdania ostatniego wymaganego egzaminu, umowną datę zdania
  # ostatniego wymaganego egzaminu lub datę odbioru dyplomu)
  tabeleWejsciowe$W9 <- tabeleWejsciowe$W9 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_DYP_ZAW = as.Date(DATA_DYP_ZAW, "%d-%m-%Y"),
           ROK_DYP = as.numeric(format(DATA_DYP_ZAW, "%Y")))
  tabeleWejsciowe$W11 <- tabeleWejsciowe$W11 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_DYP_ZAW = as.Date(DATA_DYP_ZAW, "%d-%m-%Y"),
           ROK_DYP = as.numeric(format(DATA_DYP_ZAW, "%Y")))
  multiDypZaw <- bind_rows(list(W9 = tabeleWejsciowe$W9,
                                W11 = tabeleWejsciowe$W11), .id = "table") %>%
    distinct() %>%
    group_by(ID_ABS, ROK_ABS, KOD_ZAW) %>%
    mutate(n = n(),
           rozne_tabele = n_distinct(table)) %>%
    filter(n > 1L)
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W911 (dyplomy zawodowe)...")
    dbExecute(con,
              "INSERT INTO w911 (id_abs, rok_abs, kod_zaw, rok_dyp, dok_potw_dyp,
                                 data_dyp_zaw) VALUES ($1, $2, $3, $4, $5, $6)",
              params = bind_rows(tabeleWejsciowe$W9,
                                 tabeleWejsciowe$W11) %>%
                select(ID_ABS, ROK_ABS, KOD_ZAW, ROK_DYP, DOK_POTW_DYP,
                       DATA_DYP_ZAW) %>%
                filter(!is.na(KOD_ZAW)) %>%
                distinct() %>%
                arrange(ID_ABS, ROK_ABS, KOD_ZAW, ROK_DYP, DATA_DYP_ZAW) %>%
                group_by(ID_ABS, ROK_ABS, KOD_ZAW) %>%
                slice(1L) %>%
                ungroup() %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  if (any(is.na(tabeleWejsciowe$W9$KOD_ZAW)) ||
      any(is.na(tabeleWejsciowe$W11$KOD_ZAW))) {
    message("\nWykryto osoby, dla których w informacjach o dyplomie (W9 lub W11) występują braki danych (najbardziej prawdopodobna przyczyna to, że są to zawody eksperymentalne).\nRekordy te nie zostaną zapisane do bazy.")
  }
  if (nrow(multiDypZaw) > 0L) {
    message("\nWykryto osoby, które kilkakrotnie uzyskały dyplom w tym samym zawodzie.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych osób i zawodów zostanie zapisane w pliku 'wielokrotne-dyplomy.csv'.",
                   "\nJeżeli chcesz móc sprawdzić, które to osoby i zawody, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`."))
    if (zapiszProblemy) {
      write.csv2(multiDypZaw,
                 "wielokrotne-dyplomy.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  ## Tabela do diagnozowania kompletności danych z OKE #########################
  if (zapiszProblemy) {
    tabeleWejsciowe$W2 %>%
      filter(!is.na(KOD_ZAW)) %>%
      mutate(wojewodztwo = floor(TERYT_SZK / 100000)) %>%
      select(ID_ABS, ROK_ABS, TYP_SZK, wojewodztwo) %>%
      distinct() %>%
      left_join(tabeleWejsciowe$W16 %>%
                  semi_join(tabeleWejsciowe$W22 %>%
                              filter(MLODOC %in% 1) %>%
                              select(KOD_ZUS = KOD) %>%
                              mutate(KOD_ZUS = 100 * KOD_ZUS),
                            by = "KOD_ZUS") %>%
                  select(ID_ABS, ROK_ABS) %>%
                  distinct() %>%
                  mutate(mlodociany = TRUE),
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(TYP_SZK =
               case_when(
                 TYP_SZK == "Branżowa szkoła I stopnia" & mlodociany ~
                   "Branżowa szkoła I stopnia - młodociani",
                 TYP_SZK == "Branżowa szkoła I stopnia" ~
                   "Branżowa szkoła I stopnia - inni",
                 TRUE ~ TYP_SZK)) %>%
      left_join(bind_rows(tabeleWejsciowe$W8,
                          tabeleWejsciowe$W10) %>%
                  select(ID_ABS, ROK_ABS) %>%
                  distinct() %>%
                  mutate(swiadectwo = 1),
                by = c("ID_ABS", "ROK_ABS")) %>%
      left_join(bind_rows(tabeleWejsciowe$W9,
                          tabeleWejsciowe$W11) %>%
                  select(ID_ABS, ROK_ABS) %>%
                  distinct() %>%
                  mutate(dyplom = 1),
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(across(c(swiadectwo, dyplom), ~if_else(is.na(.), 0, .)),
             tylko_swiadectwo = swiadectwo == 1 & dyplom == 0) %>%
      group_by(wojewodztwo, TYP_SZK, ROK_ABS) %>%
      summarise(across(c(swiadectwo, dyplom, tylko_swiadectwo), mean),
                .groups = "drop") %>%
      select(-swiadectwo) %>%
      pivot_wider(names_from = TYP_SZK,
                  names_sep = ": ", names_vary = "slowest",
                  values_from = c(tylko_swiadectwo, dyplom)) %>%
      write.csv2("kompletnosc-danych-o-egzaminach.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
  }
  ## W24 (mapowanie kodów zawodów KZSB na ISCED-F) #############################
  if (anyNA(tabeleWejsciowe$W24) || any(tabeleWejsciowe$W24 == '')) {
    tabeleWejsciowe$W24 <- tabeleWejsciowe$W24 %>%
      filter(!is.na(KOD_ZAW),
             !(KOD_ISCED %in% c(NA_character_, '')),
             !(GRUPA_ISCED %in% c(NA_character_, '')),
             !(PODGRUPA_ISCED %in% c(NA_character_, '')),
             !(NAZWA_ISCED %in% c(NA_character_, '')))
    message("Z tabeli 'W24' zostały usunięte wiersze zawierające braki danych lub puste ciagi znaków w nazwach jednostek klasyfikacji ISCED-F.")
  }
  brakMapowaniaNaISCED <- tabeleWejsciowe$W20a$KOD_ZAW %>%
    setdiff(tabeleWejsciowe$W24$KOD_ZAW)
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W24 (mapowanie kodów zawodów na ISCED-F)...")
    dbExecute(con,
              "INSERT INTO w24 (kod_zaw, kod_isced, grupa_isced, podgrupa_isced,
                                nazwa_isced) VALUES ($1, $2, $3, $4, $5)",
              params = tabeleWejsciowe$W24 %>%
                semi_join(tabeleWejsciowe$W20a,
                          by = "KOD_ZAW") %>%
                select(KOD_ZAW, KOD_ISCED, GRUPA_ISCED, PODGRUPA_ISCED,
                       NAZWA_ISCED) %>%
                as.list() %>%
                unname())
    cat(" zakończony.")
  }
  if (length(brakMapowaniaNaISCED) > 0L) {
    message("\nWykryto następujące kody zawodów, dla których brak jest mapowania na klasyfikację ISCED-F:\n",
            paste(brakMapowaniaNaISCED, collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W24.csv'.")
    if (zapiszProblemy) {
      write.csv2(zawodyBezMapowania,
                 "kody-zawodów-bez-przypisania-do-ISCED-F.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
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
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
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
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
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
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_ADR_OD = as.Date(DATA_ADR_OD, "%d-%m-%Y"),
           DATA_ADR_DO = as.Date(DATA_ADR_DO, "%d-%m-%Y")) %>%
    arrange(ID_ABS, ROK_ABS, ADRES_TYP, DATA_ADR_OD, DATA_ADR_OD) %>%
    group_by(ID_ABS, ROK_ABS, ADRES_TYP, DATA_ADR_OD) %>%
    mutate(lp = seq_len(n())) %>%
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
    filter(!is.na(PKD)) %>%
    distinct()
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
                                bezrob_ela, student_ela, zagranic_ela,
                                prawnik_ela, samoz_ela, nspraw_ela, rolnik_ela,
                                rentemer_ela, mundur_ela, dziecko, etatnokid,
                                netatnokid_ela, samoznokid_ela, inne_ela, inne_ibe,
                                macierzynski_ela, dziecko_pracownik_ela,
                                dziecko_samozatrudnienie_ela, dziecko_zlecenie_ela,
                                dziecko_bezpracy_ela, wychowawczy_opieka, mlodoc,
                                benepomspol, bezrobotnystaz, bezrob_ibe,
                                pomoc_spol, macierz, wychow
              )
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                   $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
                   $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37)",
           params = tabeleWejsciowe$W22 %>%
             select(KOD, OPIS, ETAT_ELA, ETAT_IBE, SKLADKA_SZAC_WYNAG, NETAT_IBE,
                    BIERNY_SKLADKA, NETAT_ELA, ZLEC_ELA, BEZROB_ELA,
                    STUDENT_ELA, ZAGRANIC_ELA, PRAWNIK_ELA, SAMOZ_ELA, NSPRAW_ELA,
                    ROLNIK_ELA, RENTEMER_ELA, MUNDUR_ELA, DZIECKO, ETATNOKID,
                    NETATNOKID_ELA, SAMOZNOKID_ELA, INNE_ELA, INNE_IBE,
                    MACIERZYNSKI_ELA, DZIECKO_PRACOWNIK_ELA,
                    DZIECKO_SAMOZATRUDNIENIE_ELA, DZIECKO_ZLECENIE_ELA,
                    DZIECKO_BEZPRACY_ELA, WYCHOWAWCZY_OPIEKA, MLODOC,
                    BENEPOMSPOL, BEZROBOTNYSTAZ, BEZROB_IBE, POMOC_SPOL,
                    MACIERZ, WYCHOW) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W16 (składki) #############################################################
  # uwaga! agreguję po absolwento-okreso-płatniko-(skróconych)kodach!
  tabeleWejsciowe$W16 <- tabeleWejsciowe$W16 %>%
    filter(!is.na(KOD_ZUS)) %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(EMERYT_RENCISTA = floor(KOD_ZUS / 10) - 10 * floor(KOD_ZUS / 100),
           NIEPELNOSPRAWNY = KOD_ZUS - 10 * floor(KOD_ZUS / 10),
           KOD_ZUS = floor(KOD_ZUS / 100),
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
      summarise(across(c(CZY_30, CZY_RSA), ~any(., na.rm = TRUE)),
                across(c(EMERYT_RENCISTA, NIEPELNOSPRAWNY), max),
                across(starts_with("PODST_"), ~sum(., na.rm = TRUE)),
                .groups = "drop"))
  kodyZusBezMapowania <- tabeleWejsciowe$W16 %>%
    select(KOD_ZUS) %>%
    anti_join(tabeleWejsciowe$W22 %>%
                rename(KOD_ZUS = KOD),
              by = "KOD_ZUS")
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W16 (miesięczne dane o składkach ZUS)...")
    dbExecute(con,
              "INSERT INTO w16 (id_abs, rok_abs, rok_skladka, mies_skladka,
                                id_platnika, kod_zus, podst_chor, podst_wypad,
                                podst_emer, podst_zdrow, czy_30, czy_rsa,
                                emeryt_rencista, niepelnosprawny)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
           params = tabeleWejsciowe$W16 %>%
             select(ID_ABS, ROK_ABS, ROK_SKLADKA, MIES_SKLADKA, ID_PLATNIKA,
                    KOD_ZUS, PODST_CHOR, PODST_WYPAD, PODST_EMER, PODST_ZDROW,
                    CZY_30, CZY_RSA, EMERYT_RENCISTA, NIEPELNOSPRAWNY) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  if (nrow(kodyZusBezMapowania) > 0L) {
    message("\nWykryto następujące kody składek ZUS, dla których brak jest mapowania na użyteczne analitycznie kategorie:\n",
            paste(unique(kodyZusBezMapowania$KOD_ZUS), collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W22.csv'.")
    if (zapiszProblemy) {
      write.csv2(zawodyBezMapowania, "kody-ZUS-niewystępujące-w-W22.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  ## W23 (mapowanie kodów przerw w opłacaniu składek ZUS) ######################
  tabeleWejsciowe$W23 <- tabeleWejsciowe$W23 %>%
    mutate(across(-c(KOD, OPIS), as.logical))
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W23 (mapowanie kodów przerw w opłacaniu składek ZUS na kategorie użyteczne analitycznie)...")
    dbExecute(con,
              "INSERT INTO w23 (kod, opis, bierny_zawodowo, dziecko2, wypadek,
                                choroba, choroba_macierz)
               VALUES ($1, $2, $3, $4, $5, $6, $7)",
           params = tabeleWejsciowe$W23 %>%
             select(KOD, OPIS, BIERNY_ZAWODOWO, DZIECKO2, WYPADEK, CHOROBA,
                    CHOROBA_MACIERZ) %>%
             as.list() %>%
             unname())
    cat(" zakończony.")
  }
  ## W17 (przerwy w opłacaniu składek) #########################################
  tabeleWejsciowe$W17 <- tabeleWejsciowe$W17 %>%
    filter(!is.na(KOD_PRZERWY)) %>%
    filter(!(is.na(DATA_OD_PRZERWA) | is.na(DATA_DO_PRZERWA))) %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_OD_PRZERWA = as.Date(DATA_OD_PRZERWA, "%d-%m-%Y"),
           DATA_DO_PRZERWA = as.Date(DATA_DO_PRZERWA, "%d-%m-%Y")) %>%
    distinct()
  kodyPrzerwBezMapowania <- tabeleWejsciowe$W17 %>%
    select(KOD_PRZERWY) %>%
    anti_join(tabeleWejsciowe$W23 %>%
                rename(KOD_PRZERWY = KOD),
              by = "KOD_PRZERWY")
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
  if (nrow(kodyPrzerwBezMapowania) > 0L) {
    message("\nWykryto następujące kody przerw w opłacaniu składek ZUS, dla których brak jest mapowania na użyteczne analitycznie kategorie:\n",
            paste(unique(kodyPrzerwBezMapowania$KOD_PRZERWY), collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W23.csv'.")
    if (zapiszProblemy) {
      write.csv2(zawodyPrzerwMapowania, "kody-przerw-ZUS-niewystępujące-w-W23.csv",
                 row.names = FALSE, fileEncoding = "UTF-8")
    }
  }
  dbExecute(con, "COMMIT;")
  cat("\nKoniec: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")
  invisible(NULL)
}
