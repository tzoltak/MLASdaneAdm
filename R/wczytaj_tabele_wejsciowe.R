#' @title Wczytywanie danych do bazy z plikow wejsciowych
#' @description Funkcja szuka w podanym folderze plików CSV z danymi
#' z tabel *wejściowych*, wczytuje je, kontroluje ich spójność i zapisuje
#' do bazy.
#' @param baza uchwyt połączenia do bazy lub lista argumentów do funkcji
#' [DBI::dbConnect()] umożliwiających nawiązanie połączenia z bazą
#' danych, w której mają zostać zapisane wczytywane dane lub `NULL`, jeśli
#' argument `wczytajDoBazy` podano jako `FALSE`
#' @param folder opcjonalnie ścieżka do folderu zawierającego pliki CSV z danymi
#' (domyślnie używany jest aktywny katalog roboczy)
#' @param wczytajDoBazy opcjonalnie wartość logiczna - czy przeprowadzić zapis
#' do bazy? (jeśli `FALSE`, funkcja tylko wczyta pliki CSV i zaraportuje
#' wykryte w nich problemy)
#' @param zapiszProblemy opcjonalnie wartość logiczna - czy funkcja ma zapisywać
#' zestawienia wykrytych we wczytanych plikach problemów w formie plików CSV?
#' @param usunAbsWKilkuZaw czy absolwenci, którzy ukończyli kilka różnych
#' zawodów w tej samej szkole, **niebędącej szkołą policealną** powinni
#' zostać wykluczeni z importu? (W edycjach monitoringu **2021 i 2022**
#' tacy absolwenci **nie** byli usuwani. Od edycji **2023** są jednak
#' usuwani.)
#' @param plikLogu opcjonalnie nazwa pliku, do którego ma być zapisany log
#' wywołania funkcji
#' @param nieWczytujDanychZUS opcjonalnie wartość logiczna pozwalająca pominąć
#' wczytywanie do bazy danych z ZUS (i tym samym wydatnie skrócić proces) -
#' przydatna, jeśli proces ma na celu tylko wprowadzenie jakichś poprawek do
#' już wcześniej przygotowanych tabel *pośrednich* w zakresie danych innych,
#' niż te z ZUS
#' @inheritParams tabele_wejsciowe wersja
#' @param rokMonitoringu rok, w którym prowadzony jest monitoring - domyślna
#' wartość, tj. brak danych, wskazuje, że ma on zostać zgadnięty z danych (jako
#' maksimum z `ROK_ABS` w *W1.csv* powiększone o jeden)
#' @details Dane wejściowe powinny obejmować **tylko jeden rok prowadzenia
#' monitoringu**, ale mogą obejmować **kilka różnych okresów od ukończenia
#' szkoły**.
#' @return `NULL`
#' @seealso [tabele_wejsciowe()]
#' @importFrom stats setNames
#' @importFrom utils write.csv2
#' @importFrom DBI dbConnect dbExecute dbDisconnect dbIsValid
#' @importFrom dplyr %>% .data across add_count anti_join any_of arrange bind_rows contains case_when count desc distinct ends_with filter group_by if_all if_else join_by left_join matches mutate n n_distinct pick pull reframe rename select semi_join slice summarise ungroup
#' @importFrom tidyr pivot_wider
#' @importFrom lubridate year
#' @export
wczytaj_tabele_wejsciowe = function(baza, folder = ".", wczytajDoBazy = TRUE,
                                    zapiszProblemy = TRUE,
                                    usunAbsWKilkuZaw = TRUE, plikLogu = NULL,
                                    nieWczytujDanychZUS = FALSE,
                                    wersja = c("2021", "2026"),
                                    rokMonitoringu = NA_integer_) {
  stopifnot(length(wczytajDoBazy) == 1L, wczytajDoBazy %in% c(FALSE, TRUE),
            is.list(baza) | inherits(baza, "DBIConnection") | wczytajDoBazy == FALSE,
            dir.exists(folder),
            length(zapiszProblemy) == 1L, zapiszProblemy %in% c(FALSE, TRUE),
            length(usunAbsWKilkuZaw) == 1L, usunAbsWKilkuZaw %in% c(FALSE, TRUE),
            length(nieWczytujDanychZUS) == 1L,
            nieWczytujDanychZUS %in% c(FALSE, TRUE))
  wersja <- match.arg(wersja, several.ok = FALSE)
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
  if (is.list(baza) & wczytajDoBazy) {
    con <- do.call(dbConnect, args = baza)
  } else if (wczytajDoBazy) {
    stopifnot(dbIsValid(baza))
    con <- baza
  }
  on.exit({
    if (!inherits(baza, "DBIConnection") & wczytajDoBazy) dbDisconnect(con)
    sink(type = "message")
    })
  if (wczytajDoBazy) dbExecute(con, "BEGIN;")
  tabeleWejsciowe <- names(tabele_wejsciowe(wersja = wersja))
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
  wystepujacaRoczniki <- brakujaceKolumny
  brakiIdAbs <- setNames(rep(NA_integer_, length(tabeleWejsciowe)),
                         names(tabeleWejsciowe))
  for (i in seq_along(tabeleWejsciowe)) {
    cat("Wczytywanie pliku", tabeleWejsciowe[[i]], "\n")
    tabeleWejsciowe[[i]] <- arrow::read_delim_arrow(tabeleWejsciowe[[i]],
                                                    delim = ";")
    brakujaceKolumny[[i]] <-
      setdiff(tabele_wejsciowe(wersja = wersja)[[names(tabeleWejsciowe)[i]]],
              names(tabeleWejsciowe[[i]]))
    if (length(brakujaceKolumny[[i]]) > 0L) {
      cat("  Brakuje wymaganych kolumn: '",
          paste0(brakujaceKolumny[[i]], collapse = "', '"), "'\n", sep = "")
    }
    if ("ID_ABS" %in% names(tabeleWejsciowe[[i]])) {
      brakiIdAbs[i] <- sum(tabeleWejsciowe[[i]] %>%
                             select(any_of(c("ID_ABS", "ROK_ABS"))) %>%
                             is.na() %>%
                             rowSums() > 0L)
      if (brakiIdAbs[i] > 0L) {
        cat(" W kolumnnie ID_ABS lub ROK_ABS występują braki danych w ",
            brakiIdAbs[i], " rekordach.\n", sep = "")
      }
    }
    if ("ROK_ABS" %in% names(tabeleWejsciowe[[i]])) {
      wystepujacaRoczniki[[i]] <- unique(tabeleWejsciowe[[i]]$ROK_ABS)
    }
  }
  if (sum(sapply(brakujaceKolumny, length)) > 0L ||
      sum(brakiIdAbs, na.rm = TRUE) > 0L) {
    stop("Brak wymaganych kolumn lub braki danych w kolumnach ROK_ABS lub ID_ABS w co najmniej jednym z wczytywanych plików (p. informacje powyżej).")
  }
  if (is.na(rokMonitoringu)) {
    rokMonitoringu <- max(tabeleWejsciowe$W1$ROK_ABS) + 1L
  }
  stopifnot(is.numeric(rokMonitoringu), length(rokMonitoringu) == 1L,
            as.integer(rokMonitoringu) == rokMonitoringu)
  if (rokMonitoringu != max(tabeleWejsciowe$W1$ROK_ABS) + 1L) {
    warning("Wartość argumentu `rokMonitoringu` nie jest równa `max(tabeleWejsciowe$W1$ROK_ABS) + 1`. Czy na pewno wskazuje ona rok prowadzenia monitoringu?", immediate. = TRUE)
  }
  message("Na podstawie danych rok prowadzenia monitoringu określono na: ",
          rokMonitoringu, "\n")
  wszystkieRoczniki <- sort(unique(unlist(wystepujacaRoczniki)))
  message("W plikach z tabelami wejściowymi wykryto następujące wartości zmiennej `ROK_ABS`: ",
          paste(wszystkieRoczniki, collapse = ", "), "\n")
  brakujaceRoczniki <-
    wystepujacaRoczniki[sapply(wystepujacaRoczniki, length) > 0L] %>%
    lapply(function(x, wszystkieRoczniki) {return(setdiff(wszystkieRoczniki, x))},
           wszystkieRoczniki = wszystkieRoczniki)
  brakujaceRoczniki <- brakujaceRoczniki[sapply(brakujaceRoczniki, length) > 0L]
  if (length(brakujaceRoczniki) > 0L) {
    brakujaceRoczniki <- sapply(brakujaceRoczniki, paste, collapse = ", ")
    warning("W niektórych z wczytanych plików nie wystąpiły niektóre roczniki absolwentów:\n",
            paste0("- ", names(brakujaceRoczniki), ": ", brakujaceRoczniki,
                   collapse = ";\n"), ".\n",
            call. = FALSE, immediate. = TRUE)
  }
  cat("\nPoczątek przetwarzania danych: ",
      format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")
  # duplikaty kombinacji wartości ID_ABS, ROK_ABS w W1 #########################
  tabeleZAbs <- names(tabele_wejsciowe(wersja = wersja))[
    sapply(tabele_wejsciowe(wersja = wersja),
           function(x) return(all(c("ID_ABS", "ROK_ABS") %in% x)))]
  duplikatyIdW1 <- tabeleWejsciowe$W1 %>%
    count(.data$ID_ABS, .data$ROK_ABS, name = "n") %>%
    filter(.data$n > 1L)
  if (nrow(duplikatyIdW1) > 0L) {
    write.csv2(duplikatyIdW1, "duplikaty-ID-absolwenta-W1.csv",
               row.names = FALSE, na = "", fileEncoding = "UTF-8")
    stop("Wykryto ", nrow(duplikatyIdW1), " kombinacji wartości (ID_ABS, ROK_ABS) występujących wielokrotnie w pliku 'W1.csv'.",
         "\nZestawienie tych kombinacji zostanie zapisane w pliku 'duplikaty-ID-absolwenta-W1.csv'.\n")
  }
  # mapowanie kraju pochodzenia na region ######################################
  if ("W29" %in% names(tabeleWejsciowe)) {
    tabeleWejsciowe$W1$KRAJ_POCHODZENIA <-
      ifelse(is.na(tabeleWejsciowe$W1$KRAJ_POCHODZENIA),
             '',
             tabeleWejsciowe$W1$KRAJ_POCHODZENIA)
    tabeleWejsciowe$W29$KRAJ_POCHODZENIA <-
      ifelse(is.na(tabeleWejsciowe$W29$KRAJ_POCHODZENIA),
             '',
             tabeleWejsciowe$W29$KRAJ_POCHODZENIA)
    stopifnot(all(!duplicated(tabeleWejsciowe$W29$KRAJ_POCHODZENIA)),
              !anyNA(tabeleWejsciowe$W29$REGION_POCHODZENIA),
              all(tabeleWejsciowe$W29$REGION_POCHODZENIA != ""))
    brakujaceKraje <- setdiff(tabeleWejsciowe$W1$KRAJ_POCHODZENIA,
                              tabeleWejsciowe$W29$KRAJ_POCHODZENIA)
    if (length(brakujaceKraje) > 0L) {
      stop("W pliku wejściowym 'W29.csv' brakuje mapowania następujących krajów:\n",
           paste(brakujaceKraje, collapse = ", "), ".")
    }
  }
  # kody szkół, dla których brak mapowania #####################################
  nieznaneKodySzkol <- full_join(tabeleWejsciowe$W2 %>%
                                   count(.data$TYP_SZK, name = "W2"),
                                 tabeleWejsciowe$W3 %>%
                                   rename(TYP_SZK = "TYP_SZK_KONT") %>%
                                   count(.data$TYP_SZK, name = "W3"),
                                 by = "TYP_SZK") %>%
    anti_join(tabeleWejsciowe$STYPSZK,
              by = "TYP_SZK") %>%
    mutate(W2 = ifelse(is.na(.data$W2), 0, .data$W2),
           W3 = ifelse(is.na(.data$W3), 0, .data$W3)) %>%
    arrange(desc(.data$W2), desc(.data$W3))
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
  # sprawdzanie, czy wszystkie kombinacje (ID_ABS, ROK_ABS) występują w W1 #####
  absNieWW1 <- sapply(tabeleWejsciowe[setdiff(tabeleZAbs, "W1")],
                      function(x, w1) {
                        anti_join(x, w1, by = c("ID_ABS", "ROK_ABS")) %>%
                          select("ID_ABS", "ROK_ABS") %>%
                          distinct() %>%
                          arrange(.data$ID_ABS, .data$ROK_ABS) %>%
                          mutate(etykieta = paste0("(", .data$ID_ABS, ", ", .data$ROK_ABS, ")")) %>%
                          pull("etykieta") %>%
                          paste(collapse = ", ") %>%
                          return()
                      }, w1 = tabeleWejsciowe$W1)
  absNieWW1 <- absNieWW1[absNieWW1 != ""]
  if (length(absNieWW1) > 0L) {
    stop("W niektórych tabelach wejściowych istnieją kombinacje wartości kolumn (ID_ABS, ROK_ABS), które nie występują w pliku 'W1.csv':\n",
         paste0(names(absNieWW1), ": ", absNieWW1, collapse = "\n"))
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
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
    tabeleWejsciowe$W1 <- tabeleWejsciowe$W1 %>%
      semi_join(tabeleWejsciowe$W2,
                by = c("ID_ABS", "ROK_ABS"))
  }
  # identyfikowanie zmarłych wg ZUS, dla których mamy w danych coś po ich śmierci ####
  tabeleWejsciowe$W14 <- tabeleWejsciowe$W14 %>%
    filter(!is.na(.data$ROK_ZGONU))
  sladyPoZgonie <-
    bind_rows(
      list(W1 = tabeleWejsciowe$W1 %>%
             mutate(data = as.Date(paste0(.data$ROK_ABS, "-06-30"))) %>%
             select("ID_ABS", "data"),
           W3 = tabeleWejsciowe$W3 %>%
             mutate(data = as.Date(.data$DATA_OD_SZK_KONT,
                                   tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
             select("ID_ABS", "data"),
           W4 = tabeleWejsciowe$W4 %>%
             mutate(data = as.Date(.data$DATA_OD_KKZ,
                                   tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
             select("ID_ABS", "data"),
           W5 = tabeleWejsciowe$W5 %>%
             mutate(data = as.Date(.data$DATA_OD_KUZ,
                                   tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
             select("ID_ABS", "data"),
           W12 = tabeleWejsciowe$W12 %>%
             mutate(data = as.Date(.data$DATA_OD_STU,
                                   tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
             select("ID_ABS", "data"),
           W32 = tabeleWejsciowe$W32 %>%
             filter(!is.na(.data$DATA_OD_UBEZP)) %>%
             mutate(data = as.Date(.data$DATA_OD_UBEZP,
                                   tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
             select("ID_ABS", "data"),
           W33 = tabeleWejsciowe$W33 %>%
             filter(!is.na(.data$DATA_OD_SWIADCZ)) %>%
             mutate(data = as.Date(.data$DATA_OD_SWIADCZ,
                                   tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
             select("ID_ABS", "data")
      ), .id = "zrodlo"
    ) %>%
    inner_join(tabeleWejsciowe$W14 %>%
                 mutate(data_zgonu = as.Date(paste0(
                   .data$ROK_ZGONU, "-", .data$MIES_ZGONU, "-01"))) %>%
                 select("ID_ABS", "data_zgonu"),
               by = join_by(x$ID_ABS == y$ID_ABS, x$data > y$data_zgonu)) %>%
    mutate(roznica = difftime(.data$data, .data$data_zgonu, units = "days")) %>%
    filter(.data$roznica > 180)
  if (nrow(sladyPoZgonie) > 0L) {
    message("Wykryto ", nrow(sladyPoZgonie),
            " absolwent(a/ów), dla któr(ego/ych) w plikach 'W1.csv', 'W3.csv', 'W4.csv', 'W5.csv', 'W12.csv', 'W32.csv' lub 'W33.csv' występują rekordy wskazujące na podjęcie nauki (ew. odprowadzanie składek lub pobieranie świadczeń) w momencie, który nastąpił później niż 180 dni od daty zgonu danej osoby opisanej w pliku 'W14.csv'.",
            "\nZostan(ie/ą) on(i) wyklucz(ony/eni) z importu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych absolwentów zostanie zapisane w pliku 'slady-po-zgonie.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, którzy to absolwenci, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(sladyPoZgonie, "slady-po-zgonie.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
    tabeleWejsciowe[tabeleZAbs] <- lapply(tabeleWejsciowe[tabeleZAbs],
                                          anti_join, y = sladyPoZgonie,
                                          by = "ID_ABS")
  }
  # identyfikowanie i wykluczanie ewidentnie zduplikowanych absolwentów ########
  cat("\nSzukanie zduplikowanych ID absolwenta...")
  duplikatyIdW2 <- tabeleWejsciowe$W2 %>%
    left_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    group_by(.data$ID_ABS) %>%
    mutate(across(any_of(c("ROK_UR", "PLEC", "KRAJ_POCHODZENIA")),
                  list(n = n_distinct))) %>%
    ungroup() %>%
    filter(rowSums(pick(any_of(c("ROK_UR_n", "PLEC_n", "KRAJ_POCHODZENIA_n"))) > 1L) > 0L)
  duplikatyId <- duplikatyIdW2 %>%
    select("ID_ABS") %>%
    distinct()
  cat(" zakończono ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".\n")
  if (nrow(duplikatyId) > 0L) {
    message("Wykryto ", nrow(duplikatyId), " ewidentnie zduplikowany(ch) ID absolwenta (różne płcie lub lata urodzenia lub, od edycji 2026, kraje pochodzenia, dla osób o tym samym ID).\nDane dotyczące tego/tych ID zostaną wykluczone z importu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych ID absolwenta zostanie zapisane w pliku 'duplikaty-ID-absolwenta.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, które to ID absolwenta, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(duplikatyIdW2, "duplikaty-ID-absolwenta.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
    tabeleWejsciowe[tabeleZAbs] <- lapply(tabeleWejsciowe[tabeleZAbs],
                                          anti_join, y = duplikatyId,
                                          by = "ID_ABS")
  }
  # sprawdzanie kompletności W25 (mapowanie TERYTów powiatów) ##################
  tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
    mutate(TERYT_POW_SZK = 100L*as.integer(floor(.data$TERYT_SZK / 1000L)))
  brakujacePowiaty <- tabeleWejsciowe$W2 %>%
    pull("TERYT_POW_SZK") %>%
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
  # sprawdzanie kompletności W26 (dane szkół z RSPO) ###########################
  brakiW26 <- tabeleWejsciowe$W26 %>%
    select(-c("SYM_UL", "ULICA", "NR_BUDYNKU", "NR_LOKALU",
              "ORGAN_REJESTRUJACY_TERYT", "ORGAN_PROWADZACY_REGON",
              "JEDN_NADRZ_ID", "JEDN_NADRZ_TYP")) %>%
    sapply(function(x) return(anyNA(x) | any(x == '')))
  if (any(brakiW26)) {
    stop("W pliku 'W26.csv' kolumny '",
         paste(names(brakiW26)[brakiW26], collapse = "', '"),
         "' zawierają braki danych (lub ew. puste ciągi znaków), choć jest to niedozwolone.")
  }
  duplikatyW26b <- tabeleWejsciowe$W26 %>%
    select("ID_SZK", "TYP_SZK", "PUBLICZNOSC", "KATEGORIA_UCZNIOW", "SPECYFIKA",
           "ORGAN_REJESTRUJACY_ID", "ORGAN_REJESTRUJACY_TYP",
           "ORGAN_REJESTRUJACY_NAZWA", "ORGAN_REJESTRUJACY_TERYT",
           "ORGAN_SPOSOB_EWIDENCJONOWANIA", "ORGAN_REJESTRUJACY_TYP") %>%
    distinct() %>%
    add_count(.data$ID_SZK) %>%
    filter(.data$n > 1L)
  if (nrow(duplikatyW26b) > 0L) {
    message("W pliku 'W26.csv' występują szkoły, dla których wartości co najmniej jednej spośród kolumn: ",
            "'TYP_SZK', 'PUBLICZNOSC', 'KATEGORIA_UCZNIOW', 'SPECYFIKA', 'ORGAN_PROWADZACY_ID', 'ORGAN_PROWADZACY_SPOSOB', 'ORGAN_PROWADZACY_TYP' ",
            "różnią się pomiędzy niektórymi latami szkolnymi.\nPamiętaj, że w ogólności również te cechy szkoły mogą zmieniać się w czasie!\n")
  }
  szkoloLataSzkWDanych <- zidentyfikuj_szkolo_lata_w_danych(tabeleWejsciowe$W2,
                                                            tabeleWejsciowe$W3,
                                                            tabeleWejsciowe$W4,
                                                            tabeleWejsciowe$W5,
                                                            rokMonitoringu)
  brakujaceDaneSzkol <- anti_join(szkoloLataSzkWDanych, tabeleWejsciowe$W26,
                                  by = c("ID_SZK", "ROK_SZK"))
  tabeleWejsciowe$W26 <- semi_join(tabeleWejsciowe$W26, szkoloLataSzkWDanych,
                                   by = c("ID_SZK", "ROK_SZK"))
  if (nrow(brakujaceDaneSzkol) > 0L) {
    write.csv2(brakujaceDaneSzkol, "brakujace-dane-szkol.csv",
               row.names = FALSE, na = "", fileEncoding = "UTF-8")
    stop("Dla niektórych szkół w plikach 'W2.csv', 'W3.csv', 'W4.csv' lub 'W5.csv' nie znaleziono danych w pliku 'W26.csv'.\n",
         "Zestawienie tych szkoło-lat szkolnych zostało zapisane w pliku 'brakujace-dane-szkol.csv'.")
  }
  # identyfikowanie i ew. usuwanie absolwentów w wielu zawodach w tej samej szkole w tym samym roku ####
  cat("\nSzukanie absolwentów w wielu zawodach w tej samej szkole...")
  multiAbsolwenci <- tabeleWejsciowe$W2 %>%
    anti_join(select(duplikatyId, "ID_ABS"), by = "ID_ABS") %>%
    group_by(.data$ID_SZK, .data$TYP_SZK, .data$ROK_ABS, .data$ID_ABS) %>%
    summarise(n_zaw = n(), .groups = "drop_last")
  multiAbsolwenciSzkoly <- multiAbsolwenci %>%
    count(.data$n_zaw) %>%
    mutate(max_n_zaw = max(.data$n_zaw)) %>%
    ungroup() %>%
    mutate(n_zaw = factor(.data$n_zaw, sort(unique(.data$n_zaw)))) %>%
    pivot_wider(names_from = "n_zaw", values_from = "n",
                names_prefix = "n_zaw", names_sort = TRUE, values_fill = 0L) %>%
    filter(.data$max_n_zaw > 1L) %>%
    mutate(czy_usunieci = usunAbsWKilkuZaw | .data$TYP_SZK == 19L)
  multiAbsolwenci <- multiAbsolwenci %>%
    ungroup() %>%
    filter(.data$n_zaw > 1L, .data$TYP_SZK != 19L)
  cat(" zakończono ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".\n")
  if (nrow(multiAbsolwenci) > 0L) {
    message("Wykryto ", nrow(multiAbsolwenci), " osoby, które są absolwentami więcej niż jednego zawodu w tej samej szkole, niebędącej szkołą policealną.",
            ifelse(usunAbsWKilkuZaw, "\nZostaną one wykluczone z importu.\n", "\n"))
    if (usunAbsWKilkuZaw) {
      tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
        anti_join(multiAbsolwenci,
                  by = c("ID_ABS", "ROK_ABS", "ID_SZK"))
      tabeleWejsciowe$W1 <- tabeleWejsciowe$W1 %>%
        semi_join(tabeleWejsciowe$W2,
                  by = c("ID_ABS", "ROK_ABS"))
    }
  }
  if (nrow(multiAbsolwenciSzkoly) > 0L) {
    message("W danych wystąpiły szkoły (włączając szkoły policealne), w których niektóre osoby są absolwentami więcej niż jednego zawodu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych szkół zostanie zapisane w pliku 'szkoły-z-multiabsolwentami.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(multiAbsolwenciSzkoly, "szkoły-z-multiabsolwentami.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  # identyfikacja zawodów przypisanych do więcej niż jednej branży #############
  tabeleWejsciowe$W20 <- tabeleWejsciowe$W20 %>%
    bind_rows(tabeleWejsciowe$W20aneks %>%
                anti_join(tabeleWejsciowe$W20 %>%
                            select("KOD_ZAW", "BRANZA") %>%
                            filter(grepl(" branża ", .data$BRANZA)),
                          by = c("KOD_ZAW", "BRANZA"))) %>%
    mutate(BRANZA = sub(paste0("^(",
                               paste(unique(.data$BRANZA_KOD), collapse = "|"),
                               ") - "),
                        "", .data$BRANZA),
           WERSJA_KLASYFIKACJI = case_when(
             .data$BRANZA_KOD == "ndt." ~ 3L,
             .data$BRANZA %in% c("branża chemiczna i ochrony środowiska",
                                 "branża poligraficzno-księgarska") ~ 4L,
             .default = nchar(.data$BRANZA_KOD))) %>%
    distinct() %>%
    filter(!is.na(.data$BRANZA), .data$BRANZA != '')
  zawodyPrzypisaneDoWieluBranz <- tabeleWejsciowe$W20 %>%
    add_count(.data$KOD_ZAW, .data$WERSJA_KLASYFIKACJI) %>%
    filter(.data$n > 1L) %>%
    group_by(.data$KOD_ZAW, .data$WERSJA_KLASYFIKACJI) %>%
    summarise(BRANZE = paste(.data$BRANZA, collapse = ", "),
              KODY_BRANZ = paste(.data$BRANZA_KOD, collapse = ", "),
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
    if ("W29" %in% names(tabeleWejsciowe)) {
      cat("\nZapis do bazy tabeli W29 (mapowanie krajów pochodzenia na regiony)...")
      # W29 (mapowanie krajów pochodzenia)
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W29, "w29", wersja)))
      cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    }
    cat("\nZapis do bazy tabeli W1 (unikalni absolwenci - bez względu na szkołę i zawód)...")
    # W1 (unikalni absolwenci)
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W1, "w1", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    # Słownik typów szkół
    cat("\nZapis do bazy tabeli TYPY_SZKOL...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       select(tabeleWejsciowe$STYPSZK, TYP_SZK = "TYP_SZK_NAZWA"),
                       "typy_szkol", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    # W25 (słownik powiatów)
    cat("\nZapis do bazy tabeli W25 (podział terytorialny Polski)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W25, "w25", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    # W19 (wskaźniki z BDL)
    cat("\nZapis do bazy tabeli W19 (wskaźniki rynku pracy w powiatach z BDL GUS)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       rename(tabeleWejsciowe$W19, TERYT_POW = "TERYT"),
                       "w19", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    # W26 (informacje o szkołach z RSPO)
    cat("\nZapis do bazy tabel W26, W26a i W26b (charakterystyki wszystkich szkoło-lat szkolnych)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       distinct(select(tabeleWejsciowe$W26, "ID_SZK", "TYP_SZK")),
                       "w26a", wersja)))
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W26, "w26", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W20a i W20 (słownik zawodów i przypisanie zawodów do branż) ###############
  tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
    mutate(KOD_ZAW = case_when(tolower(.data$KOD_ZAW) %in% "e" ~ 10101L,
                               tolower(.data$KOD_ZAW) %in% "i" ~ 10102L,
                               TRUE ~ suppressWarnings(as.integer(.data$KOD_ZAW))),
           NAZWA_ZAW = sub(" +$", "", gsub(" ", " ", .data$NAZWA_ZAW)))
  tabeleWejsciowe$W3 <- tabeleWejsciowe$W3 %>%
    mutate(KOD_ZAW_KONT = case_when(tolower(.data$KOD_ZAW_KONT) %in% "e" ~ 10101L,
                                    tolower(.data$KOD_ZAW_KONT) %in% "i" ~ 10102L,
                                    tolower(.data$KOD_ZAW_KONT) %in% "p" ~ 10103L,
                                    TRUE ~ suppressWarnings(
                                      as.integer(.data$KOD_ZAW_KONT))))
  tabeleWejsciowe$W9 <- tabeleWejsciowe$W9 %>%
    mutate(KOD_ZAW = ifelse(.data$KOD_ZAW == "-",
                            10101L, suppressWarnings(as.integer(.data$KOD_ZAW))))
  tabeleWejsciowe$W11 <- tabeleWejsciowe$W11 %>%
    mutate(KOD_ZAW = ifelse(tolower(.data$KOD_ZAW) %in% c("eksper.", "eksp01"),
                            10101L, suppressWarnings(as.integer(.data$KOD_ZAW))))
  tabeleWejsciowe$W21 <- tabeleWejsciowe$W21 %>%
    bind_rows(tabeleWejsciowe$W21aneks %>%
                anti_join(tabeleWejsciowe$W21 %>%
                            select("KOD_KWAL", "KOD_ZAW"),
                          by = c("KOD_KWAL", "KOD_ZAW")))
  tabeleWejsciowe$W20a <- list(W20 = select(tabeleWejsciowe$W20, "KOD_ZAW"),
                               W2 = select(tabeleWejsciowe$W2, "KOD_ZAW"),
                               W3 = select(tabeleWejsciowe$W3, KOD_ZAW = "KOD_ZAW_KONT"),
                               W5 = select(tabeleWejsciowe$W5, KOD_ZAW = "KOD_ZAW_KUZ"),
                               W6 = select(tabeleWejsciowe$W6, KOD_ZAW = "KOD_ZAW_CZEL"),
                               W9 = select(tabeleWejsciowe$W9, "KOD_ZAW"),
                               W11 = select(tabeleWejsciowe$W11, "KOD_ZAW"),
                               W21 = select(tabeleWejsciowe$W21, "KOD_ZAW"))
  nieliczboweKodyZawodow <- tabeleWejsciowe$W20a %>%
    sapply(function(x) {return(class(x$KOD_ZAW))})
  nieliczboweKodyZawodow <-
    nieliczboweKodyZawodow[nieliczboweKodyZawodow != "integer"]
  komunikatONieliczbowychKodachZawodów <-
    vector(mode = "character", length = length(nieliczboweKodyZawodow))
  names(komunikatONieliczbowychKodachZawodów) <- names(nieliczboweKodyZawodow)
  for (i in names(nieliczboweKodyZawodow)) {
    tabeleWejsciowe[[i]] <- tabeleWejsciowe[[i]] %>%
      mutate(across(starts_with("KOD_ZAW"), list(ORYG = identity)),
             across(c(starts_with("KOD_ZAW"), -ends_with("_ORYG")),
                    ~suppressWarnings(as.integer(KOD_ZAW_ORYG))))
    komunikatONieliczbowychKodachZawodów[i] <-
      paste0("- W pliku '", i, ".csv': '",
             paste(tabeleWejsciowe[[i]] %>%
                     filter(if_all(c(starts_with("KOD_ZAW"), -ends_with("_ORYG")),
                                   is.na),
                            if_all(matches("^KOD_ZAW.*_ORYG$"),
                                   Negate(is.na))) %>%
                     pull("KOD_ZAW_ORYG") %>%
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
    filter(!is.na(.data$KOD_ZAW))
  zawodyBezMapowania <- tabeleWejsciowe$W20a %>%
    anti_join(tabeleWejsciowe$W20, by = "KOD_ZAW") %>%
    filter(!(.data$KOD_ZAW %in% c(100001L, 10101L:10103L))) %>%
    group_by(.data$KOD_ZAW) %>%
    summarise(tabele = paste(.data$tabela, collapse = ", "),
              .groups = "drop")
  tabeleWejsciowe$W20a <- tabeleWejsciowe$W20a %>%
    select("KOD_ZAW") %>%
    distinct()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabel W20 i W20a (kody zawodów i ich mapowanie na branże)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W20a, "w20a", wersja)))
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       filter(tabeleWejsciowe$W20, !is.na(.data$BRANZA)),
                       "w20", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  if (length(nieliczboweKodyZawodow) > 0) {
    message("Wykryto następujące kody zawodów, niebędące liczbami całkowitymi:\n",
            paste(komunikatONieliczbowychKodachZawodów, collapse = "\n"), "\n")
  }
  if (nrow(zawodyBezMapowania) > 0L) {
    message("Wykryto następujące kody zawodów, dla których brak jest mapowania na branże:\n",
            paste(zawodyBezMapowania %>%
                    mutate(KOD_ZAW = paste0(.data$KOD_ZAW,
                                            " (", .data$tabele, ")")) %>%
                    pull("KOD_ZAW"),
                  collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W20aneks.csv'.\n")
    if (zapiszProblemy) {
      write.csv2(zawodyBezMapowania,
                 "kody-zawodów-bez-przypisania-do-branż.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  ## W2 (stałe w czasie charakterystyki absolwentów) ###########################
  tabeleWejsciowe$W2 <- tabeleWejsciowe$W2 %>%
    left_join(tabeleWejsciowe$STYPSZK, by = "TYP_SZK") %>%
    select(-"TYP_SZK") %>%
    rename(TYP_SZK = "TYP_SZK_NAZWA") %>%
    mutate(NAUKA_POZA_SZKOLA = as.logical(.data$NAUKA_POZA_SZKOLA)) %>%
    group_by(.data$ID_ABS, .data$ROK_ABS, .data$ID_SZK) %>%
    mutate(LP = seq_len(n())) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W2 (absolwenci poszczególnych szkół w poszczególnych zawodach)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W2, "w2", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W3 (kontynuacja nauki w szkołach objętych SIO) ############################
  tabeleWejsciowe$W3 <- tabeleWejsciowe$W3 %>%
    left_join(tabeleWejsciowe$STYPSZK %>%
                rename(TYP_SZK_KONT = "TYP_SZK"),
              by = "TYP_SZK_KONT") %>%
    select(-"TYP_SZK_KONT") %>%
    rename(TYP_SZK_KONT = "TYP_SZK_NAZWA") %>%
    mutate(DATA_DO_SZK_KONT = ifelse(.data$DATA_DO_SZK_KONT == "31-12-9999",
                                     NA,
                                     .data$DATA_DO_SZK_KONT)) %>%
    mutate(DATA_OD_SZK_KONT = as.Date(.data$DATA_OD_SZK_KONT,
                                      tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           DATA_DO_SZK_KONT = as.Date(.data$DATA_DO_SZK_KONT,
                                      tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           CZY_UKONCZ_SZK_KONT = if_else(!is.na(.data$DATA_DO_SZK_KONT) &
                                           .data$CZY_UKONCZ_SZK_KONT %in% 0L,
                                         1L, .data$CZY_UKONCZ_SZK_KONT)) %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    group_by(.data$ID_ABS, .data$ROK_ABS, .data$ID_SZK_KONT,
             .data$DATA_OD_SZK_KONT) %>%
    mutate(LP = seq_len(n())) %>%
    ungroup()
  ### zestawienie z kontynuacją w wielu zawodach w tej samej szkole ############
  multiAbsolwenciKont <- tabeleWejsciowe$W3 %>%
    count(.data$ID_SZK_KONT, .data$TYP_SZK_KONT, .data$DATA_OD_SZK_KONT,
          .data$ID_ABS, .data$ROK_ABS,
          name = "n_zaw") %>%
    count(.data$ID_SZK_KONT, .data$TYP_SZK_KONT, .data$DATA_OD_SZK_KONT,
          .data$n_zaw) %>%
    group_by(.data$ID_SZK_KONT, .data$TYP_SZK_KONT, .data$DATA_OD_SZK_KONT) %>%
    mutate(max_n_zaw = max(.data$n_zaw)) %>%
    ungroup() %>%
    mutate(n_zaw = factor(.data$n_zaw, sort(unique(.data$n_zaw)))) %>%
    pivot_wider(names_from = "n_zaw", values_from = "n",
                names_prefix = "n_zaw", names_sort = TRUE, values_fill = 0L) %>%
    filter(.data$max_n_zaw > 1L)
  # nauka w szkole, jako absolwent której ktoś został objęty monitoringiem
  # pojawiająca się w danych o kontynuowaniu nauki
  # (spolic. z założenia wykluczane z zestawienia)
  kontynuacjaWsteczna <- inner_join(tabeleWejsciowe$W2 %>%
                                      select("ID_ABS", "ROK_ABS",
                                             "ID_SZK", "TYP_SZK", "KOD_ZAW") %>%
                                      distinct(),
                                    tabeleWejsciowe$W3 %>%
                                      select("ID_ABS", "ROK_ABS", "ID_SZK_KONT",
                                             "DATA_OD_SZK_KONT", "DATA_DO_SZK_KONT",
                                             "KOD_ZAW_KONT") %>%
                                      distinct(),
                                    by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(dlNauki = case_when(.data$TYP_SZK == "Branżowa szkoła I stopnia" ~ 3,
                               .data$TYP_SZK == "Branżowa szkoła II stopnia" ~ 2,
                               .data$TYP_SZK == "Technikum" &
                                 .data$ROK_ABS < 2024 ~ 4,
                               .data$TYP_SZK == "Liceum ogólnokształcące" &
                                 .data$ROK_ABS < 2023 ~ 3,
                               .data$TYP_SZK == "Technikum" ~ 5,
                               .data$TYP_SZK == "Liceum ogólnokształcące" ~ 4,
                               .data$TYP_SZK == "Szkoła specjalna przysposabiająca do pracy" ~ 3,
                               .data$TYP_SZK == "Bednarska Szkoła Realna" &
                                 .data$ROK_ABS < 2024 ~ 4,
                               .data$TYP_SZK == "Bednarska Szkoła Realna" ~ 5)) %>%
    filter(.data$ID_SZK_KONT == .data$ID_SZK,
           .data$KOD_ZAW_KONT %in% .data$KOD_ZAW,
           .data$TYP_SZK != "Szkoła policealna",
           year(.data$DATA_OD_SZK_KONT) >= (.data$ROK_ABS - .data$dlNauki),
           .data$DATA_DO_SZK_KONT <= as.Date(paste0(.data$ROK_ABS, "-08-31"))) %>%
    select(-"dlNauki")
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W3 (kontynuacja nauki w szkołach objętych SIO)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W3, "w3", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  if (nrow(multiAbsolwenciKont) > 0L) {
    message("Wśród szkół, w których absolwenci kontynuują naukę wykryto takie, w których te same osoby są absolwentami więcej niż jednego zawodu.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych szkół zostanie zapisane w pliku 'szkoły-z-multiabsolwentami-kontynuacja-nauki.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(multiAbsolwenciKont,
                 "szkoły-z-multiabsolwentami-kontynuacja-nauki.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  if (nrow(kontynuacjaWsteczna) > 0L) {
    message("Wśród danych o kontynuacji nauki zidentyfikowano rekordy, które wedle wszelkiego prawdopodobieństwa opisują naukę w szkole (i zawodzie), jako absolwent której dana osoba została objęta monitoringiem.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych rekordów zostanie zapisane w pliku 'nauka-w-szkole-ktorej-absolwentem-w-kontynuacji-nauki.csv'.\nUwaga, z założenia nie obejmuje ono szkół policealnych, może też nie obejmować uczniów, którzy powtarzali klasę i pewnie jeszcze jakichś innych mniej typowych przypadków.\n",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(kontynuacjaWsteczna,
                 "nauka-w-szkole-ktorej-absolwentem-w-kontynuacji-nauki.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  ## W21a i W21 (słownik kwalifikacji i przypisanie kwalifikacji do zawodów) ####
  # najpierw trzeba ujednolicić zapis kodów kwalifikacji, bo zdarzają się oboczności
  tabeleWejsciowe$W21 <- tabeleWejsciowe$W21 %>%
    mutate(KOD_KWAL = sub("\\.0", ".", sub("\\.$", "", .data$KOD_KWAL))) %>%
    distinct
  tabeleWejsciowe$W4$KOD_KWAL_KKZ <- sub("\\.0", ".",
                                         sub("\\.$", "",
                                             tabeleWejsciowe$W4$KOD_KWAL_KKZ))
  tabeleWejsciowe$W8$KOD_KWAL <- sub("\\.0", ".",
                                     sub("\\.$", "", tabeleWejsciowe$W8$KOD_KWAL))
  tabeleWejsciowe$W10$KOD_KWAL <- sub("\\.0", ".",
                                      sub("\\.$", "", tabeleWejsciowe$W10$KOD_KWAL))
  tabeleWejsciowe$W21a <- bind_rows(select(tabeleWejsciowe$W21, "KOD_KWAL"),
                                    select(tabeleWejsciowe$W4,
                                           KOD_KWAL = "KOD_KWAL_KKZ"),
                                    select(tabeleWejsciowe$W8, "KOD_KWAL"),
                                    select(tabeleWejsciowe$W10, "KOD_KWAL")) %>%
    distinct() %>%
    filter(!is.na(.data$KOD_KWAL))
  kwalifikacjeBezMapowania <- tabeleWejsciowe$W21a %>%
    anti_join(tabeleWejsciowe$W21,
              by = "KOD_KWAL") %>%
    arrange(.data$KOD_KWAL)
  if (nrow(kwalifikacjeBezMapowania) > 0L) {
    message("Wykryto następujące kody kwalifikacji, dla których brak jest mapowania na zawody:\n",
            paste(kwalifikacjeBezMapowania$KOD_KWAL, collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W21aneks.csv'.\n")
    if (zapiszProblemy) {
      write.csv2(kwalifikacjeBezMapowania,
                 "kody-kwalifikacji-bez-przypisania-do-zawodów.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabel W21 i W21a (mapowanie kodów kwalifikacji na kody zawodów)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W21a, "w21a", wersja)))
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W21, "w21", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W4 (KKZ) ##################################################################
  tabeleWejsciowe$W4 <- tabeleWejsciowe$W4 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_DO_KKZ = ifelse(.data$DATA_DO_KKZ == "31-12-9999",
                                NA_character_,
                                .data$DATA_DO_KKZ)) %>%
    mutate(DATA_OD_KKZ = as.Date(.data$DATA_OD_KKZ,
                                 tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           DATA_DO_KKZ = as.Date(.data$DATA_DO_KKZ,
                                 tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
    group_by(.data$ID_ABS, .data$ROK_ABS, .data$ID_SZK_KONT) %>%
    mutate(LP = seq_len(n())) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W4 (uczestnictwo w KKZ)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W4, "w4", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W5 (KUZ) ##################################################################
  tabeleWejsciowe$W5 <- tabeleWejsciowe$W5 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_DO_KUZ = ifelse(.data$DATA_DO_KUZ == "31-12-9999",
                                NA_character_,
                                .data$DATA_DO_KUZ)) %>%
    mutate(DATA_OD_KUZ = as.Date(.data$DATA_OD_KUZ,
                                 tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           DATA_DO_KUZ = as.Date(.data$DATA_DO_KUZ,
                                 tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
    group_by(.data$ID_ABS, .data$ROK_ABS, .data$ID_SZK_KONT) %>%
    mutate(LP = seq_len(n())) %>%
    ungroup()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W5 (uczestnictwo w KUZ)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W5, "w5", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W6 (tytuły czeladnika) ####################################################
  tabeleWejsciowe$W6 <- tabeleWejsciowe$W6 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    count(.data$ID_ABS, .data$ROK_ABS, .data$KOD_ZAW_CZEL)
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W6 (tytuły czeladnika)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W6, "w6", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  if (any(tabeleWejsciowe$W6$n > 1)) {
    message("Wśród danych o dyplomach czeladnika (W6) znaleziono powtórzone rekordy.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych szkół zostanie zapisane w pliku 'wielokrotni-czeladnicy.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, które to szkoły, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(tabeleWejsciowe$W6 %>%
                   filter(n > 1),
                 "wielokrotni-czeladnicy.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  ## W7 (matury-dyplomy) #######################################################
  # uwaga! agreguję po absolwento-latach matury!
  tabeleWejsciowe$W7 <- tabeleWejsciowe$W7 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(CZY_ZDANA_MATURA = as.logical(.data$CZY_ZDANA_MATURA),
           DATA_SWIAD_MATURA = as.Date(.data$DATA_SWIAD_MATURA,
                                       tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
    add_count(.data$ID_ABS, .data$ROK_ABS, .data$ROK_MATURA) %>%
    arrange(.data$ID_ABS, .data$ROK_ABS, .data$ROK_MATURA,
            desc(.data$CZY_ZDANA_MATURA), .data$DATA_SWIAD_MATURA) %>%
    group_by(.data$ID_ABS, .data$ROK_ABS, .data$ROK_MATURA) %>%
    slice(1L) %>%
    ungroup()
  maturyBrakDaty <- is.na(tabeleWejsciowe$W7$DATA_SWIAD_MATURA) &
    tabeleWejsciowe$W7$CZY_ZDANA_MATURA
  if (any(maturyBrakDaty, na.rm = TRUE)) {
    warning("\nW danych dot. wyników matury (W7) łącznie ",
            sum(maturyBrakDaty, na.rm = TRUE),
            " rekord(y/ów) ma brak danych w kolumnie DATA_SWIAD_MATURA, choć wartość kolumny CZY_ZDANA_MATURA wskazuje na zdanie matury!\nRekord(y) te(n) zostan(ie/ą) pominięt(y/e) przy imporcie do bazy.\n",
            immediate. = TRUE, call. = FALSE)
  }
  tabeleWejsciowe$W7 <- tabeleWejsciowe$W7 %>%
    filter(!.data$CZY_ZDANA_MATURA | !is.na(.data$DATA_SWIAD_MATURA))
  maturyWPRzyszlosci <- year(tabeleWejsciowe$W7$DATA_SWIAD_MATURA) > rokMonitoringu
  if (any(maturyWPRzyszlosci, na.rm = TRUE)) {
    warning("\nW danych dot. wyników matury (W7) łącznie ",
            sum(maturyWPRzyszlosci, na.rm = TRUE),
            " rekord(y/ów) ma wartosći kolumny DATA_SWIAD_MATURA wskazujące na lata późniejsze niż ",
            rokMonitoringu, "!\n", immediate. = TRUE, call. = FALSE)
  }
  if (!any(tabeleWejsciowe$W7$ROK_MATURA < min(tabeleWejsciowe$W7$ROK_ABS))) {
    message("W danych dot. wyników matury (W7) nie ma rekordów opisujących egzaminy z lat wcześniejszych, niż rok ukończenia szkoły dla ROK_ABS równych: ",
            tabeleWejsciowe$W7 %>%
              group_by(.data$ROK_ABS) %>%
              summarise(brakWczesniejszychWynikow =
                          !any(.data$ROK_MATURA < .data$ROK_ABS)) %>%
              filter(.data$brakWczesniejszychWynikow) %>%
              pull("ROK_ABS") %>%
              paste(collapse = ", "),
            ". Dane te są najprawdopodobniej niekompletne!\n")
  }
  ## Szybki test (nie)kompletności danych o wynikach matury ####################
  diagnostykaKompletnosciMatura <- tabeleWejsciowe$W2 %>%
    filter(.data$TYP_SZK %in% c("Bednarska Szkoła Realna",
                                "Liceum ogólnokształcące",
                                "Technikum",
                                "Branżowa szkoła II stopnia")) %>%
    select("ID_ABS", "ROK_ABS", "TYP_SZK") %>%
    distinct() %>%
    left_join(tabeleWejsciowe$W7,
              by = c("ID_ABS", "ROK_ABS")) %>%
    group_by(.data$TYP_SZK, .data$ROK_ABS) %>%
    summarise(zdawal_mature_pct = round(100*mean(!is.na(.data$ROK_MATURA)), 2),
              .groups = "drop")
  if (any((diagnostykaKompletnosciMatura$zdawal_mature_pct < 80 &
           diagnostykaKompletnosciMatura$TYP_SZK != "Branżowa szkoła II stopnia") |
          diagnostykaKompletnosciMatura$zdawal_mature_pct < 40)) {
    message("Dane o wynikach matury (tabela 'W7') wyglądają na niekompletne.",
            ifelse(zapiszProblemy,
                   "\nPodsumowanie informacji o odsetkach danych, które udało się przyłączyć zostanie zapisane w pliku 'matura-kompletnosc.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, jak wielu absolwentom udało się przyłączyć dane o wynikach matury, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
  }
  if (zapiszProblemy) {
    write.csv2(diagnostykaKompletnosciMatura,
               "matura-kompletnosc.csv",
               row.names = FALSE, na = "", fileEncoding = "UTF-8")
  }
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W7 (wyniki egzaminu maturalnego)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W7, "w7", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W27 (wyniki egzaminu ósmoklasisty) ########################################
  # Informacje o wynikach są agregowane w ramach absolwento-roko-przedmiotów
  # wybierając tylko najbardziej adekwatny 'stan' wg następującej hierarchii preferencji:
  #  "Zwolniony" > "Laureat" > "Finalista" > "Przystąpił" > "Unieważniony" > "Przerwany" > "Nieobecny" > "Nie przeprowadzono egzaminu"
  # Dodatkowo, kolumna TERMIN_E8 tworzona jest z użyciem następującyej heurystyki:
  #  - statusy "Zwolniony", "Laureat" i "Finalista" zawsze są przypisywane do terminu głównego;
  # - inny status przypisywany jest do terminu głównego, jeśli dany abolwento-roko-przedmiot
  #    opisuje tylko jeden rekord i do terminu dodatkowego w każdym innym przypadku.
  if ("W27" %in% names(tabeleWejsciowe)) {
    tabeleWejsciowe$W27 <- tabeleWejsciowe$W27 %>%
      semi_join(tabeleWejsciowe$W1,
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(DOSTOS_WAR_E8 = as.logical(.data$DOSTOS_WAR_E8),
             WYNIK_E8 = ifelse(!(.data$STAN_ZDAWANIA_E8 %in%
                                   c("Przystąpił", "Laureat", "Finalista")),
                               NA_real_, .data$WYNIK_E8))
    w27WynikiPozaZakresem <- tabeleWejsciowe$W27 %>%
      filter(.data$WYNIK_E8 < 0 | .data$WYNIK_E8 > 100)
    if (nrow(w27WynikiPozaZakresem) > 0L) {
      warning("\nW danych dot. wyników egzaminu ósmoklasisty (W27) łącznie ",
              nrow(w27WynikiPozaZakresem),
              " rekord(y/ów) ma wyniki poza zakresem [0, 100]!\n",
              "Wynik(i) te(n) zostan(ą/ie) zastąpion(y/e) brakiem danych.\n",
              immediate. = TRUE, call. = FALSE)
      tabeleWejsciowe$W27 <- tabeleWejsciowe$W27 %>%
        mutate(WYNIK_E8 = ifelse(.data$WYNIK_E8 < 0 | .data$WYNIK_E8 > 100,
                                 NA_real_, .data$WYNIK_E8))
    }
    diagnostykaKompletnosciE8 <- tabeleWejsciowe$W2 %>%
      left_join(tabeleWejsciowe$W27 %>%
                  select(.data$ID_ABS, .data$ROK_ABS, .data$ROK_E8) %>%
                  distinct() %>%
                  count(.data$ID_ABS, .data$ROK_ABS, name = "lPrzystapienE8"),
                by = c("ID_ABS", "ROK_ABS")) %>%
      group_by(.data$ROK_ABS, .data$TYP_SZK) %>%
      summarise(ma_wynik_e8_pct = round(100*mean(!is.na(.data$lPrzystapienE8)), 2),
                .groups = "drop") %>%
      mutate(pierwszy_rok_abs_e8 = c("Branżowa szkoła I stopnia" = 2022L,
                                     "Liceum ogólnokształcące" = 2023L,
                                     "Bednarska Szkoła Realna" = 2023L,
                                     "Branżowa szkoła II stopnia" = 2024L,
                                     "Technikum" = 2024L)[.data$TYP_SZK],
             byc_moze_problem =
               case_when(.data$ROK_ABS >= (.data$pierwszy_rok_abs_e8 + 1L) &
                           .data$TYP_SZK %in% c("Branżowa szkoła I stopnia") &
                           .data$ma_wynik_e8_pct < 97 ~ TRUE,
                         .data$ROK_ABS >= .data$pierwszy_rok_abs_e8 &
                           .data$TYP_SZK %in% c("Bednarska Szkoła Realna",
                                                "Technikum") &
                           .data$ma_wynik_e8_pct < 97 ~ TRUE,
                         .data$ROK_ABS >= .data$pierwszy_rok_abs_e8 &
                           .data$TYP_SZK == "Liceum ogólnokształcące" &
                           .data$ma_wynik_e8_pct < 90 ~ TRUE,
                         .data$ROK_ABS >= (.data$pierwszy_rok_abs_e8 + 1L) &
                           .data$TYP_SZK == "Branżowa szkoła II stopnia" &
                           .data$ma_wynik_e8_pct < 85 ~ TRUE,
                         .default = FALSE))
    if (any(diagnostykaKompletnosciE8$byc_moze_problem)) {
      message("Dane o wynikach egzaminu ósmoklasisty (tabela 'W27') wyglądają na niekompletne.",
              ifelse(zapiszProblemy,
                     "\nPodsumowanie informacji o odsetkach danych, które udało się przyłączyć zostanie zapisane w pliku 'wyniki-e8-kompletnosc.csv.csv'.\n",
                     "\nJeżeli chcesz móc sprawdzić, jak wielu absolwentom udało się przyłączyć dane o wynikach egzaminu ósmoklasisty, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    }
    if (zapiszProblemy) {
      write.csv2(diagnostykaKompletnosciE8,
                 "wyniki-e8-kompletnosc.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
    if (wczytajDoBazy) {
      cat("\nZapis do bazy tabel W27 i W27d (wyniki egzaminu ósmoklasisty)...")
      tabeleWejsciowe$W27d <- tabeleWejsciowe$W27 %>%
        filter(.data$TYP_ARKUSZA_E8 != "standardowy") %>%
        group_by(.data$ID_ABS, .data$ROK_ABS, .data$ROK_E8, .data$PRZEDMIOT_E8) %>%
        reframe(DOSTOS_FORM_OPIS_E8 =
                  unique(unlist(strsplit(.data$TYP_ARKUSZA_E8, " / "))),
                DOSTOS_WAR_E8 = any(.data$DOSTOS_WAR_E8)) %>%
        add_count(.data$ID_ABS, .data$ROK_ABS, .data$ROK_E8, .data$PRZEDMIOT_E8,
                  name = "lWynikow")
      tabeleWejsciowe$W27 <- tabeleWejsciowe$W27 %>%
        mutate(DOSTOS_FORM_E8 = .data$TYP_ARKUSZA_E8 != "standardowy") %>%
        add_count(.data$ID_ABS, .data$ROK_ABS, .data$ROK_E8, .data$PRZEDMIOT_E8,
                  name = "lWynikow")
      tabeleWejsciowe$W27 <-
        bind_rows(tabeleWejsciowe$W27 %>%
                    filter(.data$lWynikow == 1L) %>%
                    mutate(TERMIN_E8 = "główny") %>%
                    select(-c("TYP_ARKUSZA_E8", "lWynikow")),
                  tabeleWejsciowe$W27 %>%
                    filter(.data$lWynikow > 1L) %>%
                    mutate(STAN_ZDAWANIA_E8 =
                             factor(.data$STAN_ZDAWANIA_E8,
                                    c("Zwolniony", "Laureat", "Finalista",
                                      "Przystąpił",
                                      "Unieważniony", "Przerwany", "Nieobecny",
                                      "Nie przeprowadzono egzaminu"))) %>%
                    arrange(.data$ID_ABS, .data$ROK_ABS, .data$ROK_E8,
                            .data$PRZEDMIOT_E8, .data$STAN_ZDAWANIA_E8) %>%
                    group_by(.data$ID_ABS, .data$ROK_ABS, .data$ROK_E8,
                             .data$PRZEDMIOT_E8) %>%
                    summarise(DOSTOS_WAR_E8 = any(.data$DOSTOS_WAR_E8),
                              DOSTOS_FORM_E8 = any(.data$DOSTOS_FORM_E8),
                              WYNIK_E8 = max(.data$WYNIK_E8[
                                .data$STAN_ZDAWANIA_E8 == .data$STAN_ZDAWANIA_E8[1]]),
                              STAN_ZDAWANIA_E8 =
                                as.character(.data$STAN_ZDAWANIA_E8[1]),
                              TERMIN_E8 =
                                ifelse(.data$STAN_ZDAWANIA_E8 %in%
                                         c("Zwolniony", "Laureat", "Finalista"),
                                       "główny", "dodatkowy"),
                              .groups = "drop"))
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W27, "w27", wersja)))
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W27d, "w27d", wersja)))
      cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    }
  }
  ## W28 (wyniki procentowe matury) ############################################
  # W rzadkich przypadkach, gdy występuje kilka rekordów opisujących ten sam
  # absolwento-roko-przedmioto-poziomo-formę zdawania wybieram rekord
  # z najwyższym wynikiem.
  if ("W28" %in% names(tabeleWejsciowe)) {
    tabeleWejsciowe$W28 <- tabeleWejsciowe$W28 %>%
      semi_join(tabeleWejsciowe$W1,
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(TERMIN_MATURA =
               as.character(factor(.data$TERMIN_MATURA,
                                   1L:3L,
                                   c("główny", "poprawkowy", "dodatkowy"))),
             FORMA_ZDAWANIA =
               as.character(factor(.data$FORMA_ZDAWANIA,
                                   c(1L, 3L),
                                   c("pisemna", "ustna"))),
             DOSTOS_WAR_MATURA = as.logical(ifelse(.data$ROK_MATURA < 2021L,
                                                   NA_integer_,
                                                   .data$DOSTOS_WAR_MATURA)),
             WYNIK_MATURA = ifelse(!(.data$STAN_ZDAWANIA_MATURA %in%
                                   c("Przystąpił", "Laureat", "Finalista")),
                               NA_real_, .data$WYNIK_MATURA))
    w28WynikiPozaZakresem <- tabeleWejsciowe$W28 %>%
      filter(.data$WYNIK_MATURA < 0 | .data$WYNIK_MATURA > 100)
    if (nrow(w28WynikiPozaZakresem) > 0L) {
      warning("\nW danych dot. wyników procentowych matury (W28) łącznie ",
              nrow(w28WynikiPozaZakresem),
              " rekord(y/ów) ma wyniki poza zakresem [0, 100]!\n",
              "Wynik(i) te(n) zostan(ą/ie) zastąpion(y/e) brakiem danych.\n",
              immediate. = TRUE, call. = FALSE)
      tabeleWejsciowe$W28 <- tabeleWejsciowe$W28 %>%
        mutate(WYNIK_MATURA = ifelse(.data$WYNIK_MATURA < 0 | .data$WYNIK_MATURA > 100,
                                 NA_real_, .data$WYNIK_MATURA))
    }
    diagnostykaKompletnosciMatura <- diagnostykaKompletnosciMatura %>%
      full_join(tabeleWejsciowe$W2 %>%
                  filter(.data$TYP_SZK %in% c("Bednarska Szkoła Realna",
                                              "Liceum ogólnokształcące",
                                              "Technikum",
                                              "Branżowa szkoła II stopnia")) %>%
                  select("ID_ABS", "ROK_ABS", "TYP_SZK") %>%
                  distinct() %>%
                  left_join(tabeleWejsciowe$W28,
                            by = c("ID_ABS", "ROK_ABS")) %>%
                  group_by(.data$TYP_SZK, .data$ROK_ABS) %>%
                  summarise(ma_wyniki_matury_pct =
                              round(100*mean(!is.na(.data$ROK_MATURA)), 2),
                            .groups = "drop"),
                by = c("TYP_SZK", "ROK_ABS"))
    if (any(diagnostykaKompletnosciMatura$ma_wyniki_matury_pct < 80)) {
      message("Dane o wynikach procentowych matury (tabela 'W28') wyglądają na niekompletne.",
              ifelse(zapiszProblemy,
                     "\nPodsumowanie informacji o odsetkach danych, które udało się przyłączyć zostanie zapisane w pliku 'matura-wyniki-kompletnosc.csv'.\n",
                     "\nJeżeli chcesz móc sprawdzić, jak wielu absolwentom udało się przyłączyć dane o wynikach matury, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    }
    if (zapiszProblemy) {
      write.csv2(diagnostykaKompletnosciMatura,
                 "matura-wyniki-kompletnosc.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
    tabeleWejsciowe$W28 <- tabeleWejsciowe$W28 %>%
      mutate(DOSTOS_FORM_MATURA = .data$ARKUSZ_MATURA != "standardowy") %>%
      add_count(.data$ID_ABS, .data$ROK_ABS, .data$ROK_MATURA,
                .data$TERMIN_MATURA, .data$PRZEDMIOT_MATURA,
                .data$POZIOM_MATURA, .data$FORMA_ZDAWANIA, name = "lWynikow")
    if (any(tabeleWejsciowe$W28$lWynikow > 1L)) {
      message("W danych o wynikach procentowych matury (tabela 'W28') wystąpił(y/o) ",
              tabeleWejsciowe$W28 %>%
                filter(.data$lWynikow > 1L) %>%
                select("ID_ABS", "ROK_ABS", "ROK_MATURA", "TERMIN_MATURA",
                       "PRZEDMIOT_MATURA", "POZIOM_MATURA", "FORMA_ZDAWANIA") %>%
                distinct() %>%
                nrow(), " kombinacj(a/e) wartości kolumn (ID_ABS, ROK_ABS, ROK_MATURA, TERMIN_MATURA, PRZEDMIOT_MATURA, POZIOM_MATURA, FORMA_ZDAWANIA), któr(ą/e) opisuje więcej niż jeden rekord.\n",
              "W ramach każdej z tych kombinacji zostanie zapisany tylko rekord z najwyższym wynikiem.",
              ifelse(zapiszProblemy,
                     "\nWycinek 'W28' zawierający te rekordy zostanie zapisany w pliku 'wielokrotne-wyniki-matury.csv'.\n",
                     "\nJeżeli chcesz móc sprawdzić, jak wielu absolwentom udało się przyłączyć dane o wynikach matury, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
      if (zapiszProblemy) {
        write.csv2(tabeleWejsciowe$W28 %>%
                     filter(.data$lWynikow > 1L),
                   "wielokrotne-wyniki-matury.csv",
                   row.names = FALSE, na = "", fileEncoding = "UTF-8")
      }
    }
    if (wczytajDoBazy) {
      cat("\nZapis do bazy tabel W28 i W28d (wyniki procentowe matury)...")
      tabeleWejsciowe$W28d <- tabeleWejsciowe$W28 %>%
        filter(.data$ARKUSZ_MATURA != "standardowy") %>%
        group_by(.data$ID_ABS, .data$ROK_ABS, .data$ROK_MATURA,
                 .data$TERMIN_MATURA, .data$PRZEDMIOT_MATURA,
                 .data$POZIOM_MATURA, .data$FORMA_ZDAWANIA,
                 .data$DOSTOS_WAR_MATURA) %>%
        reframe(DOSTOS_FORM_OPIS_MATURA = unique(unlist(strsplit(.data$ARKUSZ_MATURA, " / "))))
      tabeleWejsciowe$W28 <-
        bind_rows(tabeleWejsciowe$W28 %>%
                    filter(.data$lWynikow == 1L) %>%
                    select(-c("ARKUSZ_MATURA", "lWynikow")),
                  tabeleWejsciowe$W28 %>%
                    filter(.data$lWynikow > 1L) %>%
                    mutate(STAN_ZDAWANIA_MATURA =
                             factor(.data$STAN_ZDAWANIA_MATURA,
                                    c("Zwolniony", "Laureat", "Finalista",
                                      "Przystąpił",
                                      "Unieważniony", "Przerwany", "Nieobecny",
                                      "Nie przeprowadzono egzaminu"))) %>%
                    arrange(.data$ID_ABS, .data$ROK_ABS, .data$ROK_MATURA,
                            .data$TERMIN_MATURA, .data$PRZEDMIOT_MATURA,
                            .data$POZIOM_MATURA, .data$FORMA_ZDAWANIA,
                            .data$STAN_ZDAWANIA_MATURA) %>%
                    group_by(.data$ID_ABS, .data$ROK_ABS, .data$ROK_MATURA,
                             .data$TERMIN_MATURA, .data$PRZEDMIOT_MATURA,
                             .data$POZIOM_MATURA, .data$FORMA_ZDAWANIA) %>%
                    summarise(DOSTOS_WAR_MATURA = any(.data$DOSTOS_WAR_MATURA),
                              DOSTOS_FORM_MATURA = any(.data$DOSTOS_FORM_MATURA),
                              WYNIK_MATURA = max(.data$WYNIK_MATURA[
                                .data$STAN_ZDAWANIA_MATURA == .data$STAN_ZDAWANIA_MATURA[1]]),
                              STAN_ZDAWANIA_MATURA =
                                as.character(.data$STAN_ZDAWANIA_MATURA[1]),
                              .groups = "drop"))
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W28, "w28", wersja)))
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W28d, "w28d", wersja)))
      cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    }
  }
  ## W8 i W10 (świadectwa potwierdzające kwalifikacje) #########################
  # uwaga! agreguję po absolwento-kwalifikacjach! (wybierając pierwsze zdanie)
  # uwaga! DATA_KWAL może opisywać różne rzeczy w zależności od OKE (zapewne:
  # datę faktycznego potwierdzenia zdania egzaminu, umowną datę zdania egzaminu
  # lub datę odbioru certfikatu)
  tabeleWejsciowe$W8 <- tabeleWejsciowe$W8 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    # W W8 zdarzają się rekordy, które mają podaną DATA_KWAL, choć nie mają
    # DOK_POTW_KWAL, ani wyników, ani dostosowania - usuwam je
    mutate(DATA_KWAL = as.Date(ifelse(!is.na(.data$DOK_POTW_KWAL),
                                      .data$DATA_KWAL, NA_character_),
                               tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           across(any_of("SESJA_EGZ_ZAW"),
                  ~sub("^([[:digit:]]{2}\\.)([[:digit:]])$", "\\10\\2", .))) %>%
    filter(rowSums(!is.na(pick(any_of(c("DOK_POTW_KWAL",
                                        "WYNIK_PISEMN_EGZ_ZAW",
                                        "WYNIK_PRAKT_EGZ_ZAW",
                                        "DOSTOSOWANIE_EGZ_ZAW")))),
                   na.rm = TRUE) > 0)
  w8BrakDaty <- is.na(tabeleWejsciowe$W8$DATA_KWAL) &
    !is.na(tabeleWejsciowe$W8$DOK_POTW_KWAL)
  if (any(w8BrakDaty, na.rm = TRUE)) {
    warning("\nW danych dot. certyfikatów kwalifikacji (W8) łącznie ",
            sum(w8BrakDaty, na.rm = TRUE),
            " rekord(y/ów) ma brak danych w kolumnie DATA_KWAL!\n",
            immediate. = TRUE, call. = FALSE)
  }
  w8WPRzyszlosci <- year(tabeleWejsciowe$W8$DATA_KWAL) > rokMonitoringu
  if (any(w8WPRzyszlosci, na.rm = TRUE)) {
    warning("\nW danych dot. certyfikatów kwalifikacji (W8) łącznie ",
            sum(w8WPRzyszlosci, na.rm = TRUE),
            " rekord(y/ów) ma w kolumnie DATA_KWAL wartości wskazujące na lata późniejsze niż ",
            rokMonitoringu, "!\n", immediate. = TRUE, call. = FALSE)
  }
  w8WynikiPozaZakresem <- tabeleWejsciowe$W8 %>%
    filter((rowSums(pick(any_of(c("WYNIK_PISEMN_EGZ_ZAW",
                                  "WYNIK_PRAKT_EGZ_ZAW"))) < 0,
                    na.rm = TRUE) > 0) |
             (rowSums(pick(any_of(c("WYNIK_PISEMN_EGZ_ZAW",
                                    "WYNIK_PRAKT_EGZ_ZAW"))) > 100,
                      na.rm = TRUE) > 0))
  if (nrow(w8WynikiPozaZakresem) > 0L) {
    warning("\nW danych dot. certyfikatów kwalifikacji (W10) łącznie ",
            nrow(w8WynikiPozaZakresem),
            " rekord(y/ów) ma wyniki egzaminów zawodowych poza zakresem [0, 100]!\n",
            "Wynik(i) te(n) zostan(ą/ie) zastąpion(y/e) brakiem danych.\n",
            immediate. = TRUE, call. = FALSE)
    tabeleWejsciowe$W8 <- tabeleWejsciowe$W8 %>%
      mutate(across(c("WYNIK_PISEMN_EGZ_ZAW", "WYNIK_PRAKT_EGZ_ZAW"),
                    ~ifelse(. < 0 | . > 100, NA_real_, .)))
  }
  tabeleWejsciowe$W10 <- tabeleWejsciowe$W10 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_KWAL = as.Date(ifelse(!is.na(.data$DOK_POTW_KWAL),
                                      .data$DATA_KWAL, NA_character_),
                               tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           across(any_of("SESJA_EGZ_ZAW"),
                  ~sub("^([[:digit:]]{2}\\.)([[:digit:]])$", "\\10\\2", .))) %>%
    filter(rowSums(!is.na(pick(any_of(c("DOK_POTW_KWAL",
                                        "WYNIK_PISEMN_EGZ_ZAW",
                                        "WYNIK_PRAKT_EGZ_ZAW",
                                        "DOSTOSOWANIE_EGZ_ZAW")))),
                   na.rm = TRUE) > 0)
  w10BrakDaty <- is.na(tabeleWejsciowe$W10$DATA_KWAL) &
    !is.na(tabeleWejsciowe$W10$DOK_POTW_KWAL)
  if (any(w10BrakDaty, na.rm = TRUE)) {
    warning("\nW danych dot. certyfikatów kwalifikacji (W10) łącznie ",
            sum(w10BrakDaty, na.rm = TRUE),
            " rekord(y/ów) ma brak danych w kolumnie DATA_KWAL!\n",
            immediate. = TRUE, call. = FALSE)
  }
  w10WPRzyszlosci <- year(tabeleWejsciowe$W10$DATA_KWAL) > rokMonitoringu
  if (any(w10WPRzyszlosci, na.rm = TRUE)) {
    warning("\nW danych dot. certyfikatów kwalifikacji (W10) łącznie ",
            sum(w10WPRzyszlosci, na.rm = TRUE),
            " rekord(y/ów) ma wartości wskazujące na lata późniejsze niż ",
            rokMonitoringu, "!\n",
            immediate. = TRUE, call. = FALSE)
  }
  w10WynikiPozaZakresem <- tabeleWejsciowe$W10 %>%
    filter((rowSums(pick(any_of(c("WYNIK_PISEMN_EGZ_ZAW",
                                  "WYNIK_PRAKT_EGZ_ZAW"))) < 0,
                    na.rm = TRUE) > 0) |
             (rowSums(pick(any_of(c("WYNIK_PISEMN_EGZ_ZAW",
                                    "WYNIK_PRAKT_EGZ_ZAW"))) > 100,
                      na.rm = TRUE) > 0))
  if (nrow(w10WynikiPozaZakresem) > 0L) {
    warning("\nW danych dot. certyfikatów kwalifikacji (W10) łącznie ",
            nrow(w10WynikiPozaZakresem),
            " rekord(y/ów) ma wyniki egzaminów zawodowych poza zakresem [0, 100]!\n",
            "Wynik(i) te(n) zostan(ą/ie) zastąpion(y/e) brakiem danych.\n",
            immediate. = TRUE, call. = FALSE)
    tabeleWejsciowe$W10 <- tabeleWejsciowe$W10 %>%
      mutate(across(c("WYNIK_PISEMN_EGZ_ZAW", "WYNIK_PRAKT_EGZ_ZAW"),
                    ~ifelse(. < 0 | . > 100, NA_real_, .)))
  }
  multiKwal <- bind_rows(list(W8 = tabeleWejsciowe$W8,
                              W10 = tabeleWejsciowe$W10), .id = "table") %>%
    select(-any_of(c("DOSTOSOWANIE_EGZ_ZAW",
                     "WYNIK_PISEMN_EGZ_ZAW", "WYNIK_PRAKT_EGZ_ZAW"))) %>%
    filter(!is.na(.data$DOK_POTW_KWAL)) %>%
    distinct() %>%
    group_by(.data$ID_ABS, .data$ROK_ABS, .data$KOD_KWAL) %>%
    mutate(n = n(),
           rozne_tabele = n_distinct(.data$table)) %>%
    ungroup() %>%
    filter(n > 1L) %>%
    arrange(.data$ID_ABS, .data$ROK_ABS, .data$KOD_KWAL, .data$DATA_KWAL)
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W810 (certyfikaty kwalifikacji)...")
    tabeleWejsciowe$w810 <- bind_rows(tabeleWejsciowe$W8,
                                      tabeleWejsciowe$W10) %>%
      select("ID_ABS", "ROK_ABS", "KOD_KWAL", "DOK_POTW_KWAL", "DATA_KWAL",
             any_of(c("ROK_KWAL", "ROK_EGZ_ZAW", "SESJA_EGZ_ZAW",
                      "WYNIK_PISEMN_EGZ_ZAW", "WYNIK_PRAKT_EGZ_ZAW"))) %>%
      distinct() %>%
      arrange(.data$ID_ABS, .data$ROK_ABS, .data$KOD_KWAL, .data$DATA_KWAL,
              desc(across(any_of(c("WYNIK_PRAKT_EGZ_ZAW",
                                   "WYNIK_PISEMN_EGZ_ZAW"))))) %>%
      group_by(across(c(all_of(c("ID_ABS", "ROK_ABS", "KOD_KWAL")),
                        any_of(c("ROK_EGZ_ZAW", "SESJA_EGZ_ZAW"))))) %>%
      slice(1L) %>%
      ungroup() %>%
      mutate(across(any_of("SESJA_EGZ_ZAW"), as.character))
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$w810, "w810", wersja)))
    if ("DOSTOSOWANIE_EGZ_ZAW" %in% c(names(tabeleWejsciowe$W8),
                                      names(tabeleWejsciowe$W10))) {
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         bind_rows(tabeleWejsciowe$W8,
                                   tabeleWejsciowe$W10) %>%
                           filter(!is.na(.data$DOSTOSOWANIE_EGZ_ZAW)) %>%
                           mutate(across(any_of("SESJA_EGZ_ZAW"), as.character)) %>%
                           select("ID_ABS", "ROK_ABS", "KOD_KWAL",
                                  "ROK_EGZ_ZAW", "SESJA_EGZ_ZAW",
                                  "DOSTOSOWANIE_EGZ_ZAW") %>%
                           distinct() %>%
                           semi_join(tabeleWejsciowe$w810,
                                     by = c("ID_ABS", "ROK_ABS", "KOD_KWAL",
                                            "ROK_EGZ_ZAW", "SESJA_EGZ_ZAW")),
                         "w810d", wersja)))
    }
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  if (nrow(multiKwal) > 0L) {
    message("Wykryto osoby, które kilkakrotnie zdawały egzaminy na tę samą kwalifikację.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych osób i kwalifikacji zostanie zapisane w pliku 'wielokrotne-kwalifikacje.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, które to osoby i kwalifikacje, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(multiKwal,
                 "wielokrotne-kwalifikacje.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
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
    mutate(DATA_DYP_ZAW = as.Date(.data$DATA_DYP_ZAW,
                                  tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           ROK_DYP = as.numeric(format(.data$DATA_DYP_ZAW, "%Y")))
  w9BrakDaty <- is.na(tabeleWejsciowe$W9$DATA_DYP_ZAW)
  if (any(w9BrakDaty, na.rm = TRUE)) {
    warning("\nW danych dot. dyplomów zawodowych (W9) łącznie ",
            sum(w9BrakDaty, na.rm = TRUE),
            " rekord(y/ów) ma brak danych w kolumnie DATA_DYP_ZAW!\n",
            immediate. = TRUE, call. = FALSE)
  }
  w9WPRzyszlosci <- year(tabeleWejsciowe$W9$DATA_DYP_ZAW) > rokMonitoringu
  if (any(w9WPRzyszlosci, na.rm = TRUE)) {
    warning("\nW danych dot. dyplomów zawodowych (W9) łącznie ",
            sum(w9WPRzyszlosci, na.rm = TRUE),
            " rekord(y/ów) ma w kolumnie DATA_DYP_ZAW wartości wskazujące na lata późniejsze niż ",
            rokMonitoringu, "!\n", immediate. = TRUE, call. = FALSE)
  }
  tabeleWejsciowe$W11 <- tabeleWejsciowe$W11 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_DYP_ZAW = as.Date(.data$DATA_DYP_ZAW,
                                  tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           ROK_DYP = as.numeric(format(.data$DATA_DYP_ZAW, "%Y")))
  w11BrakDaty <- is.na(tabeleWejsciowe$W11$DATA_DYP_ZAW)
  if (any(w11BrakDaty, na.rm = TRUE)) {
    warning("\nW danych dot. dyplomów zawodowych (W11) łącznie ",
            sum(w11BrakDaty, na.rm = TRUE),
            " rekord(y/ów) ma brak danych w kolumnie DATA_DYP_ZAW!\n",
            immediate. = TRUE, call. = FALSE)
  }
  w11WPRzyszlosci <- year(tabeleWejsciowe$W11$DATA_DYP_ZAW) > rokMonitoringu
  if (any(w11WPRzyszlosci, na.rm = TRUE)) {
    warning("\nW danych dot. dyplomów zawodowych (W11) łącznie ",
            sum(w11WPRzyszlosci, na.rm = TRUE),
            " rekord(y/ów) ma w kolumnie DATA_DYP_ZAW wartości wskazujące na lata późniejsze niż ",
            rokMonitoringu, "!\n", immediate. = TRUE, call. = FALSE)
  }
  multiDypZaw <- bind_rows(list(W9 = tabeleWejsciowe$W9,
                                W11 = tabeleWejsciowe$W11), .id = "table") %>%
    distinct() %>%
    group_by(.data$ID_ABS, .data$ROK_ABS, .data$KOD_ZAW) %>%
    mutate(n = n(),
           rozne_tabele = n_distinct(.data$table)) %>%
    filter(n > 1L)
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W911 (dyplomy zawodowe)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       bind_rows(tabeleWejsciowe$W9,
                                 tabeleWejsciowe$W11) %>%
                         select("ID_ABS", "ROK_ABS", "KOD_ZAW", "ROK_DYP",
                                "DOK_POTW_DYP", "DATA_DYP_ZAW") %>%
                         filter(!is.na(.data$KOD_ZAW)) %>%
                         distinct() %>%
                         arrange(.data$ID_ABS, .data$ROK_ABS, .data$KOD_ZAW,
                                 .data$ROK_DYP, .data$DATA_DYP_ZAW) %>%
                         group_by(.data$ID_ABS, .data$ROK_ABS, .data$KOD_ZAW) %>%
                         slice(1L) %>%
                         ungroup(),
                       "w911", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  if (any(is.na(tabeleWejsciowe$W9$KOD_ZAW)) ||
      any(is.na(tabeleWejsciowe$W11$KOD_ZAW))) {
    message("Wykryto osoby, dla których w informacjach o dyplomie (W9 lub W11) występują braki danych (najbardziej prawdopodobna przyczyna to, że są to zawody eksperymentalne).\nRekordy te nie zostaną zapisane do bazy.\n")
  }
  if (nrow(multiDypZaw) > 0L) {
    message("Wykryto osoby, które kilkakrotnie uzyskały dyplom w tym samym zawodzie.",
            ifelse(zapiszProblemy,
                   "\nZestawienie tych osób i zawodów zostanie zapisane w pliku 'wielokrotne-dyplomy.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, które to osoby i zawody, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(multiDypZaw,
                 "wielokrotne-dyplomy.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  ## Szybki test (nie)kompletności danych o egzaminach zawodowych ##############
  diagnostykaKompletnosciEgzZaw <- tabeleWejsciowe$W2 %>%
    filter(!is.na(.data$KOD_ZAW)) %>%
    select("ID_ABS", "ROK_ABS", "TYP_SZK") %>%
    distinct() %>%
    left_join(bind_rows(tabeleWejsciowe$W8,
                        tabeleWejsciowe$W10) %>%
                select("ID_ABS", "ROK_ABS") %>%
                distinct() %>%
                mutate(swiadectwo = 1),
              by = c("ID_ABS", "ROK_ABS")) %>%
    left_join(bind_rows(tabeleWejsciowe$W9,
                        tabeleWejsciowe$W11) %>%
                select("ID_ABS", "ROK_ABS") %>%
                distinct() %>%
                mutate(dyplom = 1),
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(across(c("swiadectwo", "dyplom"),
                  ~if_else(is.na(.), 0, .))) %>%
    group_by(.data$TYP_SZK, .data$ROK_ABS) %>%
    summarise(across(c("swiadectwo", "dyplom"),
                     list(pct = ~round(100*mean(.), 2))),
              .groups = "drop")
  if (any(diagnostykaKompletnosciEgzZaw$swiadectwo_pct < 40) ||
      any(diagnostykaKompletnosciEgzZaw$dyplom_pct < 35) ||
      any(diagnostykaKompletnosciEgzZaw$swiadectwo_pct < 70 &
          diagnostykaKompletnosciEgzZaw$TYP_SZK != "Branżowa szkoła I stopnia") ||
      any(diagnostykaKompletnosciEgzZaw$dyplom_pct < 60 &
          diagnostykaKompletnosciEgzZaw$TYP_SZK != "Branżowa szkoła I stopnia")) {
    message("Dane o certyfikatach lub dyplomach zawodowych (tabele 'W8', 'W9', 'W10' i 'W11') wyglądają na niekompletne.",
            ifelse(zapiszProblemy,
                   "\nPodsumowanie informacji o odsetkach danych, które udało się przyłączyć zostanie zapisane w pliku 'certyfikaty-i-dyplomy-kompletnosc.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, jak wielu absolwentom udało się przyłączyć dane o certyfikatach i dyplomach, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
  }
  ## Tabela do diagnozowania kompletności danych z OKE #########################
  if (zapiszProblemy) {
    write.csv2(diagnostykaKompletnosciEgzZaw,
               "certyfikaty-i-dyplomy-kompletnosc.csv",
               row.names = FALSE, na = "", fileEncoding = "UTF-8")
    tabeleWejsciowe$W2 %>%
      filter(!is.na(.data$KOD_ZAW)) %>%
      mutate(wojewodztwo = floor(.data$TERYT_SZK / 100000)) %>%
      select("ID_ABS", "ROK_ABS", "TYP_SZK", "wojewodztwo") %>%
      distinct() %>%
      left_join(tabeleWejsciowe$W16 %>%
                  semi_join(tabeleWejsciowe$W22 %>%
                              filter(.data$MLODOC %in% 1) %>%
                              select(KOD_ZUS = .data$KOD) %>%
                              mutate(KOD_ZUS = 100 * .data$KOD_ZUS),
                            by = "KOD_ZUS") %>%
                  select("ID_ABS", "ROK_ABS") %>%
                  distinct() %>%
                  mutate(mlodociany = TRUE),
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(TYP_SZK =
               case_when(
                 .data$TYP_SZK == "Branżowa szkoła I stopnia" & mlodociany ~
                   "Branżowa szkoła I stopnia - młodociani",
                 .data$TYP_SZK == "Branżowa szkoła I stopnia" ~
                   "Branżowa szkoła I stopnia - niemłodociani",
                 TRUE ~ TYP_SZK)) %>%
      left_join(bind_rows(tabeleWejsciowe$W8,
                          tabeleWejsciowe$W10) %>%
                  select("ID_ABS", "ROK_ABS") %>%
                  distinct() %>%
                  mutate(swiadectwo = 1),
                by = c("ID_ABS", "ROK_ABS")) %>%
      left_join(bind_rows(tabeleWejsciowe$W9,
                          tabeleWejsciowe$W11) %>%
                  select("ID_ABS", "ROK_ABS") %>%
                  distinct() %>%
                  mutate(dyplom = 1),
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(across(c("swiadectwo", "dyplom"),
                    ~if_else(is.na(.), 0, .)),
             tylko_swiadectwo = .data$swiadectwo == 1 & .data$dyplom == 0) %>%
      group_by(.data$wojewodztwo, .data$TYP_SZK, .data$ROK_ABS) %>%
      summarise(across(c("swiadectwo", "dyplom", "tylko_swiadectwo"), mean),
                .groups = "drop") %>%
      select(-"swiadectwo") %>%
      pivot_wider(names_from = "TYP_SZK",
                  names_sep = ": ", names_vary = "slowest",
                  values_from = c("tylko_swiadectwo", "dyplom")) %>%
      write.csv2("kompletnosc-danych-o-egzaminach.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    tabeleWejsciowe$W2 %>%
      filter(!is.na(.data$KOD_ZAW)) %>%
      mutate(wojewodztwo = floor(.data$TERYT_SZK / 100000)) %>%
      select("ID_ABS", "ROK_ABS", "TYP_SZK", "wojewodztwo") %>%
      distinct() %>%
      left_join(tabeleWejsciowe$W16 %>%
                  semi_join(tabeleWejsciowe$W22 %>%
                              filter(.data$MLODOC %in% 1) %>%
                              select(KOD_ZUS = .data$KOD) %>%
                              mutate(KOD_ZUS = 100 * .data$KOD_ZUS),
                            by = "KOD_ZUS") %>%
                  select("ID_ABS", "ROK_ABS") %>%
                  distinct() %>%
                  mutate(mlodociany = TRUE),
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(TYP_SZK =
               case_when(
                 .data$TYP_SZK == "Branżowa szkoła I stopnia" & mlodociany ~
                   "Branżowa szkoła I stopnia - młodociani",
                 .data$TYP_SZK == "Branżowa szkoła I stopnia" ~
                   "Branżowa szkoła I stopnia - niemłodociani",
                 TRUE ~ TYP_SZK)) %>%
      left_join(bind_rows(tabeleWejsciowe$W8,
                          tabeleWejsciowe$W10) %>%
                  select("ID_ABS", "ROK_ABS") %>%
                  distinct() %>%
                  mutate(swiadectwo = 1),
                by = c("ID_ABS", "ROK_ABS")) %>%
      left_join(bind_rows(tabeleWejsciowe$W9,
                          tabeleWejsciowe$W11) %>%
                  select("ID_ABS", "ROK_ABS") %>%
                  distinct() %>%
                  mutate(dyplom = 1),
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(across(c("swiadectwo", "dyplom"),
                    ~if_else(is.na(.), 0, .)),
             tylko_swiadectwo = .data$swiadectwo == 1 & .data$dyplom == 0) %>%
      group_by(.data$wojewodztwo, .data$TYP_SZK, .data$ROK_ABS) %>%
      summarise(across(c("swiadectwo", "dyplom", "tylko_swiadectwo"), mean),
                .groups = "drop") %>%
      select(-"swiadectwo") %>%
      pivot_wider(names_from = "TYP_SZK",
                  names_sep = ": ", names_vary = "slowest",
                  values_from = c("tylko_swiadectwo", "dyplom")) %>%
      write.csv2("kompletnosc-danych-o-egzaminach.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
  }
  ## W24 (mapowanie kodów zawodów KZSB na ISCED-F) #############################
  if (anyNA(tabeleWejsciowe$W24) || any(tabeleWejsciowe$W24 == '')) {
    tabeleWejsciowe$W24 <- tabeleWejsciowe$W24 %>%
      filter(!is.na(.data$KOD_ZAW),
             !(.data$KOD_ISCED %in% c(NA_character_, '')),
             !(.data$GRUPA_ISCED %in% c(NA_character_, '')),
             !(.data$PODGRUPA_ISCED %in% c(NA_character_, '')),
             !(.data$NAZWA_ISCED %in% c(NA_character_, '')))
    message("Z tabeli 'W24' zostały usunięte wiersze zawierające braki danych lub puste ciagi znaków w nazwach jednostek klasyfikacji ISCED-F.\n")
  }
  brakMapowaniaNaISCED <- tabeleWejsciowe$W20a$KOD_ZAW %>%
    setdiff(tabeleWejsciowe$W24$KOD_ZAW) %>%
    sort()
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W24 (mapowanie kodów zawodów na ISCED-F)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W24 %>%
                         semi_join(tabeleWejsciowe$W20a,
                                   by = "KOD_ZAW"),
                       "w24", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  if (length(brakMapowaniaNaISCED) > 0L) {
    message("Wykryto następujące kody zawodów, dla których brak jest mapowania na klasyfikację ISCED-F:\n",
            paste(brakMapowaniaNaISCED, collapse = ", "),
            ".\nRozważ uzupełnienie mapowania w pliku 'W24.csv'.\n")
    if (zapiszProblemy) {
      write.csv2(zawodyBezMapowania,
                 "kody-zawodów-bez-przypisania-do-ISCED-F.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  ## W13 (przypisanie kierunków studiów do dziedzin i dyscyplin) ###############
  tabeleWejsciowe$W13 <- tabeleWejsciowe$W13 %>%
    mutate(CZY_DYSCYPLINA_WIODACA = as.logical(.data$CZY_DYSCYPLINA_WIODACA))
  kierunkiBezDziedzin <- tabeleWejsciowe$W12 %>%
    filter(!is.na(.data$ID_KIERUNKU_STU)) %>%
    count(.data$ID_KIERUNKU_STU, name = "lRekordow") %>%
    anti_join(tabeleWejsciowe$W13,
              by = "ID_KIERUNKU_STU") %>%
    arrange(desc(.data$lRekordow))
  if (nrow(kierunkiBezDziedzin) > 0L) {
    message("W pliku 'W12.csv' wykryto ", nrow(kierunkiBezDziedzin),
            " kierun(ek/ki/ków) studiów (które obejmują ", sum(kierunkiBezDziedzin$lRekordow),
            " rekordów w tym pliku), dla któr(ego/ych) nie ma podanego przypisania do dziedzin/dyscyplin w pliku 'W13.csv'.",
            ifelse(zapiszProblemy,
                   "\nZestawienie identyfikatorów tych/tego kierun(ku/ków) studiow zostanie zapisane w pliku 'kierunki-bez-przypisania-do-dyscyplin.csv'.\n",
                   "\nJeżeli chcesz móc sprawdzić, jaki(e) to kierun(ek/ki), uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`.\n"))
    if (zapiszProblemy) {
      write.csv2(kierunkiBezDziedzin,
                 "kierunki-bez-przypisania-do-dyscyplin.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabel W13 i W13a (mapowanie kierunków studiów na dziedziny i dyscypliny)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       bind_rows(tabeleWejsciowe$W12 %>%
                                   filter(!is.na(.data$ID_KIERUNKU_STU)) %>%
                                   select("ID_KIERUNKU_STU"),
                                 tabeleWejsciowe$W13 %>%
                                   select("ID_KIERUNKU_STU")) %>%
                         distinct(),
                       "w13a", wersja)))
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W13, "w13", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W30 (dane uczelni i ich filii) ############################################
  if ("W30" %in% names(tabeleWejsciowe)) {
    tabeleWejsciowe$W30a <- tabeleWejsciowe$W30 %>%
      select("ID_PODMIOTU", "ID_UCZELNI", "RODZAJ_UCZELNI") %>%
      distinct()
    brakiW30 <- tabeleWejsciowe$W30 %>%
      select(-c("TERYT_POW_PODMIOTU", "WOJEWODZTWO_PODMIOTU", "POWIAT_PODMIOTU",
                "RODZAJ_GMINY_PODMIOTU", "ORGAN_NADZORUJACY",
                "ORGAN_NADZORUJACY_TYP")) %>%
      sapply(function(x) return(anyNA(x) | any(x == '')))
    if (any(brakiW30)) {
      stop("W pliku 'W30.csv' kolumny '",
           paste(names(brakiW30)[brakiW30], collapse = "', '"),
           "' zawierają braki danych (lub ew. puste ciągi znaków), choć jest to niedozwolone.")
    }
    uczelnioLataAkadWDanych <- tabeleWejsciowe$W12 %>%
      filter(!(.data$ID_PODMIOTU %in% c("", NA_character_))) %>%
      mutate(DATA_DO = case_when(.data$CZY_UKONCZ_STU %in% 1 ~ .data$DATA_DO_STU,
                                 .data$CZY_UKONCZ_STU %in% 2 ~ .data$DATA_SKRESL_STU,
                                 .default = paste0("30-09-", rokMonitoringu)),
             across(contains("DATA"), ~as.Date(., format = "%d-%m-%Y"))) %>%
      group_by(.data$ID_PODMIOTU) %>%
      summarise(DATA_MIN = min(.data$DATA_OD_STU, na.rm = TRUE),
                DATA_MAX = max(.data$DATA_DO, na.rm = TRUE),
                .groups = "drop") %>%
      mutate(across(all_of(c("DATA_MIN", "DATA_MAX")),
                    ~data2rokszk(., max = rokMonitoringu - 1L,
                                 akademicki = TRUE, tekst = FALSE))) %>%
      group_by(.data$ID_PODMIOTU) %>%
      reframe(ROK_AKAD = .data$DATA_MIN:.data$DATA_MAX) %>%
      mutate(ROK_AKAD = paste0(.data$ROK_AKAD, "/", .data$ROK_AKAD + 1L))
    brakujaceDaneUczelni <- anti_join(uczelnioLataAkadWDanych, tabeleWejsciowe$W30,
                                      by = c("ID_PODMIOTU", "ROK_AKAD"))
    tabeleWejsciowe$W30 <- semi_join(tabeleWejsciowe$W30, uczelnioLataAkadWDanych,
                                     by = c("ID_PODMIOTU", "ROK_AKAD"))
    tabeleWejsciowe$W30a <- semi_join(tabeleWejsciowe$W30a, tabeleWejsciowe$W30,
                                      by = c("ID_PODMIOTU"))
    if (nrow(brakujaceDaneUczelni) > 0L) {
      write.csv2(brakujaceDaneUczelni, "brakujace-dane-uczelni.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
      stop("Dla niektórych uczelni/filii uczelni występujących w pliku 'W12.csv' nie znaleziono danych w pliku 'W30.csv'.\n",
           "Zestawienie tych szkoło-lat szkolnych zostało zapisane w pliku 'brakujace-dane-uczelni.csv'.")
    }
    if (wczytajDoBazy) {
      cat("\nZapis do bazy tabel W30 i W30a (dane uczelni i ich filii)...")
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W30a, "w30a", wersja)))
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W30, "w30", wersja)))
      cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    }
  }
  ## W12 (studiowanie) #########################################################
  # uwaga! agreguję po absolwento-kierunko-datach_rozpoczęcia! (w ramach tego
  # samego kierunku, rozpoczętego z tą samą datą można np. zmieniać tryb - i nie
  # tylko tryb - co generuje oddzielne rekordy w pliku przysyłanym przez OPI)
  tabeleWejsciowe$W12 <- tabeleWejsciowe$W12 %>%
    filter(!is.na(.data$ID_KIERUNKU_STU)) %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(across(c("DATA_OD_STU", "DATA_DO_STU", "DATA_SKRESL_STU"),
                  ~as.Date(., tryFormats = c("%d-%m-%Y", "%d/%m/%Y")))) %>%
    arrange(.data$ID_ABS, .data$ROK_ABS, .data$ID_KIERUNKU_STU,
            .data$DATA_OD_STU, desc(.data$DATA_DO_STU), .data$CZY_UKONCZ_STU,
            desc(.data$DATA_SKRESL_STU)) %>%
    group_by(.data$ID_ABS, .data$ROK_ABS, .data$ID_KIERUNKU_STU,
             .data$DATA_OD_STU) %>%
    slice(1) %>%
    ungroup()
  w12BrakDatyUkoncz <- is.na(tabeleWejsciowe$W12$DATA_DO_STU) &
    tabeleWejsciowe$W12$CZY_UKONCZ_STU == 1L
  if (any(w12BrakDatyUkoncz, na.rm = TRUE)) {
    warning("\nW danych dot. studiów (W12) łącznie ",
            sum(w12BrakDatyUkoncz, na.rm = TRUE),
            " rekord(y/ów) ma brak danych w kolumnie DATA_DO_STU, choć wartość kolumny CZY_UKONCZ_STU (1) wskazuje, że studia zostały ukończone (z sukcesem)!\n",
            immediate. = TRUE, call. = FALSE)
  }
  w12UkonczWPRzyszlosci <- year(tabeleWejsciowe$W12$DATA_DO_STU) > rokMonitoringu
  if (any(w12UkonczWPRzyszlosci, na.rm = TRUE)) {
    warning("\nW danych dot. studiów (W12) łącznie ",
            sum(w12UkonczWPRzyszlosci, na.rm = TRUE),
            " rekord(y/ów) ma w kolumnie DATA_DO_STU wartości wskazujące na lata późniejsze niż ",
            rokMonitoringu, "!\n", immediate. = TRUE, call. = FALSE)
  }
  w12BrakDatySkresl <- is.na(tabeleWejsciowe$W12$DATA_SKRESL_STU) &
    tabeleWejsciowe$W12$CZY_UKONCZ_STU == 2L
  if (any(w12BrakDatySkresl, na.rm = TRUE)) {
    warning("\nW danych dot. studiów (W12) łącznie ",
            sum(w12BrakDatySkresl, na.rm = TRUE),
            " rekord(y/ów) ma brak danych w kolumnie DATA_SKRESL_STU, choć wartość kolumny CZY_UKONCZ_STU (2) wskazuje na skreślenie z listy studentów!\n",
            immediate. = TRUE, call. = FALSE)
  }
  w12SkreslWPRzyszlosci <- year(tabeleWejsciowe$W12$DATA_SKRESL_STU) > rokMonitoringu
  if (any(w12SkreslWPRzyszlosci, na.rm = TRUE)) {
    warning("\nW danych dot. studiów (W12) łącznie ",
            sum(w12SkreslWPRzyszlosci, na.rm = TRUE),
            " rekord(y/ów) ma w kolumnie DATA_SKRESL_STU wartości wskazujące na lata późniejsze niż ",
            rokMonitoringu, "!\n", immediate. = TRUE, call. = FALSE)
  }
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W12 (kontynuacja nauki na studiach)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W12, "w12", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W14 (ew. zgony) ###########################################################
  cat("\nRozpoczęto przetwarzanie danych ZUS ",
      format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  tabeleWejsciowe$W14 <- tabeleWejsciowe$W14 %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS"))

  if (wczytajDoBazy & !nieWczytujDanychZUS) {
    cat("\nZapis do bazy tabeli W14 (ew. zgony - z danych ZUS)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W14, "w14", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  if (nrow(tabeleWejsciowe$W14) == 0L) {
    message("W pliku 'W14.csv' nie ma informacji o żadnym zgonie.\n")
  }
  ## W15 (zmiany adresów - z ZUS) ##############################################
  # uwaga, nie ma żadnej gwarancji, że dane ZUS o adresach są spójne
  # i wyczerpujące i generalnie nie należy tego od nich oczekiwać
  cat("\nRozpoczęto przetwarzanie danych ZUS dot. adresów ",
      format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  tabeleWejsciowe$W15 <- tabeleWejsciowe$W15 %>%
    filter(!is.na(.data$ADRES_TYP),
           !is.na(.data$TERYT)) %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_ADR_OD = as.Date(.data$DATA_ADR_OD,
                                 tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           DATA_ADR_DO = as.Date(.data$DATA_ADR_DO,
                                 tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
    arrange(.data$ID_ABS, .data$ROK_ABS, .data$ADRES_TYP,
            .data$DATA_ADR_OD, .data$DATA_ADR_OD) %>%
    group_by(.data$ID_ABS, .data$ROK_ABS, .data$ADRES_TYP, .data$DATA_ADR_OD) %>%
    mutate(LP = seq_len(n())) %>%
    ungroup()
  if (wczytajDoBazy & !nieWczytujDanychZUS) {
    cat("\nZapis do bazy tabeli W15 (dane adresowe z ZUS)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W15, "w15", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W18 (informacje o płatnikach) #############################################
  # uwaga! agreguję po płatnikach!
  cat("\nRozpoczęto przetwarzanie danych ZUS dot. płatników",
      format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  tabeleWejsciowe$W18 <- tabeleWejsciowe$W18 %>%
    filter(!is.na(.data$PKD)) %>%
    distinct()
  brakujacyPlatnicy <- bind_rows(select(tabeleWejsciowe$W16, .data$ID_PLATNIKA),
                                 select(tabeleWejsciowe$W17, .data$ID_PLATNIKA)) %>%
    distinct() %>%
    filter(!is.na(.data$ID_PLATNIKA)) %>%
    anti_join(tabeleWejsciowe$W18, by = "ID_PLATNIKA")
  tabeleWejsciowe$W18 <- tabeleWejsciowe$W18 %>%
    bind_rows(brakujacyPlatnicy)
  if (wczytajDoBazy & !nieWczytujDanychZUS) {
    cat("\nZapis do bazy tabeli W18 (dane o pracodawcach z ZUS)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W18, "w18", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W22 (mapowanie kodów tytułów składek ZUS) #################################
  cat("\nRozpoczęto przetwarzanie danych ZUS dot. składek ",
      format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  tabeleWejsciowe$W22 <- tabeleWejsciowe$W22 %>%
    mutate(across(-c("KOD", "OPIS"), as.logical))
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W22 (mapowanie kodów składek ZUS na kategorie użyteczne analitycznie)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W22 %>% rename(KOD_ZUS = "KOD"),
                       "w22", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W16 (składki ZUS) #########################################################
  # uwaga! agreguję po absolwento-okreso-płatniko-(skróconych)kodach!
  tabeleWejsciowe$W16 <- tabeleWejsciowe$W16 %>%
    filter(!is.na(.data$KOD_ZUS)) %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS"))
  if (wersja == "2026") {
    cat("\nIdentyfikowanie nieprawdopodobnych duplikatów w 'W16.csv' ",
        format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    nieprawdopodobneDuplikaty <- tabeleWejsciowe$W16 %>%
      filter(floor(.data$KOD_ZUS / 100) == 110 & .data$WYMIAR_ZATR_LICZNIK %in% 1 &
               .data$WYMIAR_ZATRUDNIENIA_MIANOWNIK %in% 1) %>%
      count(across(everything()),  name = "n") %>%
      filter(.data$n > 1)
    if (nrow(nieprawdopodobneDuplikaty) > 0L) {
      warning("W pliku 'W16.csv' wykryto duplikaty rekordów opisujących zatrudnienie w formie umowy o pracę na pełen etat - możliwe, że zostały one utworzone w wyniku błędu w procedurze generowania danych w ZUS.",
              #"\nDuplikaty te zostały usunięte (ale pozostawiono w danych po jednej kopii każdego z takich rekordów).",
              ifelse(zapiszProblemy,
                     "\nZestawienie tych rekordów zostanie zapisane w pliku 'nieprawdopodobne-duplikaty-skladek-zus.csv'.",
                     "\nJeżeli chcesz móc sprawdzić, których rekordów dotyczy problem, uruchom funkcję `wczytaj_tabele_wejsciowe()` z argumentem `zapiszProblemy=TRUE`."),
              "\nUWAGA! Wystąpienie takich duplikatów wskazuje na prawdopodobieństwo błędu w procedurze eksportu danych po stronie ZUS, który skutkuje zapewne występowaniem w danych również innych duplikatów, których nie da się w tak prosty i jednoznaczny sposób zidentyfikować!\n",
              immediate. = TRUE, call. = FALSE)
      if (zapiszProblemy) {
        write.csv2(nieprawdopodobneDuplikaty,
                   "nieprawdopodobne-duplikaty-skladek-zus.csv",
                   row.names = FALSE, na = "", fileEncoding = "UTF-8")
      }
    }
  }
  cat("\nRozpoczęto przekształcanie danych o składkach ('W16.csv') ",
      format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  tabeleWejsciowe$W16 <- tabeleWejsciowe$W16 %>%
    mutate(EMERYT_RENCISTA = floor(.data$KOD_ZUS / 10) - 10 * floor(.data$KOD_ZUS / 100),
           NIEPELNOSPRAWNY = .data$KOD_ZUS - 10 * floor(.data$KOD_ZUS / 10),
           KOD_ZUS = floor(.data$KOD_ZUS / 100),
           ROK_SKLADKA = as.integer(substr(.data$OKRES_ROZL, 7L, 10L)),
           MIES_SKLADKA = as.integer(substr(.data$OKRES_ROZL, 4L, 5L)),
           CZY_30 = as.logical(.data$CZY_30),
           CZY_RSA = as.logical(.data$CZY_RSA),
           OKRES_ROZL = as.Date(.data$OKRES_ROZL, format = "%d-%m-%Y"),
           across(starts_with("DATA_"), ~as.Date(., format = "%d-%m-%Y")),
           across(any_of("KOD_ZAW_PRACA"),
                  ~ifelse(. < 10000L, NA_integer_, .)))
  if (all(c("WYMIAR_ZATR_LICZNIK", "WYMIAR_ZATRUDNIENIA_MIANOWNIK") %in%
          names(tabeleWejsciowe$W16))) {
    tabeleWejsciowe$W16 <- tabeleWejsciowe$W16 %>%
      mutate(WYMIAR_ZATR_LICZNIK =
               ifelse(is.na(.data$WYMIAR_ZATR_LICZNIK) &
                        !is.na(.data$WYMIAR_ZATRUDNIENIA_MIANOWNIK),
                      1L, .data$WYMIAR_ZATR_LICZNIK),
             WYMIAR_ZATRUDNIENIA_MIANOWNIK =
               ifelse(is.na(.data$WYMIAR_ZATRUDNIENIA_MIANOWNIK) &
                        .data$WYMIAR_ZATR_LICZNIK %in% 1L,
                      1L, .data$WYMIAR_ZATRUDNIENIA_MIANOWNIK),
             WYMIAR_ZATR =
               .data$WYMIAR_ZATR_LICZNIK / .data$WYMIAR_ZATRUDNIENIA_MIANOWNIK) %>%
      mutate(WYMIAR_ZATR =
               ifelse(.data$KOD_ZUS %in% c(1110:1114, 1120, 1130:1133) &
                        is.na(.data$WYMIAR_ZATR),
                      1, .data$WYMIAR_ZATR))
  }
  if (all(c("DATA_OD_UBEZP", "DATA_DO_UBEZP") %in% names(tabeleWejsciowe$W16))) {
    tabeleWejsciowe$W16 <- tabeleWejsciowe$W16 %>%
      mutate(OKRES_ROZL_KON =
               as.Date(paste(as.integer(format(.data$OKRES_ROZL + 31, "%Y")),
                             as.integer(format(.data$OKRES_ROZL + 31, "%m")),
                             "01", sep = "-")) - 1L,
             DATA_OD_UBEZP = if_else(is.na(.data$DATA_OD_UBEZP) & !is.na(.data$DATA_DO_UBEZP),
                                     .data$OKRES_ROZL, .data$DATA_OD_UBEZP),
             DATA_DO_UBEZP = if_else(is.na(.data$DATA_DO_UBEZP) & !is.na(.data$DATA_OD_UBEZP),
                                     .data$OKRES_ROZL_KON, .data$DATA_DO_UBEZP),
             DATA_OD_UBEZP = if_else(.data$DATA_OD_UBEZP < .data$OKRES_ROZL,
                                     .data$OKRES_ROZL, .data$DATA_OD_UBEZP),
             DATA_DO_UBEZP = if_else(.data$DATA_DO_UBEZP > .data$OKRES_ROZL_KON,
                                     .data$OKRES_ROZL_KON, .data$DATA_DO_UBEZP),
             DNI_ZATR = 1L + difftime(.data$DATA_DO_UBEZP, .data$DATA_OD_UBEZP,
                                      units = "days"),
             DNI_W_MIES = 1L + difftime(.data$OKRES_ROZL_KON, .data$OKRES_ROZL,
                                        units = "days"),
             UDZIAL_DNI_ZATR_MIES =
               ifelse(.data$KOD_ZUS %in% 1240, # urlop macierzyński
                      1,
                      as.numeric(.data$DNI_ZATR) / as.numeric(.data$DNI_W_MIES))
               ) %>%
      select(-c("DATA_OD_UBEZP" , "DATA_DO_UBEZP", "DNI_ZATR", "DNI_W_MIES",
                "OKRES_ROZL_KON"))
  }
  # od edycji 2026, gdy mamy informacje o liczbie przepracowanych dni i (czasem)
  # wymiarze, trzeba to robić w mniej heurystyczny sposób i odwlekam to na etap
  # tworzenia tabel pośrednich
  tabeleWejsciowe$W16 <- tabeleWejsciowe$W16 %>%
    add_count(.data$ID_ABS, .data$ROK_ABS, .data$ROK_SKLADKA,
              .data$MIES_SKLADKA, .data$ID_PLATNIKA, .data$KOD_ZUS)
  if (wersja == "2021") {
    tabeleWejsciowe$W16 <- bind_rows(
      tabeleWejsciowe$W16 %>%
        filter(.data$n == 1L) %>%
        select(-"n"),
      tabeleWejsciowe$W16 %>%
        filter(n > 1L) %>%
        select(-"n") %>%
        group_by(.data$ID_ABS, .data$ROK_ABS, .data$ROK_SKLADKA,
                 .data$MIES_SKLADKA, .data$ID_PLATNIKA, .data$KOD_ZUS) %>%
        summarise(across(c("CZY_30", "CZY_RSA"), ~any(., na.rm = TRUE)),
                  across(c("EMERYT_RENCISTA", "NIEPELNOSPRAWNY"), max),
                  across(starts_with("PODST_"), ~sum(., na.rm = TRUE)),
                  .groups = "drop"))
  } else {
    tabeleWejsciowe$W16 <- bind_rows(
      tabeleWejsciowe$W16 %>%
        filter(.data$n == 1L) %>%
        mutate(LP = 0L) %>%
        select(-"n"),
      tabeleWejsciowe$W16 %>%
        filter(n > 1L) %>%
        mutate(LP = seq_len(n())) %>%
        ungroup() %>%
        select(-"n"))
  }
  kodyZusBezMapowania <- tabeleWejsciowe$W16 %>%
    select("KOD_ZUS") %>%
    anti_join(tabeleWejsciowe$W22 %>%
                rename(KOD_ZUS = "KOD"),
              by = "KOD_ZUS")
  if (nrow(kodyZusBezMapowania) > 0L) {
    warning("\nWykryto następujące kody składek ZUS, dla których brak jest mapowania na użyteczne analitycznie kategorie:\n",
            paste(unique(kodyZusBezMapowania$KOD_ZUS), collapse = ", "),
            ".\nKonieczne jest uzupełnienie mapowania w pliku 'W22.csv'.",
            immediate. = TRUE, call. = FALSE)
    if (zapiszProblemy) {
      write.csv2(kodyZusBezMapowania, "kody-ZUS-niewystępujące-w-W22.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  if (wczytajDoBazy & !nieWczytujDanychZUS) {
    cat("\nZapis do bazy tabeli W16 (miesięczne dane o składkach ZUS)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W16, "w16", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W23 (mapowanie kodów przerw w opłacaniu składek ZUS) ######################
  cat("\nRozpoczęto przetwarzanie danych ZUS dot. przerw w opłacaniu składek ",
      format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  tabeleWejsciowe$W23 <- tabeleWejsciowe$W23 %>%
    mutate(across(-c("KOD", "OPIS"), as.logical))
  if (wczytajDoBazy) {
    cat("\nZapis do bazy tabeli W23 (mapowanie kodów przerw w opłacaniu składek ZUS na kategorie użyteczne analitycznie)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W23, "w23", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  ## W17 (przerwy w opłacaniu składek ZUS) #####################################
  tabeleWejsciowe$W17 <- tabeleWejsciowe$W17 %>%
    filter(!is.na(.data$KOD_PRZERWY)) %>%
    filter(!(is.na(.data$DATA_OD_PRZERWA) | is.na(.data$DATA_DO_PRZERWA))) %>%
    semi_join(tabeleWejsciowe$W1,
              by = c("ID_ABS", "ROK_ABS")) %>%
    mutate(DATA_OD_PRZERWA = as.Date(.data$DATA_OD_PRZERWA,
                                     tryFormats = c("%d-%m-%Y", "%d/%m/%Y")),
           DATA_DO_PRZERWA = as.Date(.data$DATA_DO_PRZERWA,
                                     tryFormats = c("%d-%m-%Y", "%d/%m/%Y"))) %>%
    distinct()
  kodyPrzerwBezMapowania <- tabeleWejsciowe$W17 %>%
    select("KOD_PRZERWY") %>%
    anti_join(tabeleWejsciowe$W23 %>%
                rename(KOD_PRZERWY = "KOD"),
              by = "KOD_PRZERWY")
  if (nrow(kodyPrzerwBezMapowania) > 0L) {
    warning("\nWykryto następujące kody przerw w opłacaniu składek ZUS, dla których brak jest mapowania na użyteczne analitycznie kategorie:\n",
            paste(unique(kodyPrzerwBezMapowania$KOD_PRZERWY), collapse = ", "),
            ".\nKonieczne jest uzupełnienie mapowania w pliku 'W23.csv'.",
            immediate. = TRUE, call. = FALSE)
    if (zapiszProblemy) {
      write.csv2(kodyPrzerwBezMapowania, "kody-przerw-ZUS-niewystępujące-w-W23.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  if (wczytajDoBazy & !nieWczytujDanychZUS) {
    cat("\nZapis do bazy tabeli W17 (przerwy w opłacaniu składek ZUS)...")
    do.call(dbExecute,
            args = c(list(conn = con),
                     przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                       tabeleWejsciowe$W17, "w17", wersja)))
    cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  }
  cat("\nZakończono przetwarzanie danych ZUS ",
      format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
  ## W31 (orzeczenia o niepełnosprawności) #####################################
  if ("W31" %in% names(tabeleWejsciowe)) {
    tabeleWejsciowe$W31 <- tabeleWejsciowe$W31 %>%
      semi_join(tabeleWejsciowe$W1,
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(ORZECZENIE_DATA_DO = ifelse(.data$ORZECZENIE_DATA_DO == "31.12.9999",
                                         paste0("30.06.", .data$ROK_ABS),
                                         .data$ORZECZENIE_DATA_DO)) %>%
      mutate(across(contains("_DATA_"), ~as.Date(., format = "%d.%m.%Y")),
             across(starts_with("NIEPELNOSPRAWNOSC_SPRZ"),
                    ~. %in% 1L))
    if (wczytajDoBazy) {
      cat("\nZapis do bazy tabeli W31 (orzeczenia o niepełnosprawności)...")
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W31, "w31", wersja)))
      cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    }
  }
  ## W32 (skladki KRUS) ########################################################
  # spośród kilku rozpoczynających się w tym samym dniu wybieram rekord trwający
  # najdłużej, a ew. w dalszej kolejności opisywany niższym kodem roli
  if ("W32" %in% names(tabeleWejsciowe)) {
    tabeleWejsciowe$W32 <- tabeleWejsciowe$W32 %>%
      filter(!is.na(.data$DATA_OD_UBEZP)) %>%
      semi_join(tabeleWejsciowe$W1,
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(across(starts_with("DATA_"), ~as.Date(., format = "%Y-%m-%d")))
    liczbaDuplikatowW32 <- sum(duplicated(tabeleWejsciowe$W32))
    if (liczbaDuplikatowW32 > 0L) {
      message("W pliku wejściowym `W32.csv` ", liczbaDuplikatowW32,
              " rekord(y/ów) stanowi(ą) duplikat(y) innych rekordów z tego pliku. Zostaną one usunięte.")
      tabeleWejsciowe$W32 <- distinct(tabeleWejsciowe$W32)
    }
    tabeleWejsciowe$W32 <- tabeleWejsciowe$W32 %>%
      arrange(.data$ID_ABS, .data$ROK_ABS, .data$DATA_OD_UBEZP,
              desc(is.na(.data$DATA_DO_UBEZP)), desc(.data$DATA_DO_UBEZP),
              .data$ROLA_UBEZP) %>%
      group_by(.data$ID_ABS, .data$ROK_ABS, .data$DATA_OD_UBEZP) %>%
      slice(1) %>%
      ungroup()
    if (wczytajDoBazy) {
      cat("\nZapis do bazy tabeli W32 (składki KRUS)...")
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W32, "w32", wersja)))
      cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    }
  }
  ## W33 (przerwy w opłacaniu składek KRUS) ####################################
  # spośród kilku rozpoczynających się w tym samym dniu świadczeń tego samego
  # rodzaju wybieram rekord trwający najdłużej
  if ("W33" %in% names(tabeleWejsciowe)) {
    tabeleWejsciowe$W33 <- tabeleWejsciowe$W33 %>%
      filter(!is.na(.data$DATA_OD_SWIADCZ)) %>%
      semi_join(tabeleWejsciowe$W1,
                by = c("ID_ABS", "ROK_ABS")) %>%
      mutate(across(starts_with("DATA_"), ~as.Date(., format = "%Y-%m-%d")))
    liczbaDuplikatowW33 <- sum(duplicated(tabeleWejsciowe$W33))
    if (liczbaDuplikatowW33 > 0L) {
      message("W pliku wejściowym `W33.csv` ", liczbaDuplikatowW33,
              " rekord(y/ów) stanowi(ą) duplikat(y) innych rekordów z tego pliku. Zostaną one usunięte.")
      tabeleWejsciowe$W33 <- distinct(tabeleWejsciowe$W33)
    }
    tabeleWejsciowe$W33 <- tabeleWejsciowe$W33 %>%
      arrange(.data$ID_ABS, .data$ROK_ABS, .data$DATA_OD_SWIADCZ,
              .data$RODZAJ_SWIADCZ, desc(is.na(.data$DATA_DO_SWIADCZ)),
              desc(.data$DATA_DO_SWIADCZ)) %>%
      group_by(.data$ID_ABS, .data$ROK_ABS, .data$DATA_OD_SWIADCZ,
               .data$RODZAJ_SWIADCZ) %>%
      slice(1) %>%
      ungroup()
    if (wczytajDoBazy) {
      cat("\nZapis do bazy tabeli W33 (przerwy w opłacaniu składek KRUS)...")
      do.call(dbExecute,
              args = c(list(conn = con),
                       przygotuj_argumenty_do_zapisania_tabeli_wejsciowej(
                         tabeleWejsciowe$W33, "w33", wersja)))
      cat(" zakończony ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), ".", sep = "")
    }
  }
  ## koniec ####################################################################
  cat("\nKoniec: ", format(Sys.time(), "%Y.%m.%d %H:%M:%S"), "\n", sep = "")
  if (wczytajDoBazy) dbExecute(con, "COMMIT;")
  invisible(NULL)
}
