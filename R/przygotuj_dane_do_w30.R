#' @title Przygotowanie danych do tabeli wejsciowej W30
#' @description Funkcja przekształca dane o uczelniach i ich filiach
#' wyeksportowane z (publicznego API WWW) RAD-on do struktury, jaka
#' jest oczekiwana w tabeli *wejściowej* W30. Sprawdza przy tym, czy w danych
#' z RSPO znajdują się wszystkie szkoły, które występują w danych z danej edycji
#' monitoringu (pliku *W12.csv*).
#' @param rokMonitoringu rok (edycja) prowadzenie monitoringu (liczba)
#' @param plikInstytucje opcjonalnie ciąg znaków z nazwą pliku CSV zawierającego
#' dane instytucji nauki wyeksportowane z (publicznego API WWW) RAD-ON
#' [https://radon.nauka.gov.pl/dane/instytucje-systemu-szkolnictwa-wyzszego-i-nauki](https://radon.nauka.gov.pl/dane/instytucje-systemu-szkolnictwa-wyzszego-i-nauki);
#' domyślnie "radon-instytucje.csv"
#' @param plikFilie opcjonalnie ciąg znaków z nazwą pliku CSV zawierającego
#' dane filii instytucji nauki wyeksportowane z (publicznego API WWW) RAD-ON
#' [https://radon.nauka.gov.pl/dane/filie-instytucji-systemu-szkolnictwa-wyzszego-i-nauki](https://radon.nauka.gov.pl/dane/filie-instytucji-systemu-szkolnictwa-wyzszego-i-nauki);
#' domyślnie "radon-filie.csv"
#' @param zapiszDoPliku opcjonalnie ciąg znaków z nazwą pliku CSV, do którego ma
#' zostać zapisane przygotowane zestawienie; aby uniknąć zapisu do pliku, jako
#' wartość argumentu należy podać `NULL`
#' @param brakujaceDane opcjonalnie ciąg znaków wskazujący, co funkcja ma
#' zrobić, jeśli napotka sytuację, że w pliku 'W12.csv' występują rekordy
#' opisujące studiowanie w okresie, dla którego nie ma w RAD-on
#' (i 'SUCZELNIA.csv') danych o danej uczelni (lub - w praktyce częściej - jej
#' filii); domyślna wartość oznacza zapisanie na dysku oraz zwrócenie przez
#' funkcję zestawienia takich brakujących uczelni-lat akademickich (bez
#' przygotowania samej tabeli W30), `"ekstrapoluj"``nakazuje rozszerzyć dostępne
#' dane na brakujące lata akademickie, `"usun z W12"` nakazuje usunąć rekordy
#' opisujące studiowanie w okresie, dla którego brak jest informacji
#' o uczelni/filii z tabeli W12: tak odfiltrowana tabela zostanie zapisana
#' na dysku w pliku "W12popr.csv"
#' @returns ramka danych w formacie odpowiadającym specyfikacji
#' tabeli *wejściowej* W30; jeśli nastąpił zapis do pliku CSV, wynik zwracany
#' jest jako *ukryty*
#' @details
#' SUCZELNIA.csv zawiera nieliczne dodatkowe (w stosunku do zrzutów z RAN-onu)
#' rekordy, jak się zdaje, mogące mieć coś wspólnego z występowaniem
#' *pogrobowców*, którzy ani nie ukończyli studiów, ani nie zostali skreśleni,
#' choć podmiot, w którym się kształcili, przestał istnieć. Rekordy te są
#' pomijane.
#' @seealso [wczytaj_tabele_wejsciowe()], [przetworz_czesc_danych_o_uczelni()]
#'
#'
#' @importFrom stats na.omit
#' @importFrom utils read.csv read.csv2 write.csv2
#' @importFrom dplyr %>% .data across anti_join arrange bind_rows case_when coalesce contains distinct everything filter group_by inner_join left_join mutate reframe rename rename_with select slice_tail summarise ungroup
#' @export
przygotuj_dane_do_w30 <- function(rokMonitoringu,
                                  plikInstytucje = "radon-instytucje.csv",
                                  plikFilie = "radon-filie.csv",
                                  zapiszDoPliku = "W30.csv",
                                  brakujaceDane = c("zidentyfikuj",
                                                    "ekstrapoluj",
                                                    "usun z W12")) {
  brakujaceDane <- match.arg(brakujaceDane, several.ok = FALSE)
  stopifnot(is.numeric(rokMonitoringu), length(rokMonitoringu) == 1L,
            !is.na(rokMonitoringu), rokMonitoringu >= 2021L,
            as.integer(rokMonitoringu) == rokMonitoringu,
            is.character(plikInstytucje), length(plikInstytucje) == 1L,
            !is.na(plikInstytucje), plikInstytucje != "",
            file.exists(plikInstytucje),
            file.access(plikInstytucje, mode = 4) == 0L,
            is.character(plikFilie), length(plikFilie) == 1L,
            !is.na(plikFilie), plikFilie != "",
            file.exists(plikFilie), file.access(plikFilie, mode = 4) == 0L,
            "W katalogu roboczym nie ma pliku 'W25.csv'." =
              file.exists("W25.csv"),
            "W katalogu roboczym nie ma pliku 'SUCZELNIA.csv'." =
              file.exists("W12.csv"),
              file.exists("W12.csv"),
            "W katalogu roboczym nie ma pliku 'W12.csv'." =
              file.exists("W12.csv"))
  if (!is.null(zapiszDoPliku)) {
    stopifnot(length(zapiszDoPliku) == 1L, !is.na(zapiszDoPliku))
  }

  message("Wczytywanie plików 'W12.csv', 'SUCZELNIA.csv' i 'W25.csv'...")
  w12 <- read.csv2("W12.csv")
  stopifnot(all(c("ID_PODMIOTU", "ROK_ABS", "DATA_OD_STU", "CZY_UKONCZ_STU",
                  "DATA_DO_STU", "DATA_SKRESL_STU") %in% names(w12)))
  w12 <- w12 %>%
    filter(!(.data$ID_PODMIOTU %in% c("", NA_character_))) %>%
    mutate(DATA_DO = case_when(.data$CZY_UKONCZ_STU %in% 1 ~ .data$DATA_DO_STU,
                               .data$CZY_UKONCZ_STU %in% 2 ~ .data$DATA_SKRESL_STU,
                               .default = paste0("30-09-", rokMonitoringu)),
           across(contains("DATA"), ~as.Date(., format = "%d-%m-%Y")))
  w12MinMax <- w12 %>%
    group_by(.data$ID_PODMIOTU) %>%
    summarise(DATA_MIN = min(.data$DATA_OD_STU, na.rm = TRUE),
              DATA_MAX = max(.data$DATA_DO, na.rm = TRUE),
              .groups = "drop")
  suczelnia <- read.csv2("SUCZELNIA.csv")
  stopifnot(all(c("ID_UCZELNI", "ID_PODMIOTU", "DATA_OD", "DATA_DO",
                  "NAZWA_PODMIOTU", "TERYT_PODMIOTU",
                  "TERYT_GMI_PODMIOTU") %in% names(suczelnia)),
            !anyNA(suczelnia$DATA_OD == ""),
            !any(suczelnia$DATA_OD == ""),
            !anyNA(suczelnia$DATA_DO == ""),
            !any(suczelnia$DATA_DO == ""))
  suczelnia <- suczelnia %>%
    mutate(across(starts_with("DATA_"), ~as.Date(., format = "%d/%m/%Y")),
           TERYT_POW_PODMIOTU = 100L*.data$TERYT_PODMIOTU) %>%
    left_join(read.csv2("W25.csv") %>%
                rename_with(~paste0(., "_PODMIOTU")),
              by = "TERYT_POW_PODMIOTU") %>%
    select("ID_PODMIOTU", "DATA_OD", "DATA_DO", "TERYT_POW_PODMIOTU",
           "WOJEWODZTWO_PODMIOTU", "POWIAT_PODMIOTU",
           RODZAJ_GMINY_PODMIOTU = "TERYT_GMI_PODMIOTU") %>%
    distinct()
  message("...zakończone.")

  kolumnyInstytucje <- c(ID_PODMIOTU = "Id",
                         DATA_UTWORZENIA =
                           "Data.utworzenia...data.wpisu.do.ewiencji.uczelni.niepublicznych",
                         DATA_LIKWIDACJI = "Data.likwidacji",
                         RODZAJ_UCZELNI = "Rodzaj.instytucji",
                         NAZWA_PODMIOTU = "Nazwy...Nazwa",
                         NAZWA_PODMIOTU_DATA_OD = "Nazwy...Data.nadania.nazwy",
                         ORGAN_NADZORUJACY =
                           "Historia.zmian.organu...Organ.nadzorujący",
                         ORGAN_NADZORUJACY_DATA_OD =
                           "Historia.zmian.organu...Data.przypisania.do.organu",
                         STATUS_PODMIOTU =
                           "Historia.zmian.statusu...Status",
                         STATUS_PODMIOTU_DATA_OD =
                           "Historia.zmian.statusu...Data.nadania.statusu",
                         TYP_PODMIOTU =
                           "Historia.zmian.typu...Typ",
                         TYP_PODMIOTU_DATA_OD =
                           "Historia.zmian.typu...Data.nadania.typu",
                         ADRES =
                           "Historia.zmian.adresu...Adres",
                         ADRES_DATA_OD =
                           "Historia.zmian.adresu...Data.otrzymania.adresu")
  kolumnyFilie <- c(ID_PODMIOTU = "Id",
                    ID_UCZELNI = "Id.instytucji.głównej",
                    DATA_UTWORZENIA = "Data.utworzenia",
                    DATA_LIKWIDACJI = "Data.likwidacji",
                    NAZWA_PODMIOTU = "Historia.zmian.nazwy...Nazwa",
                    NAZWA_PODMIOTU_DATA_OD = "Historia.zmian.nazwy...Data.nadania.nazwy",
                    STATUS_PODMIOTU =
                      "Historia.zmian.statusu...Status",
                    STATUS_PODMIOTU_DATA_OD =
                      "Historia.zmian.statusu...Data.nadania.statusu",
                    ADRES =
                      "Historia.zmian.adresu...Adres",
                    ADRES_DATA_OD =
                      "Historia.zmian.adresu...Data.otrzymania.adresu")

  message("Wczytywanie plików z danymi uczelni i ich filii...")
  instytucje <- read.csv(plikInstytucje)
  brakujaceKolumny <- setdiff(kolumnyInstytucje, names(instytucje))
  if (length(brakujaceKolumny) > 0L) {
    stop("W pliku '", plikInstytucje, "' brakuje wymaganych kolumn: '",
         paste(brakujaceKolumny, collapse = "', '"), "'.")
  }
  instytucje <- instytucje[, kolumnyInstytucje]
  names(instytucje) <- names(kolumnyInstytucje)

  filie <- read.csv(plikFilie)
  brakujaceKolumny <- setdiff(kolumnyFilie, names(filie))
  if (length(brakujaceKolumny) > 0L) {
    stop("W pliku '", plikFilie, "' brakuje wymaganych kolumn: '",
         paste(brakujaceKolumny, collapse = "', '"), "'.")
  }
  filie <- filie[, kolumnyFilie]
  names(filie) <- names(kolumnyFilie)
  filie <- filie %>%
    inner_join(w12MinMax, by = "ID_PODMIOTU") %>%
    left_join(instytucje %>%
                select(ID_UCZELNI = "ID_PODMIOTU", "RODZAJ_UCZELNI") %>%
                distinct(),
              by = "ID_UCZELNI",
              relationship = "many-to-many") %>%
    mutate(RODZAJ_UCZELNI = paste0("Filia ",
                                   sub("^Uczelnia (.*)a$", "uczelni \\1ej",
                                       .data$RODZAJ_UCZELNI)),
           MIEJSCE_W_STRUKT = "filia uczelni")
  instytucje <- instytucje %>%
    filter(.data$ID_PODMIOTU %in% c(w12MinMax$ID_PODMIOTU, filie$ID_UCZELNI)) %>%
    left_join(w12MinMax, by = "ID_PODMIOTU") %>%
    left_join(filie %>%
                group_by(.data$ID_UCZELNI) %>%
                summarise(DATA_MIN_FILIA = min(.data$DATA_MIN),
                          DATA_MAX_FILIA = max(.data$DATA_MAX),
                          .groups = "drop") %>%
                rename(ID_PODMIOTU = "ID_UCZELNI"),
              by = "ID_PODMIOTU") %>%
    mutate(DATA_MIN = coalesce(.data$DATA_MIN, .data$DATA_MIN_FILIA),
           DATA_MIN = if_else(.data$DATA_MIN <= .data$DATA_MIN_FILIA |
                                 is.na(.data$DATA_MIN_FILIA),
                               .data$DATA_MIN, .data$DATA_MIN_FILIA),
           DATA_MAX = coalesce(.data$DATA_MAX, .data$DATA_MAX_FILIA),
           DATA_MAX = if_else(.data$DATA_MAX >= .data$DATA_MAX_FILIA |
                                 is.na(.data$DATA_MAX_FILIA),
                               .data$DATA_MAX, .data$DATA_MAX_FILIA),
           MIEJSCE_W_STRUKT = "uczelnia") %>%
    select(-c("DATA_MIN_FILIA", "DATA_MAX_FILIA")) %>%
    bind_rows(filie) %>%
    mutate(DATA_LIKWIDACJI = ifelse(.data$DATA_LIKWIDACJI %in% c("", NA_character_),
                                    paste0(rokMonitoringu, "-09-30"),
                                    .data$DATA_LIKWIDACJI),
           across(contains("DATA"), ~as.Date(., format = "%Y-%m-%d")))
  message("...zakończone.")

  suczelnia <- suczelnia %>%
    inner_join(instytucje %>%
                 select("ID_PODMIOTU", "DATA_MIN", "DATA_MAX",
                        "DATA_LIKWIDACJI") %>%
                 distinct(),
               by = "ID_PODMIOTU")

  slownikOrganow = c("Minister Transportu, Budownictwa i Gospodarki Morskiej" =
                       "Minister ds. gospodarki morskiej i żeglugi śródlądowej",
                     "Minister Gospodarki Morskiej i Żeglugi Śródlądowej" =
                       "Minister ds. gospodarki morskiej i żeglugi śródlądowej",
                     "Minister Infrastruktury i Rozwoju" =
                       "Minister ds. gospodarki morskiej i żeglugi śródlądowej",
                     "Minister Infrastruktury (poprzednio MiB)" =
                       "Minister ds. gospodarki morskiej i żeglugi śródlądowej",
                     "Minister Kultury i Dziedzictwa Narodowego (poprzednio MKDNiS)" =
                       "Minister ds. kultury i dziedzictwa narodowego",
                     "Minister Nauki i Szkolnictwa Wyższego (poprzednio MEiN)" =
                       "Minister ds. szkolnictwo wyższego i nauki",
                     "Minister Obrony Narodowej" =
                       "Minister ds. obrony",
                     "Minister Sprawiedliwości" =
                       "Minister ds. sprawiedliwości",
                     "Minister Spraw Wewnętrznych i Administracji" =
                       "Minister ds. wewnętrznych i administracji",
                     "Minister Zdrowia" = "Minister ds. zdrowia")
  brakujaceOrgany <- setdiff(na.omit(instytucje$ORGAN_NADZORUJACY),
                             names(slownikOrganow))
  if (length(brakujaceOrgany) > 0L) {
    warning("W słowniku organów, zakodowanym 'na twardo' wewnątrz funkcji przygotuj_dane_do_w30(), brakuje definicji typów dla następujących organów: '",
            paste(brakujaceOrgany, collapse = "', '"), "'.",
            call. = FALSE, immediate. = TRUE)
    names(brakujaceOrgany) <- brakujaceOrgany
    slownikOrganow <- c(slownikOrganow,
                        sapply(brakujaceOrgany, function(x) return(NA_character_)))
  }

  instytucjeNazwy <- instytucje %>%
    select("ID_PODMIOTU", "NAZWA_PODMIOTU", DATA_OD = "NAZWA_PODMIOTU_DATA_OD",
           "DATA_LIKWIDACJI", "DATA_MIN", "DATA_MAX") %>%
    przetworz_czesc_danych_o_uczelni(rokMonitoringu = rokMonitoringu)
  instytucjeAdresy <- instytucje %>%
    select("ID_PODMIOTU", "ADRES", DATA_OD = "ADRES_DATA_OD",
           "DATA_LIKWIDACJI", "DATA_MIN", "DATA_MAX") %>%
    distinct() %>%
    mutate(KRAJ_PODMIOTU = sub("^.*, ", "", .data$ADRES),
           ULICA = ifelse(.data$KRAJ_PODMIOTU == "Polska",
                          sub(", [[:digit:]]{2}-[[:digit:]]{3} .*$",
                              "", .data$ADRES),
                          sub(", .*$", "", .data$ADRES)),
           # z obsługą brytyjskich kodów pocztowych
           PNA = ifelse(.data$KRAJ_PODMIOTU == "Polska",
                        sub("^.*, ([[:digit:]]{2}-[[:digit:]]{3}) .*$",
                            "\\1", .data$ADRES),
                        sub("^.*, ([[:digit:]-]+|[[:digit:][:upper:]]{2,4} [[:digit:]][[:upper:]]{2}) .*$",
                            "\\1", .data$ADRES)),
           POCZTA = ifelse(.data$KRAJ_PODMIOTU == "Polska",
                           sub("^.*, [[:digit:]]{2}-[[:digit:]]{3} ([^,]+), .*$",
                               "\\1", .data$ADRES),
                           sub("^.*, ([[:digit:]-]+|[[:digit:][:upper:]]{2,4} [[:digit:]][[:upper:]]{2}) ([^,]+), .*$",
                               "\\2", .data$ADRES))) %>%
    select(-"ADRES") %>%
    przetworz_czesc_danych_o_uczelni(rokMonitoringu = rokMonitoringu)
  instytucjeStatusy <- instytucje %>%
    select("ID_PODMIOTU", "STATUS_PODMIOTU", DATA_OD = "STATUS_PODMIOTU_DATA_OD",
           "DATA_LIKWIDACJI", "DATA_MIN", "DATA_MAX") %>%
    przetworz_czesc_danych_o_uczelni(rokMonitoringu = rokMonitoringu)
  instytucjeTypy <- instytucje %>%
    select("ID_PODMIOTU", "TYP_PODMIOTU", DATA_OD = "TYP_PODMIOTU_DATA_OD",
           "DATA_LIKWIDACJI", "DATA_MIN", "DATA_MAX") %>%
    przetworz_czesc_danych_o_uczelni(rokMonitoringu = rokMonitoringu)
  instytucjeOrgany <- instytucje %>%
    filter(!is.na(.data$ORGAN_NADZORUJACY)) %>%
    select("ID_PODMIOTU", "ORGAN_NADZORUJACY",
           DATA_OD = "ORGAN_NADZORUJACY_DATA_OD", "DATA_LIKWIDACJI",
           "DATA_MIN", "DATA_MAX") %>%
    mutate(ORGAN_NADZORUJACY_TYP = slownikOrganow[.data$ORGAN_NADZORUJACY]) %>%
    przetworz_czesc_danych_o_uczelni(rokMonitoringu = rokMonitoringu)

  instytucjeTeryty <- suczelnia %>%
    przetworz_czesc_danych_o_uczelni(rokMonitoringu = rokMonitoringu)

  stopifnot(nrow(instytucjeNazwy) == nrow(instytucjeAdresy),
            nrow(instytucjeAdresy) == nrow(instytucjeStatusy),
            nrow(instytucjeOrgany) == nrow(instytucjeTypy),
            nrow(instytucjeTeryty) >= nrow(instytucjeAdresy))

  instytucje <- instytucje %>%
    select("ID_PODMIOTU", "RODZAJ_UCZELNI", "ID_UCZELNI") %>%
    distinct() %>%
    left_join(instytucjeNazwy,
              by = "ID_PODMIOTU") %>%
    left_join(instytucjeAdresy,
              by = c("ID_PODMIOTU", "ROK_AKAD")) %>%
    left_join(instytucjeTeryty,
              by = c("ID_PODMIOTU", "ROK_AKAD")) %>%
    left_join(instytucjeStatusy,
              by = c("ID_PODMIOTU", "ROK_AKAD")) %>%
    left_join(instytucjeTypy,
              by = c("ID_PODMIOTU", "ROK_AKAD")) %>%
    left_join(instytucjeOrgany,
              by = c("ID_PODMIOTU", "ROK_AKAD"))
  instytucje <- bind_rows(
    instytucje %>%
      filter(is.na(.data$ID_UCZELNI)),
    instytucje %>%
      filter(!is.na(.data$ID_UCZELNI)) %>%
      select(-c("TYP_PODMIOTU", "ORGAN_NADZORUJACY", "ORGAN_NADZORUJACY_TYP")) %>%
      left_join(instytucje %>%
                  select(ID_UCZELNI = "ID_PODMIOTU", "ROK_AKAD", "TYP_PODMIOTU",
                         "ORGAN_NADZORUJACY", "ORGAN_NADZORUJACY_TYP",
                         NAZWA_UCZELNI = "NAZWA_PODMIOTU"),
                by = c("ID_UCZELNI", "ROK_AKAD")) %>%
      mutate(NAZWA_PODMIOTU = paste0(.data$NAZWA_PODMIOTU,
                                     " (", .data$NAZWA_UCZELNI, ")")) %>%
      select(-"NAZWA_UCZELNI")
  ) %>%
    select("ID_PODMIOTU", "ID_UCZELNI", "RODZAJ_UCZELNI", "ROK_AKAD",
           "TYP_PODMIOTU", "NAZWA_PODMIOTU", "STATUS_PODMIOTU", "KRAJ_PODMIOTU",
           "TERYT_POW_PODMIOTU", "WOJEWODZTWO_PODMIOTU", "POWIAT_PODMIOTU",
           "RODZAJ_GMINY_PODMIOTU", "ULICA", "PNA", "POCZTA",
           "ORGAN_NADZORUJACY", "ORGAN_NADZORUJACY_TYP") %>%
    mutate(ID_UCZELNI = ifelse(is.na(.data$ID_UCZELNI),
                               .data$ID_PODMIOTU, .data$ID_UCZELNI)) %>%
    arrange(.data$ID_UCZELNI, .data$ID_UCZELNI != .data$ID_PODMIOTU,
            .data$ID_PODMIOTU, .data$ROK_AKAD)

  message("Sprawdzanie kompletności danych...")
  uczelnieWDanych <- w12MinMax %>%
    mutate(across(all_of(c("DATA_MIN", "DATA_MAX")),
                  ~data2rokszk(., max = rokMonitoringu - 1L,
                               tekst = FALSE, akademicki = TRUE))) %>%
    group_by(.data$ID_PODMIOTU) %>%
    reframe(ROK_AKAD = .data$DATA_MIN:.data$DATA_MAX) %>%
    mutate(ROK_AKAD = paste0(.data$ROK_AKAD, "/", .data$ROK_AKAD + 1L))
  brakujaceUczelnie <- uczelnieWDanych %>%
    anti_join(instytucje,
              by = intersect(names(uczelnieWDanych), names(instytucje))) %>%
    left_join(instytucje %>%
                select("ID_PODMIOTU", "RODZAJ_UCZELNI", "NAZWA_PODMIOTU",
                       "ID_UCZELNI") %>%
                group_by(.data$ID_PODMIOTU, .data$RODZAJ_UCZELNI,
                         .data$ID_UCZELNI) %>%
                slice_tail(n = 1) %>% # najnowsza nazwa
                ungroup(),
              by = "ID_PODMIOTU") %>%
    arrange(desc(is.na(.data$RODZAJ_UCZELNI)), .data$RODZAJ_UCZELNI,
            .data$ID_PODMIOTU, .data$ROK_AKAD)
  if (nrow(brakujaceUczelnie) > 0L) {
    if (brakujaceDane == "zidentyfikuj") {
      warning("W danych z RAD-on brakuje niektórych uczelnio/filio-lat, aby pokryć wszystkie okresy studiowania występujące w pliku 'W12.csv'.\n",
              "Zestawienie tych uczelnio/filio-lat zostanie zapisane w pliku 'brakujace-uczelnio-lata.csv' oraz zwrócone jako efekt działania funkcji.",
              call. = FALSE, immediate. = TRUE)
      write.csv2(brakujaceUczelnie, "brakujace-uczelnio-lata.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
      return(as.data.frame(brakujaceUczelnie))
    } else if (brakujaceDane == "ekstrapoluj") {
      uzupelnioneUczelnie <- bind_rows(
        brakujaceUczelnie %>%
          select("ID_PODMIOTU", "ROK_AKAD") %>%
          inner_join(instytucje %>%
                       mutate(STATUS_PODMIOTU = "Dane ekstrapolowane w przeszłość"),
                     by = join_by("ID_PODMIOTU",
                                  closest(x$ROK_AKAD < y$ROK_AKAD)),
                     suffix = c("", ".y")) %>%
          select(-"ROK_AKAD.y"),
        brakujaceUczelnie %>%
          select("ID_PODMIOTU", "ROK_AKAD") %>%
          inner_join(instytucje %>%
                       mutate(STATUS_PODMIOTU = "Dane ekstrapolowane w przyszłość"),
                     by = join_by("ID_PODMIOTU",
                                  closest(x$ROK_AKAD > y$ROK_AKAD)),
                     suffix = c("", ".y")) %>%
          select(-"ROK_AKAD.y")
      )
      brakujaceUczelnie <- brakujaceUczelnie %>%
        anti_join(uzupelnioneUczelnie,
                  by = c("ID_PODMIOTU", "ROK_AKAD"))
      if (nrow(brakujaceUczelnie) > 0L) {
        stop("W danych z RAD-on brakuje uczelni (lub ich filii) występujących w pliku 'W12.csv'. Problem dotyczy ID_PODMIOTU:\n'",
             paste(unique(brakujaceUczelnie$ID_PODMIOTU), collapse = "',\n'"), "'.")
      } else {
        instytucje <- instytucje %>%
          bind_rows(uzupelnioneUczelnie) %>%
          arrange(.data$ID_UCZELNI, .data$ID_UCZELNI != .data$ID_PODMIOTU,
                  .data$ID_PODMIOTU, .data$ROK_AKAD)
        warning("W danych z RAD-on brakuje ",
                format(nrow(uzupelnioneUczelnie), big.mark = " "),
                " uczelnio/filio-lat, aby pokryć wszystkie okresy studiowania występujące w pliku 'W12.csv'.\n",
                "Dane dotyczące tych uczelnio/filio-lat zostały uzupełnione z wykorzystaniem danych dotyczących tych samych uczelni/filii z innych, najbliższych w czasie lat akademickich.",
                call. = FALSE, immediate. = TRUE)
      }
    } else {
      w12Niepasujace <- w12 %>%
        semi_join(brakujaceUczelnie,
                  by = c("ID_PODMIOTU")) %>% # żeby nie wykonywać kosztownej operacji na całym w12
        mutate(DATA_OD = .data$DATA_OD_STU,
               DATA_OD_STU = format(.data$DATA_OD_STU, "%d-%m-%Y")) %>%
        select("ID_ABS", "ROK_ABS", "ID_KIERUNKU_STU", "ID_PODMIOTU",
               "DATA_OD_STU", "DATA_OD", "DATA_DO") %>%
        mutate(across(all_of(c("DATA_OD", "DATA_DO")),
                      ~data2rokszk(., max = rokMonitoringu - 1L,
                                   tekst = FALSE, akademicki = TRUE))) %>%
        distinct() %>%
        group_by(across(everything())) %>%
        reframe(ROK_AKAD = .data$DATA_OD:.data$DATA_DO) %>%
        mutate(ROK_AKAD = paste0(.data$ROK_AKAD, "/", .data$ROK_AKAD + 1L)) %>%
        semi_join(brakujaceUczelnie,
                  by = c("ID_PODMIOTU", "ROK_AKAD")) %>%
        select(-"ROK_AKAD") %>%
        distinct()
      w12 <- read.csv2("W12.csv") %>%
        filter(!is.na(.data$ID_KIERUNKU_STU))
      w12Popr <- w12 %>%
        anti_join(w12Niepasujace,
                  by = c("ID_ABS", "ROK_ABS", "ID_KIERUNKU_STU", "DATA_OD_STU",
                         "ID_PODMIOTU"))
      warning("W danych o studiowaniu w pliku 'W12.csv' występują rekordy opisujące studiowanie w podmiotach, ktore wg bazy RAD-on w tym czasie nie istniały.\n",
              "Problem dotyczy ",
              format(nrow(w12) - nrow(w12Popr), big.mark = " "), " rekordow.\n",
              "Dane po usunięciu tych rekordów zostaną zapisane w pliku 'W12popr.csv'.\n",
              "Aby przygotować dane do tabeli W30, zmień nazwę wspomnianego wyżej pliku na 'W12.csv' i uruchom funkcję ponownie.",
              call. = FALSE, immediate. = TRUE)
      write.csv2(w12Popr, "W12popr.csv",
                 row.names = FALSE, na = "", fileEncoding = "UTF-8")
    }
  }
  message("...zakończone.")

  if (!is.null(zapiszDoPliku)) {
    zapiszDoPliku <- paste0(sub("\\.csv$", "", zapiszDoPliku), ".csv")
    write.csv2(instytucje, zapiszDoPliku, row.names = FALSE, na = "")
    invisible(instytucje)
  } else {
    return(instytucje)
  }
}
