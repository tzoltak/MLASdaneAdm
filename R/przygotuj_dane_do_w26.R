#' @title Przygotowanie danych do tabeli wejsciowej W26
#' @description Funkcja przekształca dane o szkołach wyeksportowane
#' z (publicznego API WWW) RSPO i, opcjonalnie, dane o *idPodmiotu* SIO organów
#' prowadzących szkół, wyeksportowane z hurtowni danych SIO, do struktury, jaka
#' jest oczekiwana w tabeli *wejściowej* W26. Sprawdza przy tym, czy w danych
#' z RSPO znajdują się wszystkie szkoły, które występują w danych z danej edycji
#' monitoringu (plikach *W2.csv*, *W3.csv*, *W4.csv* i *W5.csv*).
#' @param rokMonitoringu rok (edycja) prowadzenie monitoringu (liczba)
#' @param rspo opcjonalnie wektor ciągów znaków z nazwami plików CSV
#' zawierającymi dane wyeksportowane z (publicznego API WWW) RSPO (lub pozyskane
#' w inny sposób, ale zapisane w tej samej strukturze); **ignorowany, jeśli
#' podano wartość argumentu `rspoHurtownia`** (inną niż `NULL`)
#' @param idOrgRejestr opcjonalnie nazwa pliku zawierającego dane o *idPodmiotu*
#' SIO organów **rejestrujących** szkoły (JST, na terenie których działają
#' poszczególne szkoły lub ministerstwa), wyeksportowane z hurtowni danych SIO;
#' można też podać `NULL`, aby nie dołączać tych danych (w szczególności, gdy
#' nie są dostępne); **ignorowany, jeśli podano wartość argumentu
#' `rspoHurtownia`** (inną niż `NULL`)
#' @param rspoHurtownia opcjonalnie nazwa pliku zawierającego dane szkół
#' wyeksportowane z hurtowni SIO; można też podać `NULL`, aby skorzystać
#' z danych we wcześniej wykorzystywanych formatach, opisanych argumentami
#' `rspo` i `idOrgRejestr`
#' @param zapiszDoPliku opcjonalnie ciąg znaków z nazwą pliku CSV, do którego ma
#' zostać zapisane przygotowane zestawienie; aby uniknąć zapisu do pliku, jako
#' wartość argumentu należy podać `NULL`
#' @returns ramka danych w formacie odpowiadającym specyfikacji
#' tabeli *wejściowej* W26; jeśli nastąpił zapis do pliku CSV, wynik zwracany
#' jest jako *ukryty*
#' @seealso [wczytaj_tabele_wejsciowe()], [zidentyfikuj_szkolo_lata_w_danych()]
#' @importFrom stats setNames
#' @importFrom utils write.csv2
#' @importFrom dplyr %>% .data arrange bind_rows case_match distinct everything inner_join left_join mutate select rename_with
#' @export
przygotuj_dane_do_w26 <- function(rokMonitoringu,
                                  rspo = list.files(pattern = "^rspo_.*\\.csv$"),
                                  idOrgRejestr = "id-organow-rejestrujacych.csv",
                                  rspoHurtownia = "dane-szkół-z-hurtowni-SIO.csv",
                                  zapiszDoPliku = "W26.csv") {
  stopifnot(is.numeric(rokMonitoringu), length(rokMonitoringu) == 1L,
            !is.na(rokMonitoringu), rokMonitoringu >= 2021L,
            as.integer(rokMonitoringu) == rokMonitoringu,
            is.null(rspo) | is.character(rspo),
            is.null(idOrgRejestr) | is.character(idOrgRejestr),
            is.null(rspoHurtownia) | is.character(rspoHurtownia),
            is.null(zapiszDoPliku) | is.character(zapiszDoPliku),
            "W katalogu roboczym nie ma pliku 'W2.csv'." =
              file.exists("W2.csv"),
            "Nie można uzyskać dostępu do pliku 'W2.csv'" =
              file.access("W2.csv", mode = 4) == 0L,
            "W katalogu roboczym nie ma pliku 'W3.csv'." =
              file.exists("W3.csv"),
            "Nie można uzyskać dostępu do pliku 'W3.csv'" =
              file.access("W3.csv", mode = 4) == 0L,
            "W katalogu roboczym nie ma pliku 'W4.csv'." =
              file.exists("W4.csv"),
            "Nie można uzyskać dostępu do pliku 'W4.csv'" =
              file.access("W4.csv", mode = 4) == 0L,
            "W katalogu roboczym nie ma pliku 'W5.csv'." =
              file.exists("W5.csv"),
            "Nie można uzyskać dostępu do pliku 'W5.csv'" =
              file.access("W5.csv", mode = 4) == 0L)
  if (!is.null(rspoHurtownia)) {
    stopifnot(length(rspoHurtownia) == 1L, !is.na(rspoHurtownia), rspoHurtownia != "",
              file.exists(rspoHurtownia), file.access(rspoHurtownia, mode = 4) == 0L)
  } else {
    stopifnot(is.character(rspo),
              "Nie podano plików zawierających dane z RSPO do przetworzenia." = length(rspo) > 0L,
              !anyNA(rspo), all(rspo != ""),
              all(file.exists(rspo)), all(file.access(rspo, mode = 4) == 0L))
    if (!is.null(idOrgRejestr)) {
      stopifnot(length(idOrgRejestr) == 1L, !is.na(idOrgRejestr), idOrgRejestr != "",
                file.exists(idOrgRejestr), file.access(idOrgRejestr, mode = 4) == 0L)
    }
  }
  if (!is.null(zapiszDoPliku)) {
    stopifnot(length(zapiszDoPliku) == 1L, !is.na(zapiszDoPliku))
  }

  message("Wczytywanie plików 'W2.csv', 'W3.csv', 'W4.csv' i 'W5.csv'...")
  w2 <- arrow::read_csv2_arrow("W2.csv")
  stopifnot(all(c("ID_SZK", "ROK_ABS") %in% names(w2)))
  w3 <- arrow::read_csv2_arrow("W3.csv")
  stopifnot(all(c("ID_SZK_KONT", "DATA_OD_SZK_KONT",
                  "DATA_DO_SZK_KONT") %in% names(w3)))
  w4 <- arrow::read_csv2_arrow("W4.csv")
  stopifnot(all(c("ID_SZK_KONT", "DATA_OD_KKZ", "DATA_DO_KKZ") %in% names(w4)))
  w5 <- arrow::read_csv2_arrow("W5.csv")
  stopifnot(all(c("ID_SZK_KONT", "DATA_OD_KUZ", "DATA_DO_KUZ") %in% names(w5)))
  message("...zakończone.")

  kolumnyRSPO <- c(ID_SZK = "NUMER.RSPO", TYP_SZK = "TYP", NAZWA_SZK = "NAZWA",
                   TERYT_GMI_SZK = "KOD.TERYTORIALNY.GMINA",
                   WOJEWODZTWO_SZK = "WOJEWÓDZTWO",
                   POWIAT_SZK = "POWIAT", GMINA_SZK = "GMINA",
                   SIMC_MIEJSC = "KOD.TERYTORIALNY.MIEJSCOWOŚĆ",
                   MIEJSCOWOSC = "MIEJSCOWOŚĆ",
                   RODZAJ_MIEJSC = "RODZAJ.MIEJSCOWOŚCI",
                   SYM_UL = "KOD.TERYTORIALNY.ULICA",
                   ULICA = "ULICA", NR_BUDYNKU = "NUMER.BUDYNKU",
                   NR_LOKALU = "NUMER.LOKALU",
                   PNA = "KOD.POCZTOWY", POCZTA = "POCZTA",
                   PUBLICZNOSC = "PUBLICZNOŚĆ.STATUS",
                   KATEGORIA_UCZNIOW = "KATEGORIA.UCZNIÓW",
                   SPECYFIKA = "SPECYFIKA.PLACÓWKI",
                   ORGAN_PROWADZACY_TYP = "TYP.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_NAZWA = "NAZWA.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_REGON = "REGON.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_WOJ = "WOJEWÓDZTWO.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_POW = "POWIAT.ORGANU.PROWADZĄCEGO",
                   ORGAN_PROWADZACY_GMI = "GMINA.ORGANU.PROWADZĄCEGO",
                   MIEJSCE_W_STRUKT = "MIEJSCE.W.STRUKTURZE",
                   JEDN_NADRZ_ID = "RSPO.PODMIOTU.NADRZĘDNEGO",
                   JEDN_NADRZ_TYP = "TYP.PODMIOTU.NADRZĘDNEGO")
  kolumnyidOrgRejestr <- c("ID_SZK", "ORGAN_REJESTRUJACY_ID",
                           "ORGAN_REJESTRUJACY_TYP",
                           "ORGAN_REJESTRUJACY_NAZWA",
                           "ORGAN_REJESTRUJACY_TERYT",
                           "ORGAN_SPOSOB_EWIDENCJONOWANIA")

  if (!is.null(rspoHurtownia)) {
    message("Wczytywanie pliku z danymi z RSPO...")
    rspo <- arrow::read_csv2_arrow(rspoHurtownia)
    brakujaceKolumny <- setdiff(c("ROK_SZK", names(kolumnyRSPO),
                                  kolumnyidOrgRejestr),
                                names(rspo))
    if (length(brakujaceKolumny) > 0L) {
      stop("W pliku '", rspoHurtownia, "' brakuje wymaganych kolumn: '",
           paste(brakujaceKolumny, collapse = "', '"), "'.")
    }
    message("...zakończone.")
    rspo <- rspo[, unique(c("ROK_SZK", names(kolumnyRSPO), kolumnyidOrgRejestr))]
  } else{
    rspo <- setNames(lapply(rspo, arrow::read_csv2_arrow), rspo)
    czyProblem <- FALSE
    message("Rok monitoringu: ", rokMonitoringu)
    message("Wczytywanie plików z danymi z RSPO:")
    for (p in seq_along(rspo)) {
      names(rspo[[p]]) <- toupper(names(rspo[[p]]))
      brakujaceKolumny <- setdiff(kolumnyRSPO, names(rspo[[p]]))
      if (length(brakujaceKolumny) > 0L) {
        czyProblem <- czyProblem | TRUE
      }
      message("- plik '", names(rspo)[p], "' ",
              ifelse(length(brakujaceKolumny) == 0L,
                     "ma wszystkie wymagane kolumny;",
                     paste0("brakuje kolumn (z dokładnością do wielkości liter): '",
                            paste(brakujaceKolumny, collapse = "', '"), "';")))
    }
    message("...zakończone.")
    stopifnot("We wczytywanym pliku/plikach brakuje wymaganych kolumn." = !czyProblem)
    rspo <- bind_rows(rspo)
    rspo <- rspo[, kolumnyRSPO]
    names(rspo) <- names(kolumnyRSPO)

    if (!is.null(idOrgRejestr)) {
      message("Wczytywanie danych o idPodmiotu SIO organów prowadzących...")
      idOrgRejestrDane <- arrow::read_csv2_arrow(idOrgRejestr)
      names(idOrgRejestrDane) <- toupper(names(idOrgRejestrDane))
      brakujaceKolumny <- setdiff(kolumnyidOrgRejestr, names(idOrgRejestrDane))
      if (length(brakujaceKolumny) > 0L) {
        stop("W pliku '", idOrgRejestr, "' brakuje wymaganych kolumn: '",
             paste(brakujaceKolumny, collapse = "', '"), "'.")
      }
      idOrgRejestrDane$ORGAN_REJESTRUJACY_TERYT <-
        as.integer(idOrgRejestrDane$ORGAN_REJESTRUJACY_TERYT) *
        10^case_match(nchar(idOrgRejestrDane$ORGAN_REJESTRUJACY_TERYT),
                      c(6L, 7L) ~ 0, c(3L, 4L) ~ 3, c(1L, 2L) ~ 5)
      rspo <- left_join(rspo, idOrgRejestrDane[, kolumnyidOrgRejestr], by = "ID_SZK")
      message("...zakończone.")
    }
  }

  message("Sprawdzanie kompletności danych...")
  rspo <- rspo %>%
    left_join(rspo %>%
                select(ORGAN_PROWADZACY_TERYT = "TERYT_GMI_SZK",
                       "WOJEWODZTWO_SZK", "POWIAT_SZK") %>%
                mutate(ORGAN_PROWADZACY_TERYT =
                         100 * floor(.data$ORGAN_PROWADZACY_TERYT / 1000)) %>%
                distinct() %>%
                rename_with(~paste0("ORGAN_PROWADZACY_", substr(., 1, 3)),
                            .cols = -"ORGAN_PROWADZACY_TERYT"),
              by = c("ORGAN_PROWADZACY_WOJ", "ORGAN_PROWADZACY_POW"))
  szkolyWDanych <- zidentyfikuj_szkolo_lata_w_danych(w2, w3, w4, w5,
                                                     rokMonitoringu)
  message("...zakończone.")
  brakujaceSzkoly <- szkolyWDanych %>%
    anti_join(rspo,
              by = intersect(names(szkolyWDanych), names(rspo)))
  if (nrow(brakujaceSzkoly) > 0L) {
    if (ncol(brakujaceSzkoly) == 1L) {
      stop("W danych z RSPO brakuje szkół o następujących id: ",
           paste(brakujaceSzkoly$ID_SZK, collapse = ", "), ".")
    } else {
      stop("W danych z RSPO brakuje następujących szkoło-lat:\n",
           paste(apply(brakujaceSzkoly, 1,
                       function(x) paste0(names(x), ": ", x, collapse = ", "))
                 , collapse = ";\n"), ".")
    }
  }
  rspo <- rspo %>%
    inner_join(szkolyWDanych,
               by = intersect(c("ID_SZK", "ROK_SZK"), names(rspo))) %>%
    select("ID_SZK", "ROK_SZK", everything()) %>%
    arrange(.data$ID_SZK, .data$ROK_SZK)
  stopifnot("Zduplikowane kombinacje wartości (ID_SZK, ROK_SZK)." =
              0L == (rspo %>%
                       count(.data$ID_SZK, .data$ROK_SZK) %>%
                       filter(n > 1) %>%
                       nrow()))

  if (!is.null(zapiszDoPliku)) {
    zapiszDoPliku <- paste0(sub("\\.csv$", "", zapiszDoPliku), ".csv")
    write.csv2(rspo, zapiszDoPliku, row.names = FALSE, na = "")
    invisible(rspo)
  } else {
    return(rspo)
  }
}
